package backend

import (
	"errors"
	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/mysql"
	"github.com/neverlee/glog"
	"github.com/satori/go.uuid"
	"strconv"
	"sync"
	"time"
)

var (
	DBConfErr    = errors.New("DB config error")
	TableConfErr = errors.New("Table config error")
)

type DBInfo struct {
	DBName string `gorm:"primary key"`
	User   string
	Passwd string
	Host   string
}

func (version *DBInfo) TableName() string {
	return "db_info"
}

type FileVersion struct {
	FileVersion int    `gorm:"primary_key"`
	FileName    string `gorm:"primary_key"`
	DirName     string `gorm:"primary_key"`
	FileType    int    `gorm:"primary_key"` // 1: file 2: dir
	Repo        int    `gorm:"primary_key"`
	EventId     int
	FileHash    string
	Op          int
	From        string
	TimeStamp   time.Time
}

func (version *FileVersion) TableName() string {
	res := DefaultGenName("file_version", version.Repo, DBM.Conf.TableGap)
	return res
}

type FileMeta struct {
	FileId   string
	FileName string
	FileType int
}

type CurrentFile struct {
	FileName    string `gorm:"primary_key"`
	DirName     string `gorm:"primary_key"`
	FileType    int    `gorm:"primary_key"`
	Repo        int    `gorm:"primary_key"`
	EventId     int
	FileVersion int
	DirFileId   string
	Status      int // 0: del 1: exist
	TimeStamp   time.Time
}

func (cur *CurrentFile) TableName() string {
	res := DefaultGenName("current_file", cur.Repo, DBM.Conf.TableGap)
	return res
}

var DBM *DBManager

type DBManager struct {
	Conf     *DBConf
	ConfDB   *gorm.DB
	Contexts map[string]*DataContext
	Mutex    sync.Mutex
}

func Init_DBManager(conf *DBConf) (*DBManager, error) {

	conf_db, err := Init_Context(conf.User, conf.Passwd, conf.DBHost, conf.DBName)
	if err != nil {
		return nil, err
	}

	DB := DBManager{
		Contexts: make(map[string]*DataContext),
		Conf:     conf,
		ConfDB:   conf_db,
	}

	return &DB, nil
}

func DefaultGenName(base string, condition interface{}, option int) string {
	if option <= 0 {
		return base
	}
	repo_id := condition.(int)
	index := repo_id / option
	res := base + "_" + strconv.Itoa(index)
	return res
}

func (m *DBManager) Get_DBContext(db_name string) (*DataContext, error) {

	m.Mutex.Lock()
	defer m.Mutex.Unlock()
	db, ok := m.Contexts[db_name]
	if ok {
		return db, nil
	}

	var dbinfo DBInfo
	temp_db := m.ConfDB.Where(&DBInfo{DBName: db_name}).First(&dbinfo)
	if temp_db.Error != nil {
		return nil, temp_db.Error
	}

	con, err := Init_Context(dbinfo.User, dbinfo.Passwd, dbinfo.Host, db_name)
	if err != nil {
		return nil, err
	}

	db_context := DataContext{
		DBConn: con,
	}
	m.Contexts[db_name] = &db_context
	return &db_context, nil
}

type DataContext struct {
	DBConn *gorm.DB
}

type TransFunc func(tx *gorm.DB) (interface{}, error)

func Init_Context(user string, passwd string, host string, db string) (*gorm.DB, error) {
	if user == "" || passwd == "" || host == "" || db == "" {
		return nil, DBConfErr
	}

	con_str := user + ":" + passwd + "@tcp(" + host + ")/" + db + "?charset=utf8&parseTime=True&loc=Local"
	db_conn, err := gorm.Open("mysql", con_str)
	if err != nil {
		glog.Errorln("failed to open db")
		return nil, err
	}

	return db_conn, nil
}

func (context *DataContext) Init_DB() error {

	ok := context.DBConn.HasTable(&FileVersion{})
	if !ok {
		temp_db := context.DBConn.CreateTable(&FileVersion{})
		if temp_db.Error != nil {
			glog.Errorln("failed to create fileversion table")
			return temp_db.Error
		}
	}

	ok = context.DBConn.HasTable(&CurrentFile{})
	if !ok {
		temp_db := context.DBConn.CreateTable(&CurrentFile{})
		if temp_db.Error != nil {
			glog.Errorln("failed to create currentfile table")
			return temp_db.Error
		}
	}

	return nil
}

func (context *DataContext) Drop_DB() error {
	context.DBConn.DropTable(&FileVersion{})
	context.DBConn.DropTable(&CurrentFile{})

	return nil
}

func (con *DataContext) Exec_Trans(ops []OpExecute) error {
	tx := con.DBConn.Begin()
	if tx.Error != nil {
		return tx.Error
	}

	defer func() {
		tx.Rollback()
	}()

	now := time.Now()
	for _, op := range ops {
		event_id, err := con.get_next_event_id()
		if err != nil {
			return err
		}

		err = op.process_op(tx, event_id, now)

		if err != nil {
			return err
		}
	}

	temp_db := tx.Commit()

	if temp_db.Error != nil {
		return temp_db.Error
	}

	return nil
}

func (con *DataContext) get_next_event_id() (int, error) {
	return 0, nil
}

func Gen_Uuid() string {
	uid := uuid.NewV4()
	return uid.String()
}
func Gen_Conflit_Name(file_name string) string {
	return file_name + Gen_Uuid() + ".conflict"
}

type File_Tuple struct {
	FileName    string
	DirFrom     string
	DirTo       string
	FileType    int
	FileVersion int
}

func Gen_Files_Op(tx *gorm.DB, dir_from_parent string, dir_from string, dir_to string) ([]*File_Tuple, error) {
	file_res := make([]*File_Tuple, 0)
	file_res = append(file_res, &File_Tuple{
		FileName: dir_from,
		DirFrom:  dir_from_parent,
		DirTo:    dir_to,
		FileType: 2,
	})

	files := make([]CurrentFile, 0)
	temp_db := tx.Where(&CurrentFile{
		DirName: dir_from,
		Status:  1,
	}).Find(&files)

	if temp_db.Error != nil {
		return nil, temp_db.Error
	}

	target_dir := dir_to + dir_from
	for _, file := range files {
		if file.FileType == 1 {
			file_res = append(file_res, &File_Tuple{
				FileName: file.FileName,
				DirFrom:  file.DirName,
				DirTo:    target_dir,
				FileType: 1,
			})
		} else {
			file_tuples, err := Gen_Files_Op(tx, file.DirName, file.FileName, target_dir)
			if err != nil {
				return nil, err
			}

			for _, sub_file := range file_tuples {
				file_res = append(file_res, sub_file)
			}

		}
	}

	return file_res, nil
}
