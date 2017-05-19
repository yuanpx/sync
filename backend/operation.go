package backend

import (
	"errors"
	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/mysql"
	"github.com/neverlee/glog"
	"time"
)

type GEN_FUNC func(repo int, file_name string, dir_name string, file_type int, op int, file_version int, file_hash string, file_from string, file_to string) ([]OpExecute, error)

var GEN_OP_FUNCS map[int]GEN_FUNC

func init() {
	GEN_OP_FUNCS := make(map[int]GEN_FUNC)
	GEN_OP_FUNCS[OP_ADD] = Gen_File_Add
	GEN_OP_FUNCS[OP_DEL] = Gen_File_Del
	GEN_OP_FUNCS[OP_COPY] = Gen_File_Copy
	GEN_OP_FUNCS[OP_UPDATE] = Gen_File_Update
	GEN_OP_FUNCS[OP_MOVE] = Gen_File_Move
	GEN_OP_FUNCS[OP_RECOVER] = Gen_File_Recover
}

const (
	OP_ADD     = 1
	OP_MOVE    = 2
	OP_COPY    = 3
	OP_RECOVER = 4
	OP_DEL     = 5
	OP_UPDATE  = 6
)

type OpFile struct {
	FileName    string
	DirName     string
	FileHash    string
	FileVersion int
	Op          int // 1: add 2: move 3: copy 4: del 5: update 6: recover
	From        string
	To          string
}

type OpFileADD struct {
	FileName string
	DirName  string
	FileHash string
	Repo     int
	FileType int
}

func (op *OpFileADD) process_op(tx *gorm.DB, event_id int, now time.Time) error {

	var count int
	var temp_db *gorm.DB
	temp_db = tx.Model(&CurrentFile{}).Where("file_name = ? and dir_name = ? and file_type = ? and status = ? and repo = ?", op.FileName, op.DirName, op.FileType, 1, op.Repo).Count(&count)
	if temp_db.Error != nil {
		glog.Errorln(temp_db.Error)
		return temp_db.Error
	}

	if count > 0 {
		glog.Errorln("file_already exist")
		return errors.New("file already exist")
	}

	var max_version int
	temp_db = tx.Model(&FileVersion{}).Where("file_name = ? and dir_name = ? and file_type = ? and repo = ?", op.FileName, op.DirName, op.FileType, op.Repo).Count(&count)
	if temp_db.Error != nil {
		glog.Errorln(temp_db.Error)
		return temp_db.Error
	}

	if count == 0 {
		max_version = 0
	} else {
		var max_file FileVersion
		temp_db = tx.Where(&FileVersion{
			Repo:     op.Repo,
			FileName: op.FileName,
			DirName:  op.DirName,
			FileType: op.FileType,
		}).Last(&max_file)
		if temp_db.Error != nil {
			glog.Errorln(temp_db.Error)
			return temp_db.Error
		}

		max_version = max_file.FileVersion
		glog.Errorln("max version: ", max_version)

	}
	fv := FileVersion{
		Repo:        op.Repo,
		FileName:    op.FileName,
		DirName:     op.DirName,
		FileVersion: max_version + 1,
		FileType:    op.FileType,
		FileHash:    op.FileHash,
		TimeStamp:   now,
		EventId:     event_id,
		Op:          OP_ADD,
	}
	temp_db = tx.Create(&fv)
	if temp_db.Error != nil {
		glog.Errorln(temp_db.Error)
		return temp_db.Error
	}

	current_file := CurrentFile{
		Repo:        op.Repo,
		EventId:     event_id,
		FileName:    op.FileName,
		DirName:     op.DirName,
		FileVersion: max_version + 1,
		FileType:    op.FileType,
		Status:      1,
		TimeStamp:   now,
	}

	if max_version == 0 {
		temp_db = tx.Create(&current_file)
		if temp_db.Error != nil {
			glog.Errorln(temp_db.Error)
			return temp_db.Error
		}
	} else {
		temp_db = tx.Save(&current_file)
		if temp_db.Error != nil {
			glog.Errorln(temp_db.Error)
			return temp_db.Error
		}
	}

	return nil
}

type OpFileRecover struct {
	FileName    string
	DirName     string
	Repo        int
	FileVersion int
	FileType    int
}

func (op *OpFileRecover) process_op(tx *gorm.DB, event_id int, now time.Time) error {

	var temp_db *gorm.DB
	var version_file FileVersion
	temp_db = tx.Where(&FileVersion{
		Repo:        op.Repo,
		FileName:    op.FileName,
		DirName:     op.DirName,
		FileType:    op.FileType,
		FileVersion: op.FileVersion,
	}).First(&version_file)

	if temp_db.Error != nil {
		glog.Errorln(temp_db.Error)
		return temp_db.Error
	}

	var max_file FileVersion
	max_file.FileVersion = 0
	temp_db = tx.Where(&FileVersion{
		Repo:     op.Repo,
		FileName: op.FileName,
		DirName:  op.DirName,
		FileType: op.FileType,
	}).Last(&max_file)
	if temp_db.Error != nil {
		glog.Errorln(temp_db.Error)
		return temp_db.Error
	}

	max_version := max_file.FileVersion

	fv := FileVersion{
		Repo:        op.Repo,
		FileName:    op.FileName,
		DirName:     op.DirName,
		FileType:    op.FileType,
		FileVersion: max_version + 1,
		FileHash:    version_file.FileHash,
		TimeStamp:   now,
		EventId:     event_id,
		Op:          OP_RECOVER,
	}
	temp_db = tx.Create(&fv)
	if temp_db.Error != nil {
		glog.Errorln(temp_db.Error)
		return temp_db.Error
	}

	current_file := CurrentFile{
		Repo:        op.Repo,
		EventId:     event_id,
		FileName:    op.FileName,
		DirName:     op.DirName,
		FileType:    op.FileType,
		FileVersion: max_version + 1,
		Status:      1,
		TimeStamp:   now,
	}

	if max_version == 0 {
		temp_db = tx.Create(&current_file)
		if temp_db.Error != nil {
			glog.Errorln(temp_db.Error)
			return temp_db.Error
		}
	} else {
		temp_db = tx.Save(&current_file)
		if temp_db.Error != nil {
			glog.Errorln(temp_db.Error)
			return temp_db.Error
		}
	}

	return nil
}

type OpFileDel struct {
	FileName    string
	DirName     string
	Repo        int
	FileType    int
	FileVersion int
}

func (op *OpFileDel) Gen_From_Tuple(t *File_Tuple) {
	op.FileName = t.FileName
	op.DirName = t.DirFrom
	op.FileType = t.FileType
	op.FileVersion = t.FileVersion
}

func (op *OpFileDel) process_op(tx *gorm.DB, event_id int, now time.Time) error {

	var count int
	var temp_db *gorm.DB
	temp_db = tx.Model(&CurrentFile{}).Where("file_name = ? and dir_name = ? and file_type = ? and status = ? and repo = ?", op.FileName, op.DirName, op.FileType, 0, op.Repo).Count(&count)
	if temp_db.Error != nil {
		glog.Errorln(temp_db.Error)
		return temp_db.Error
	}

	if count > 0 {
		return nil
	}

	var max_file FileVersion
	max_file.FileVersion = 0
	temp_db = tx.Where(&FileVersion{
		Repo:     op.Repo,
		FileName: op.FileName,
		DirName:  op.DirName,
		FileType: op.FileType,
	}).Last(&max_file)
	if temp_db.Error != nil {
		glog.Errorln(temp_db.Error)
		return temp_db.Error
	}

	max_version := max_file.FileVersion

	fv := FileVersion{
		Repo:        op.Repo,
		FileName:    op.FileName,
		DirName:     op.DirName,
		FileType:    op.FileType,
		FileVersion: max_version + 1,
		TimeStamp:   now,
		EventId:     event_id,
		Op:          OP_DEL,
	}
	temp_db = tx.Create(&fv)
	if temp_db.Error != nil {
		glog.Errorln(temp_db.Error)
		return temp_db.Error
	}

	current_file := CurrentFile{
		Repo:        op.Repo,
		EventId:     event_id,
		FileName:    op.FileName,
		DirName:     op.DirName,
		FileType:    op.FileType,
		FileVersion: max_version + 1,
		Status:      0,
		TimeStamp:   now,
	}

	if max_version == 0 {
		temp_db = tx.Create(&current_file)
		if temp_db.Error != nil {
			glog.Errorln(temp_db.Error)
			return temp_db.Error
		}
	} else {
		temp_db = tx.Save(&current_file)
		if temp_db.Error != nil {
			glog.Errorln(temp_db.Error)
			return temp_db.Error
		}
	}

	return nil
}

type OpFileUpdate struct {
	FileName    string
	DirName     string
	Repo        int
	FileType    int
	FileHash    string
	FileVersion int
}

func (op *OpFileUpdate) process_op(tx *gorm.DB, event_id int, now time.Time) error {

	var count int
	var temp_db *gorm.DB
	temp_db = tx.Model(&CurrentFile{}).Where("file_name = ? and dir_name = ? and file_type = ? and status = ? and repo = ?", op.FileName, op.DirName, op.FileType, 1, op.Repo).Count(&count)
	if temp_db.Error != nil {
		glog.Errorln(temp_db.Error)
		return temp_db.Error
	}

	if count <= 0 {
		return errors.New("no such file")
	}

	var max_file FileVersion
	max_file.FileVersion = 0
	temp_db = tx.Where(&FileVersion{
		Repo:     op.Repo,
		FileName: op.FileName,
		DirName:  op.DirName,
		FileType: op.FileType,
	}).Last(&max_file)
	if temp_db.Error != nil {
		glog.Errorln(temp_db.Error)
		return temp_db.Error
	}

	max_version := max_file.FileVersion
	if max_version > op.FileVersion {
		op.FileName = Gen_Conflit_Name(op.FileName)
	}

	fv := FileVersion{
		Repo:        op.Repo,
		FileName:    op.FileName,
		DirName:     op.DirName,
		FileType:    op.FileType,
		FileVersion: max_version + 1,
		TimeStamp:   now,
		EventId:     event_id,
		Op:          OP_UPDATE,
	}
	temp_db = tx.Create(&fv)
	if temp_db.Error != nil {
		return temp_db.Error
	}

	current_file := CurrentFile{
		Repo:        op.Repo,
		EventId:     event_id,
		FileName:    op.FileName,
		FileType:    op.FileType,
		DirName:     op.DirName,
		FileVersion: max_version + 1,
		Status:      1,
		TimeStamp:   now,
	}

	if max_version == 0 {
		temp_db = tx.Create(&current_file)
		if temp_db.Error != nil {
			glog.Errorln(temp_db.Error)
			return temp_db.Error
		}
	} else {
		temp_db = tx.Save(&current_file)
		if temp_db.Error != nil {
			glog.Errorln(temp_db.Error)
			return temp_db.Error
		}
	}

	return nil
}

type OpFileCopy struct {
	FileName    string
	DirName     string
	Repo        int
	FileType    int
	FileVersion int
	FileTo      string
}

func (op *OpFileCopy) Gen_From_Tuple(t *File_Tuple) {
	op.FileName = t.FileName
	op.DirName = t.DirFrom
	op.FileTo = t.DirTo
	op.FileType = t.FileType
	op.FileVersion = t.FileVersion
}

func (op *OpFileCopy) process_op(tx *gorm.DB, event_id int, now time.Time) error {

	var count int
	var temp_db *gorm.DB
	temp_db = tx.Model(&CurrentFile{}).Where("file_name = ? and dir_name = ? and file_type = ? and status = ? and repo = ?", op.FileName, op.FileTo, op.FileType, 0, op.Repo).Count(&count)
	if temp_db.Error != nil {
		glog.Errorln(temp_db.Error)
		return temp_db.Error
	}

	if count > 0 {
		return errors.New("file already exist")
	}

	temp_db = tx.Model(&FileVersion{}).Where("file_name = ? and dir_name = ? and file_type = ? and repo = ?", op.FileName, op.FileTo, op.FileType, op.Repo).Count(&count)
	if temp_db.Error != nil {
		glog.Errorln(temp_db.Error)
		return temp_db.Error
	}

	var max_version int
	max_file := FileVersion{}
	if count > 0 {
		var max_file FileVersion
		max_file.FileVersion = 0
		temp_db = tx.Where(&FileVersion{
			Repo:     op.Repo,
			FileName: op.FileName,
			DirName:  op.FileTo,
			FileType: op.FileType,
		}).Last(&max_file)
		if temp_db.Error != nil {
			glog.Errorln(temp_db.Error)
			return temp_db.Error
		}

		max_version = max_file.FileVersion
	} else {
		max_version = 0
	}

	temp_db = tx.Where(&FileVersion{
		Repo:        op.Repo,
		FileName:    op.FileName,
		DirName:     op.DirName,
		FileType:    op.FileType,
		FileVersion: op.FileVersion,
	}).Last(&max_file)
	if temp_db.Error != nil {
		glog.Errorln(temp_db.Error)
		return temp_db.Error
	}

	src_file_hash := max_file.FileHash
	fv := FileVersion{
		Repo:        op.Repo,
		FileName:    op.FileName,
		DirName:     op.FileTo,
		FileType:    op.FileType,
		FileVersion: max_version + 1,
		FileHash:    src_file_hash,
		TimeStamp:   now,
		EventId:     event_id,
		Op:          OP_COPY,
		From:        op.DirName,
	}
	temp_db = tx.Create(&fv)
	if temp_db.Error != nil {
		glog.Errorln(temp_db.Error)
		return temp_db.Error
	}

	current_file := CurrentFile{
		Repo:        op.Repo,
		EventId:     event_id,
		FileName:    op.FileName,
		FileType:    op.FileType,
		DirName:     op.FileTo,
		FileVersion: max_version + 1,
		Status:      1,
		TimeStamp:   now,
	}

	if max_version == 0 {
		temp_db = tx.Create(&current_file)
		if temp_db.Error != nil {
			glog.Errorln(temp_db.Error)
			return temp_db.Error
		}
	} else {
		temp_db = tx.Save(&current_file)
		if temp_db.Error != nil {
			glog.Errorln(temp_db.Error)
			return temp_db.Error
		}
	}

	return nil
}

type OpFileMove struct {
	FileName    string
	DirName     string
	Repo        int
	FileType    int
	FileVersion int
	FileTo      string
}

func (op *OpFileMove) Gen_From_Tuple(t *File_Tuple) {
	op.FileName = t.FileName
	op.DirName = t.DirFrom
	op.FileType = t.FileType
	op.FileVersion = t.FileVersion
	op.FileTo = t.DirTo
}

func (op *OpFileMove) process_op(tx *gorm.DB, event_id int, now time.Time) error {

	var count int
	var temp_db *gorm.DB
	temp_db = tx.Model(&CurrentFile{}).Where("file_name = ? and dir_name = ? and file_type = ? and status = ? and repo = ?", op.FileName, op.FileTo, op.FileType, 0, op.Repo).Count(&count)
	if temp_db.Error != nil {
		glog.Errorln(temp_db.Error)
		return temp_db.Error
	}

	if count > 0 {
		return errors.New("file already exist")
	}

	temp_db = tx.Model(&FileVersion{}).Where("file_name = ? and dir_name = ? and file_type = ? and repo = ?", op.FileName, op.FileTo, op.FileType, op.Repo).Count(&count)
	if temp_db.Error != nil {
		glog.Errorln(temp_db.Error)
		return temp_db.Error
	}

	var max_version int
	max_file := FileVersion{}
	if count > 0 {
		max_file.FileVersion = 0
		temp_db = tx.Where(&FileVersion{
			Repo:     op.Repo,
			FileName: op.FileName,
			DirName:  op.FileTo,
			FileType: op.FileType,
		}).Last(&max_file)
		if temp_db.Error != nil {
			glog.Errorln(temp_db.Error)
			return temp_db.Error
		}

		max_version = max_file.FileVersion
	} else {
		max_version = 0
	}

	temp_db = tx.Where(&FileVersion{
		Repo:        op.Repo,
		FileName:    op.FileName,
		DirName:     op.DirName,
		FileType:    op.FileType,
		FileVersion: op.FileVersion,
	}).Last(&max_file)
	if temp_db.Error != nil {
		glog.Errorln(temp_db.Error)
		return temp_db.Error
	}

	src_file_hash := max_file.FileHash

	fv := FileVersion{
		Repo:        op.Repo,
		FileName:    op.FileName,
		DirName:     op.FileTo,
		FileType:    op.FileType,
		FileVersion: max_version + 1,
		FileHash:    src_file_hash,
		TimeStamp:   now,
		EventId:     event_id,
		Op:          OP_MOVE,
		From:        op.DirName,
	}
	temp_db = tx.Create(&fv)
	if temp_db.Error != nil {
		glog.Errorln(temp_db.Error)
		return temp_db.Error
	}

	current_file := CurrentFile{
		Repo:        op.Repo,
		EventId:     event_id,
		FileName:    op.FileName,
		FileType:    op.FileType,
		DirName:     op.FileTo,
		FileVersion: max_version + 1,
		Status:      1,
		TimeStamp:   now,
	}

	if max_version == 0 {
		temp_db = tx.Create(&current_file)
		if temp_db.Error != nil {
			glog.Errorln(temp_db.Error)
			return temp_db.Error
		}
	} else {
		temp_db = tx.Save(&current_file)
		if temp_db.Error != nil {
			glog.Errorln(temp_db.Error)
			return temp_db.Error
		}
	}

	return nil
}

type OpRes struct {
	Res     int
	EventId int
	ErrMsg  string
}

type OpExecute interface {
	process_op(tx *gorm.DB, event_id int, now time.Time) error
}

func Gen_File_Add(repo int, file_name string, dir_name string, file_type int, op int, file_version int, file_hash string, file_from string, file_to string) ([]OpExecute, error) {

	res := make([]OpExecute, 0)
	if file_name == "" || dir_name == "" || file_type == 0 || file_hash == "" {
		return nil, errors.New("WRONG ARGS")
	}
	file_op_add := &OpFileADD{
		Repo:     repo,
		FileName: file_name,
		DirName:  dir_name,
		FileType: file_type,
		FileHash: file_hash,
	}
	res = append(res, file_op_add)

	return res, nil
}
func Gen_File_Recover(repo int, file_name string, dir_name string, file_type int, op int, file_version int, file_hash string, file_from string, file_to string) ([]OpExecute, error) {

	res := make([]OpExecute, 0)
	if file_name == "" || dir_name == "" || file_type == 0 || file_version < 1 {
		return nil, errors.New("WRONG ARGS")
	}
	file_op_recover := &OpFileRecover{
		Repo:        repo,
		FileName:    file_name,
		DirName:     dir_name,
		FileType:    file_type,
		FileVersion: file_version,
	}
	res = append(res, file_op_recover)

	return res, nil
}

func Gen_File_Del(repo int, file_name string, dir_name string, file_type int, op int, file_version int, file_hash string, file_from string, file_to string) ([]OpExecute, error) {

	res := make([]OpExecute, 0)
	if file_name == "" || dir_name == "" || file_type == 0 || file_version < 0 {
		return nil, errors.New("WRONG ARGS")
	}
	file_op_del := &OpFileDel{
		Repo:        repo,
		FileName:    file_name,
		DirName:     dir_name,
		FileType:    file_type,
		FileVersion: file_version,
	}
	res = append(res, file_op_del)

	return res, nil
}
func Gen_File_Update(repo int, file_name string, dir_name string, file_type int, op int, file_version int, file_hash string, file_from string, file_to string) ([]OpExecute, error) {

	res := make([]OpExecute, 0)
	if file_name == "" || dir_name == "" || file_type == 0 || file_hash == "" || file_version < 0 {
		return nil, errors.New("WRONG ARGS")
	}
	file_op_update := &OpFileUpdate{
		Repo:        repo,
		FileName:    file_name,
		DirName:     dir_name,
		FileType:    file_type,
		FileHash:    file_hash,
		FileVersion: file_version,
	}
	res = append(res, file_op_update)

	return res, nil
}

func Gen_File_Copy(repo int, file_name string, dir_name string, file_type int, op int, file_version int, file_hash string, file_from string, file_to string) ([]OpExecute, error) {

	res := make([]OpExecute, 0)
	if file_name == "" || dir_name == "" || file_type == 0 || file_hash == "" || file_version < 0 || file_to == "" {
		return nil, errors.New("WRONG ARGS")
	}
	file_op_copy := &OpFileCopy{
		Repo:        repo,
		FileName:    file_name,
		DirName:     dir_name,
		FileType:    file_type,
		FileVersion: file_version,
		FileTo:      file_to,
	}
	res = append(res, file_op_copy)

	return res, nil
}

func Gen_File_Move(repo int, file_name string, dir_name string, file_type int, op int, file_version int, file_hash string, file_from string, file_to string) ([]OpExecute, error) {

	res := make([]OpExecute, 0)
	if file_name == "" || dir_name == "" || file_type == 0 || file_hash == "" || file_version < 0 || file_to == "" {
		return nil, errors.New("WRONG ARGS")
	}
	file_op_move := &OpFileMove{
		Repo:        repo,
		FileName:    file_name,
		DirName:     dir_name,
		FileType:    file_type,
		FileVersion: file_version,
		FileTo:      file_to,
	}
	res = append(res, file_op_move)

	file_op_del := &OpFileDel{
		Repo:        repo,
		FileName:    file_name,
		DirName:     dir_name,
		FileType:    file_type,
		FileVersion: file_type,
	}

	res = append(res, file_op_del)

	return res, nil
}

func Gen_File_Op(GenFuncMap map[int]GEN_FUNC, repo int, file_name string, dir_name string, file_type int, op int, file_version int, file_hash string, file_from string, file_to string) ([]OpExecute, error) {
	gen_op_func, ok := GenFuncMap[op]
	if !ok {
		return nil, errors.New("no such op")
	}

	res, err := gen_op_func(repo, file_name, dir_name, file_type, op, file_version, file_hash, file_from, file_to)
	if err != nil {
		return nil, err
	}

	return res, nil
}
