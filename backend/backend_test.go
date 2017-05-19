package backend

import (
	"errors"
	"testing"
)

func test_init() (map[int]GEN_FUNC, *DBManager, error) {
	GEN_OP_FUNCS := make(map[int]GEN_FUNC)
	GEN_OP_FUNCS[OP_ADD] = Gen_File_Add
	GEN_OP_FUNCS[OP_DEL] = Gen_File_Del
	GEN_OP_FUNCS[OP_COPY] = Gen_File_Copy
	GEN_OP_FUNCS[OP_UPDATE] = Gen_File_Update
	GEN_OP_FUNCS[OP_MOVE] = Gen_File_Move
	GEN_OP_FUNCS[OP_RECOVER] = Gen_File_Recover

	cfgpath := "test_sync.conf"
	cfg, err := LoadConfig(cfgpath)
	if err != nil {
		return nil, nil, err
	}
	DBM, err = Init_DBManager(cfg)
	if err != nil {
		return nil, nil, err
	}

	if !DBM.ConfDB.HasTable(&DBInfo{}) {
		temp_db := DBM.ConfDB.CreateTable(&DBInfo{})
		if temp_db.Error != nil {
			return nil, nil, temp_db.Error
		}
	}

	db, err := DBM.Get_DBContext("test_0")
	if err != nil {
		return nil, nil, err
	}

	file_version_table := FileVersion{
		Repo: 0,
	}

	if !db.DBConn.HasTable(&file_version_table) {
		temp_db := db.DBConn.CreateTable(&file_version_table)
		if temp_db.Error != nil {
			return nil, nil, temp_db.Error
		}
	}

	current_file_table := CurrentFile{
		Repo: 0,
	}

	if !db.DBConn.HasTable(&current_file_table) {
		temp_db := db.DBConn.CreateTable(&current_file_table)
		if temp_db.Error != nil {
			return nil, nil, temp_db.Error
		}
	}

	return GEN_OP_FUNCS, DBM, nil
}

func Test_genOpt(t *testing.T) {
	repo := 0
	file_name := "test_file"
	dir_name := "test_dir"
	file_type := 1
	file_version := 1
	file_hash := "sdfasd"
	file_from := "test_from"
	file_to := "test_to"
	var op_type int
	GEN_OP_FUNCS, _, init_err := test_init()
	if init_err != nil {
		t.Error(init_err.Error())
		return
	}

	op_type = OP_ADD
	file_op_adds, err := Gen_File_Op(GEN_OP_FUNCS, repo, file_name, dir_name, file_type, op_type, file_version, file_hash, file_from, file_to)
	if err != nil {
		t.Error("Gen add op error: ", err)
	}

	if len(file_op_adds) != 1 {
		t.Error("Error op number")
	}

	op_type = OP_DEL
	file_op_dels, err := Gen_File_Op(GEN_OP_FUNCS, repo, file_name, dir_name, file_type, op_type, file_version, file_hash, file_from, file_to)
	if err != nil {
		t.Error("Gen del op error: ", err)
	}

	if len(file_op_dels) != 1 {
		t.Error("Error op number")
	}

	op_type = OP_COPY
	file_op_copys, err := Gen_File_Op(GEN_OP_FUNCS, repo, file_name, dir_name, file_type, op_type, file_version, file_hash, file_from, file_to)
	if err != nil {
		t.Error("Gen copy op error: ", err)
	}

	if len(file_op_copys) != 1 {
		t.Error("Error op number")
	}

	op_type = OP_RECOVER
	file_op_recovers, err := Gen_File_Op(GEN_OP_FUNCS, repo, file_name, dir_name, file_type, op_type, file_version, file_hash, file_from, file_to)
	if err != nil {
		t.Error("Gen recover op error: ", err)
	}

	if len(file_op_recovers) != 1 {
		t.Error("Error op number")
	}

	op_type = OP_MOVE
	file_op_moves, err := Gen_File_Op(GEN_OP_FUNCS, repo, file_name, dir_name, file_type, op_type, file_version, file_hash, file_from, file_to)
	if err != nil {
		t.Error("Gen move op error: ", err)
	}

	if len(file_op_moves) != 2 {
		t.Error("Error op number")
	}
}

func Test_Add(t *testing.T) {
	repo := 0
	file_name := "test_file"
	dir_name := "test_dir"
	file_type := 1
	file_version := 1
	file_hash := "sdfasd"
	file_from := "test_from"
	file_to := "test_to"
	var op_type int
	GEN_OP_FUNCS, DBM, init_err := test_init()
	if init_err != nil {
		t.Error(init_err.Error())
		return
	}

	db_name := DefaultGenName("test", repo, DBM.Conf.DBGap)

	db, err := DBM.Get_DBContext(db_name)
	if err != nil {
		t.Error(err.Error())
		return
	}

	op_type = OP_ADD
	file_op_adds, err := Gen_File_Op(GEN_OP_FUNCS, repo, file_name, dir_name, file_type, op_type, file_version, file_hash, file_from, file_to)
	if err != nil {
		t.Error("Gen add op error: ", err)
	}

	err = db.Exec_Trans(file_op_adds)
	if err != nil {
		t.Error(err.Error())
		return
	}

	file := FileVersion{}
	temp_db := db.DBConn.Where(&FileVersion{
		FileName:    file_name,
		DirName:     dir_name,
		FileType:    file_type,
		FileVersion: 1,
	}).First(&file)

	if temp_db.Error != nil {
		t.Error(temp_db.Error.Error())
		return
	}

	op_type = OP_DEL
	file_op_dels, err := Gen_File_Op(GEN_OP_FUNCS, repo, file_name, dir_name, file_type, op_type, file_version, file_hash, file_from, file_to)
	if err != nil {
		t.Error("Gen del op error: ", err)
	}

	err = db.Exec_Trans(file_op_dels)
	if err != nil {
		t.Error(err.Error())
		return
	}

	temp_db = db.DBConn.Where(&FileVersion{
		FileName:    file_name,
		DirName:     dir_name,
		FileType:    file_type,
		FileVersion: 1,
	}).First(&file)

	if temp_db.Error != nil {
		t.Error(temp_db.Error.Error())
		return
	}

	op_type = OP_ADD
	file_op_adds, err = Gen_File_Op(GEN_OP_FUNCS, repo, file_name, dir_name, file_type, op_type, file_version, file_hash, file_from, file_to)
	if err != nil {
		t.Error("Gen add op error: ", err)
	}

	err = db.Exec_Trans(file_op_adds)
	if err != nil {
		t.Error(err.Error())
		return
	}

	file = FileVersion{}
	temp_db = db.DBConn.Where(&FileVersion{
		FileName:    file_name,
		DirName:     dir_name,
		FileType:    file_type,
		FileVersion: 3,
	}).First(&file)

	if temp_db.Error != nil {
		t.Error(temp_db.Error.Error())
		return
	}
}

func Test_Update(t *testing.T) {
	repo := 0
	file_name := "test_file"
	dir_name := "test_dir"
	file_type := 1
	file_version := 1
	file_hash := "sdfasd"
	file_from := "test_from"
	file_to := "test_to"
	var op_type int
	GEN_OP_FUNCS, _, init_err := test_init()
	if init_err != nil {
		t.Error(init_err.Error())
		return
	}
	db_name := DefaultGenName("test", repo, DBM.Conf.DBGap)

	db, err := DBM.Get_DBContext(db_name)
	if err != nil {
		t.Error(err.Error())
		return
	}

	op_type = OP_ADD
	file_op_adds, err := Gen_File_Op(GEN_OP_FUNCS, repo, file_name, dir_name, file_type, op_type, file_version, file_hash, file_from, file_to)
	if err != nil {
		t.Error("Gen add op error: ", err)
	}

	err = db.Exec_Trans(file_op_adds)
	if err != nil {
		t.Error(err.Error())
		return
	}

	file := FileVersion{}
	temp_db := db.DBConn.Where(&FileVersion{
		FileName:    file_name,
		DirName:     dir_name,
		FileType:    file_type,
		FileVersion: 1,
	}).First(&file)

	if temp_db.Error != nil {
		t.Error(temp_db.Error.Error())
		return
	}

	op_type = OP_UPDATE
	file_hash = "fasdfsdf"
	file_version = 1
	file_op_dels, err := Gen_File_Op(GEN_OP_FUNCS, repo, file_name, dir_name, file_type, op_type, file_version, file_hash, file_from, file_to)
	if err != nil {
		t.Error("Gen del op error: ", err)
	}

	err = db.Exec_Trans(file_op_dels)
	if err != nil {
		t.Error(err.Error())
		return
	}

	op_type = OP_UPDATE
	file_version = 1
	file_op_adds, err = Gen_File_Op(GEN_OP_FUNCS, repo, file_name, dir_name, file_type, op_type, file_version, file_hash, file_from, file_to)
	if err != nil {
		t.Error("Gen add op error: ", err)
	}

	err = db.Exec_Trans(file_op_adds)
	if err != nil {
		t.Error("Updat failed: ", err.Error())
		return
	}
}

func Test_Recover(t *testing.T) {
	repo := 0
	file_name := "test_file"
	dir_name := "test_dir"
	file_type := 1
	file_version := 1
	file_hash := "sdfasd"
	file_from := "test_from"
	file_to := "test_to"
	var op_type int
	GEN_OP_FUNCS, _, init_err := test_init()
	if init_err != nil {
		t.Error(init_err.Error())
		return
	}
	db_name := DefaultGenName("test", repo, DBM.Conf.DBGap)

	db, err := DBM.Get_DBContext(db_name)
	if err != nil {
		t.Error(err.Error())
		return
	}

	op_type = OP_ADD
	file_op_adds, err := Gen_File_Op(GEN_OP_FUNCS, repo, file_name, dir_name, file_type, op_type, file_version, file_hash, file_from, file_to)
	if err != nil {
		t.Error("Gen add op error: ", err)
	}

	err = db.Exec_Trans(file_op_adds)
	if err != nil {
		t.Error(err.Error())
		return
	}

	file := FileVersion{}
	temp_db := db.DBConn.Where(&FileVersion{
		FileName:    file_name,
		DirName:     dir_name,
		FileType:    file_type,
		FileVersion: 1,
	}).First(&file)

	if temp_db.Error != nil {
		t.Error(temp_db.Error.Error())
		return
	}

	op_type = OP_UPDATE
	file_version = 1
	update_file_hash := "fasdfsdf"
	file_op_dels, err := Gen_File_Op(GEN_OP_FUNCS, repo, file_name, dir_name, file_type, op_type, file_version, update_file_hash, file_from, file_to)
	if err != nil {
		t.Error("Gen del op error: ", err)
	}

	err = db.Exec_Trans(file_op_dels)
	if err != nil {
		t.Error(err.Error())
		return
	}

	op_type = OP_RECOVER
	file_version = 1
	file_op_adds, err = Gen_File_Op(GEN_OP_FUNCS, repo, file_name, dir_name, file_type, op_type, file_version, update_file_hash, file_from, file_to)
	if err != nil {
		t.Error("Gen add op error: ", err)
	}

	err = db.Exec_Trans(file_op_adds)
	if err != nil {
		t.Error(err.Error())
		return
	}

	file = FileVersion{}
	temp_db = db.DBConn.Where(&FileVersion{
		FileName:    file_name,
		DirName:     dir_name,
		FileType:    file_type,
		FileVersion: 3,
	}).First(&file)

	if temp_db.Error != nil {
		t.Error(temp_db.Error.Error())
		return
	}

	if file.FileHash != file_hash {
		t.Error("error file_hash")
		return
	}
}
func Test_Move(t *testing.T) {
	repo := 1
	file_name := "test_file"
	dir_name1 := "test_dir1"
	dir_name2 := "test_dir2"
	file_type := 2
	file_version := 1
	file_hash := "sdfasd"
	file_from := "test_from"
	file_to := "test_to"
	var op_type int
	GEN_OP_FUNCS, _, init_err := test_init()
	if init_err != nil {
		t.Error(init_err.Error())
		return
	}

	db_name := DefaultGenName("test", repo, DBM.Conf.DBGap)

	db, err := DBM.Get_DBContext(db_name)
	if err != nil {
		t.Error(err.Error())
		return
	}

	op_type = OP_ADD
	file_op_adds, err := Gen_File_Op(GEN_OP_FUNCS, repo, dir_name1, "/", file_type, op_type, file_version, file_hash, file_from, file_to)
	if err != nil {
		t.Error("Gen add op error: ", err)
	}

	err = db.Exec_Trans(file_op_adds)
	if err != nil {
		t.Error(err.Error())
		return
	}

	file := FileVersion{}
	temp_db := db.DBConn.Where(&FileVersion{
		FileName:    dir_name1,
		DirName:     "/",
		FileType:    file_type,
		FileVersion: 1,
	}).First(&file)

	if temp_db.Error != nil {
		t.Error(temp_db.Error.Error())
		return
	}

	op_type = OP_ADD
	update_file_hash := "fasdfsdf"
	file_op_dels, err := Gen_File_Op(GEN_OP_FUNCS, repo, dir_name2, "/", file_type, op_type, file_version, update_file_hash, file_from, file_to)
	if err != nil {
		t.Error("Gen del op error: ", err)
	}

	err = db.Exec_Trans(file_op_dels)
	if err != nil {
		t.Error(err.Error())
		return
	}

	op_type = OP_ADD
	file_type = 1
	file_op_dels, err = Gen_File_Op(GEN_OP_FUNCS, repo, file_name, "/"+dir_name1, file_type, op_type, file_version, update_file_hash, file_from, file_to)
	if err != nil {
		t.Error("Gen del op error: ", err)
	}

	err = db.Exec_Trans(file_op_dels)
	if err != nil {
		t.Error(err.Error())
		return
	}

	op_type = OP_MOVE
	file_version = 1
	file_to = "/" + dir_name2
	file_type = 1
	file_op_adds, err = Gen_File_Op(GEN_OP_FUNCS, repo, file_name, "/"+dir_name1, file_type, op_type, file_version, update_file_hash, file_from, file_to)
	if err != nil {
		t.Error("Gen add op error: ", err)
	}

	err = db.Exec_Trans(file_op_adds)
	if err != nil {
		t.Error(err.Error())
		return
	}

	file = FileVersion{}
	temp_db = db.DBConn.Where(&FileVersion{
		FileName:    file_name,
		DirName:     "/" + dir_name2,
		FileType:    1,
		FileVersion: 1,
	}).First(&file)

	if temp_db.Error != nil {
		t.Error(temp_db.Error.Error())
		return
	}

	if file.From != ("/" + dir_name1) {
		t.Error("error file_from")
		return
	}
}

func Test_Copy(t *testing.T) {
	repo := 0
	file_name := "test_file"
	dir_name1 := "test_dir1"
	dir_name2 := "test_dir2"
	file_type := 2
	file_version := 1
	file_hash := "sdfasd"
	file_from := "test_from"
	file_to := "test_to"
	var op_type int
	GEN_OP_FUNCS, _, init_err := test_init()
	if init_err != nil {
		t.Error(init_err.Error())
		return
	}
	db_name := DefaultGenName("test", repo, DBM.Conf.DBGap)

	db, err := DBM.Get_DBContext(db_name)
	if err != nil {
		t.Error(err.Error())
		return
	}

	op_type = OP_ADD
	file_op_adds, err := Gen_File_Op(GEN_OP_FUNCS, repo, dir_name1, "/", file_type, op_type, file_version, file_hash, file_from, file_to)
	if err != nil {
		t.Error("Gen add op error: ", err)
	}

	err = db.Exec_Trans(file_op_adds)
	if err != nil {
		t.Error(err.Error())
		return
	}

	file := FileVersion{}
	temp_db := db.DBConn.Where(&FileVersion{
		FileName:    dir_name1,
		DirName:     "/",
		FileType:    file_type,
		FileVersion: 1,
	}).First(&file)

	if temp_db.Error != nil {
		t.Error(temp_db.Error.Error())
		return
	}

	op_type = OP_ADD
	update_file_hash := "fasdfsdf"
	file_op_dels, err := Gen_File_Op(GEN_OP_FUNCS, repo, dir_name2, "/", file_type, op_type, file_version, update_file_hash, file_from, file_to)
	if err != nil {
		t.Error("Gen del op error: ", err)
	}

	err = db.Exec_Trans(file_op_dels)
	if err != nil {
		t.Error(err.Error())
		return
	}

	op_type = OP_ADD
	file_type = 1
	file_op_dels, err = Gen_File_Op(GEN_OP_FUNCS, repo, file_name, "/"+dir_name1, file_type, op_type, file_version, update_file_hash, file_from, file_to)
	if err != nil {
		t.Error("Gen del op error: ", err)
	}

	err = db.Exec_Trans(file_op_dels)
	if err != nil {
		t.Error(err.Error())
		return
	}

	op_type = OP_COPY
	file_version = 1
	file_to = "/" + dir_name2
	file_type = 1
	file_op_adds, err = Gen_File_Op(GEN_OP_FUNCS, repo, file_name, "/"+dir_name1, file_type, op_type, file_version, update_file_hash, file_from, file_to)
	if err != nil {
		t.Error("Gen add op error: ", err)
	}

	err = db.Exec_Trans(file_op_adds)
	if err != nil {
		t.Error(err.Error())
		return
	}

	file = FileVersion{}
	temp_db = db.DBConn.Where(&FileVersion{
		FileName:    file_name,
		DirName:     "/" + dir_name2,
		FileType:    1,
		FileVersion: 1,
	}).First(&file)

	if temp_db.Error != nil {
		t.Error(temp_db.Error.Error())
		return
	}

	if file.From != ("/" + dir_name1) {
		t.Error("error file_from")
		return
	}
}
