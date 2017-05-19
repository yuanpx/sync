package main

import (
	//"encoding/json"
	"errors"
	//"fmt"
	//"github.com/neverlee/glog"
	//"strconv"
	"sync/backend"
	//"time"
)

var (
	RepoError = errors.New("Repo Error")
	DBError   = errors.New("DB error")
)

func init() {
	RegisterHandleFunc("/singleop", SingleHandle)
	RegisterHandleFunc("/batchop", BatchHandle)
}

func SingleHandle(r *Request) {
	repo := r.GetIntOr("repo", -1)
	file_name := r.GetString("file_name")
	dir_name := r.GetString("dir_name")
	file_type := r.GetIntOr("file_type", -1)
	op := r.GetIntOr("op", -1)
	file_version := r.GetIntOr("file_version", -1)
	file_hash := r.GetString("file_hash")
	file_from := r.GetString("file_from")
	file_to := r.GetString("file_to")

	if repo == -1 {
		r.WriteError(-1, RepoError.Error())
		return
	}

	db_name := backend.DefaultGenName("test", repo, backend.DBM.Conf.DBGap)

	db, err := backend.DBM.Get_DBContext(db_name)
	if err != nil {
		r.WriteError(-1, err.Error())
		return
	}

	res_file_ops, err := backend.Gen_File_Op(backend.GEN_OP_FUNCS, repo, file_name, dir_name, file_type, op, file_version, file_hash, file_from, file_to)
	if err != nil {
		r.WriteError(-1, err.Error())
		return
	}

	err = db.Exec_Trans(res_file_ops)
	if err != nil {
		r.WriteError(-1, err.Error())
		return
	}

	r.WriteData(0, "ok")

	return
}

func BatchHandle(r *Request) {
	repo := r.GetIntOr("repo", -1)
	file_name := r.GetString("dir_name")
	dir_name := r.GetString("dir_parent")
	file_type := r.GetIntOr("file_type", -1)
	if file_type != 2 {
		r.WriteError(-1, "wrong file type!")
		return
	}

	op := r.GetIntOr("op", -1)
	file_to := r.GetString("dir_to")
	if repo == -1 {
		r.WriteError(-1, RepoError.Error())
		return
	}

	db_name := backend.DefaultGenName("test", repo, backend.DBM.Conf.DBGap)

	db, err := backend.DBM.Get_DBContext(db_name)
	if err != nil {
		r.WriteError(-1, err.Error())
		return
	}

	tuples, err := backend.Gen_Files_Op(db.DBConn, dir_name, file_name, file_to)
	if err != nil {
		r.WriteError(-1, err.Error())
		return
	}

	file_ops := make([]backend.OpExecute, 0)
	if op == backend.OP_COPY {
		for _, t := range tuples {
			o := &backend.OpFileCopy{}
			o.Gen_From_Tuple(t)
			file_ops = append(file_ops, o)
		}
	} else if op == backend.OP_MOVE {
		for _, t := range tuples {
			o := &backend.OpFileMove{}
			o.Gen_From_Tuple(t)
			file_ops = append(file_ops, o)
		}
	} else if op == backend.OP_DEL {
		for _, t := range tuples {
			o := &backend.OpFileDel{}
			o.Gen_From_Tuple(t)
			file_ops = append(file_ops, o)
		}
	} else {
		r.WriteError(-1, "wrong op!")
		return
	}

	err = db.Exec_Trans(file_ops)
	if err != nil {
		r.WriteError(-1, err.Error())
		return
	}

	r.WriteData(0, "ok")

	return

}
