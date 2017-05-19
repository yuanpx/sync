package main

import (
	"encoding/json"
	"net/http"
	"strconv"
)

//request
type Request struct {
	w http.ResponseWriter
	*http.Request
}

func newRequest(w http.ResponseWriter, r *http.Request) *Request {
	return &Request{w, r}
}

func (r *Request) GetString(key string) string {
	return r.FormValue(key)
}
func (r *Request) GetStringOr(key string, def string) string {
	a := r.FormValue(key)
	if a == "" {
		return def
	}
	return a
}
func (r *Request) GetIntOr(key string, def int) int {
	if n, err := strconv.Atoi(r.FormValue(key)); err == nil {
		return n
	} else {
		return def
	}
}

type response struct {
	State   bool        `json:"state"`
	Code    int         `json:"code"`
	Message string      `json:"message,omitempty"`
	Data    interface{} `json:"data,omitempty"`
}

func (r *Request) WriteError(code int, err string) error {
	resp := response{
		State:   false,
		Code:    code,
		Message: err,
	}
	enc := json.NewEncoder(r.w)
	return enc.Encode(resp)
}

func (r *Request) WriteData(code int, data interface{}) error {
	resp := response{
		State: true,
		Code:  code,
		Data:  data,
	}
	enc := json.NewEncoder(r.w)
	return enc.Encode(resp)
}
