package lsm

import "errors"

var (
	ErrLSMFileOpen  = errors.New("lsm: cannot open file")
	ErrLSMWALError  = errors.New("lsm: error in WAL")
	ErrLSMDelete    = errors.New("lsm: could not delete key")
	ErrLSMAppend    = errors.New("lsm: could not append new key")
	ErrLSMFileClose = errors.New("lsm: cannot close file")
	ErrManifestFileCorrupt = errors.New("lsm: manifest file corrupted")
	ErrManifestFileSave = errors.New("lsm: could not save manifest file")
)
