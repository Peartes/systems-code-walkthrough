package memtable

import "errors"

var ErrorAttemptWriteToFrozenMemtable = errors.New("mem: cannot write to frozen memtable")
var ErrorAttemptDeleteFromFrozenMemtable = errors.New("mem: cannot delete from a frozen memtable")
