package rados

// #cgo LDFLAGS: -lrados
// #include <errno.h>
// #include <stdlib.h>
// #include <rados/librados.h>
//
// #if __APPLE__
// #define ceph_time_t __darwin_time_t
// #define ceph_suseconds_t __darwin_suseconds_t
// #elif __GLIBC__
// #define ceph_time_t __time_t
// #define ceph_suseconds_t __suseconds_t
// #else
// #define ceph_time_t time_t
// #define ceph_suseconds_t suseconds_t
// #endif
import "C"

import (
	"time"
	"unsafe"
)

// Snapshots
// NOTE: self managed snapshots have not been implemented yet.

// SnapHead represents the id of "no snapshot" used to resume normal operation when
// reading and writing from a snapshot ioctx.
//
// Note: both the type declaration and the type cast of the C value are required to use
// SnapHead without type checking errors in SetSnapRead.
const SnapHead uint64 = uint64(C.LIBRADOS_SNAP_HEAD)

// SnapCreate creates a pool-wide snapshot
func (ioctx *IOContext) SnapCreate(snapname string) error {
	cSnapname := C.CString(snapname)
	defer C.free(unsafe.Pointer(cSnapname))

	ret := C.rados_ioctx_snap_create(ioctx.ioctx, cSnapname)
	return getError(ret)
}

// SnapRemove deletes a pool snapshot
func (ioctx *IOContext) SnapRemove(snapname string) error {
	cSnapname := C.CString(snapname)
	defer C.free(unsafe.Pointer(cSnapname))

	ret := C.rados_ioctx_snap_remove(ioctx.ioctx, cSnapname)
	return getError(ret)
}

// SnapRollback an object to a pool snapshot
// The contents of the object will be the same as when the snapshot was taken.
func (ioctx *IOContext) SnapRollback(oid, snapname string) error {
	cOid := C.CString(oid)
	defer C.free(unsafe.Pointer(cOid))

	cSnapname := C.CString(snapname)
	defer C.free(unsafe.Pointer(cSnapname))

	ret := C.rados_ioctx_snap_rollback(ioctx.ioctx, cOid, cSnapname)
	return getError(ret)
}

// SetSnapRead sets the snapshot from which reads are performed.
// Subsequent reads will return data as it was at the time of that snapshot.
// Specify the id of the snapshot to set or use SnapHead for no snapshot in order to
// resume normal operation on the ioctx.
func (ioctx *IOContext) SetSnapRead(snap uint64) {
	C.rados_ioctx_snap_set_read(ioctx.ioctx, C.rados_snap_t(snap))
}

// SnapList returns the ids of pool snapshots.
// Specify the maximum length of the returned array (512 by default if zero or negative)
// If the number of snapshots is greater than this length then -ERANGE is returned and
// the user should retry with a larger maxlen.
func (ioctx *IOContext) SnapList(maxlen int) ([]uint64, error) {
	if maxlen < 1 {
		maxlen = 512
	}

	snaps := make([]uint64, maxlen)
	ret := C.rados_ioctx_snap_list(ioctx.ioctx, (*C.rados_snap_t)(unsafe.Pointer(&snaps[0])), C.int(maxlen))
	if err := getErrorIfNegative(ret); err != nil {
		return nil, err
	}

	return snaps[0:int(ret)], nil
}

// SnapLookup gets the id of a pool snapshot
func (ioctx *IOContext) SnapLookup(snapname string) (uint64, error) {
	cSnapname := C.CString(snapname)
	defer C.free(unsafe.Pointer(cSnapname))

	var snap uint64
	ret := C.rados_ioctx_snap_lookup(
		ioctx.ioctx, cSnapname, (*C.rados_snap_t)(unsafe.Pointer(&snap)),
	)
	if err := getError(ret); err != nil {
		return 0, err
	}

	return snap, nil
}

// SnapGetName gets the name of a pool snapshot
func (ioctx *IOContext) SnapGetName(snap uint64) (string, error) {
	buf := make([]byte, 4096)
	ret := C.rados_ioctx_snap_get_name(
		ioctx.ioctx, C.rados_snap_t(snap), (*C.char)(unsafe.Pointer(&buf[0])), C.int(len(buf)),
	)
	if err := getError(ret); err != nil {
		return "", err
	}

	value := C.GoString((*C.char)(unsafe.Pointer(&buf[0])))
	return value, nil
}

// SnapGetStamp finds when a pool snapshot occurred
func (ioctx *IOContext) SnapGetStamp(snap uint64) (time.Time, error) {
	var nsec int64
	ret := C.rados_ioctx_snap_get_stamp(
		ioctx.ioctx, C.rados_snap_t(snap), (*C.time_t)(unsafe.Pointer(&nsec)),
	)
	if err := getError(ret); err != nil {
		return time.Time{}, err
	}

	return time.Unix(0, nsec), nil
}
