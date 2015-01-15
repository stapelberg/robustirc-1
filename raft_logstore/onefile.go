package raft_logstore

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math"
	"os"
	"path/filepath"
	"sync"

	"github.com/hashicorp/raft"
	"golang.org/x/sys/unix"
)

var bufPool = sync.Pool{
	New: func() interface{} {
		return new(bytes.Buffer)
	},
}

type OneFile struct {
	dir string

	// l protects everything below. dir is never modified
	l         sync.RWMutex
	lock      *os.File
	file      *os.File
	lowIndex  uint64
	highIndex uint64
	logs      map[uint64]*raft.Log
}

func NewOneFile(dir string) (s *OneFile, err error) {
	s = &OneFile{}

	s.dir = filepath.Join(dir, "robustlogs")
	if err := os.MkdirAll(filepath.Join(dir, "robustlogs"), 0700); err != nil {
		return nil, err
	}

	s.lock, err = os.OpenFile(filepath.Join(dir, "lock"), os.O_CREATE, 0700)
	if err != nil {
		return nil, fmt.Errorf("could not open lock: %v", err)
	}
	defer func() {
		if err != nil {
			s.lock.Close()
		}
	}()

	if err = unix.Flock(int(s.lock.Fd()), unix.LOCK_EX); err != nil {
		return nil, fmt.Errorf("could not acquire lock: %v", err)
	}

	if s.file, err = os.OpenFile(filepath.Join(dir, "logfile"), os.O_CREATE|os.O_RDWR, 0700); err != nil {
		return nil, fmt.Errorf("could not open logfile: %v", err)
	}
	defer func() {
		if err != nil {
			s.file.Close()
		}
	}()

	var buf []byte
	if buf, err = ioutil.ReadAll(s.file); err != nil {
		return nil, fmt.Errorf("could not read logfile: %v", err)
	}

	s.lowIndex = math.MaxUint64
	s.logs = make(map[uint64]*raft.Log)
	for {
		if len(buf) < 4 {
			// If we can't seek, an incomplete write corrupted the log. We just
			// skip back to the last boundary.
			s.file.Seek(int64(-len(buf)), 1)
			break
		}
		ul := binary.LittleEndian.Uint32(buf[:4])
		if ul > math.MaxInt32 {
			return nil, fmt.Errorf("too big a log entry found")
		}

		l := int(ul)
		// Overflow check. l is an int, len(buf) >= 4 is an int, so no over- or
		// underflows here and l+4 also does not overflow.
		if l <= 0 || l > len(buf)-4 {
			s.file.Seek(int64(-len(buf)), 1)
			break
		}
		var elogs []*raft.Log
		if err := json.NewDecoder(bytes.NewReader(buf[4 : l+4])).Decode(&elogs); err != nil {
			s.file.Seek(int64(-len(buf)), 1)
			break
		}
		buf = buf[l+4:]

		for _, elog := range elogs {
			if _, ok := s.logs[elog.Index]; ok {
				return nil, fmt.Errorf("duplicate log entry %d", elog.Index)
			}
			if elog.Index < s.lowIndex {
				s.lowIndex = elog.Index
			}
			if elog.Index > s.highIndex {
				s.highIndex = elog.Index
			}
			s.logs[elog.Index] = elog
		}
	}

	return s, nil
}

func (s *OneFile) Close() error {
	s.l.Lock()
	defer s.l.Unlock()

	// Make Close idempotent
	if s.lock == nil || s.file == nil {
		return nil
	}

	err1 := s.lock.Close()
	err2 := s.file.Close()
	s.lock = nil
	s.file = nil

	if err1 != nil {
		return err1
	}
	return err2
}

func (s *OneFile) GetAll() ([]uint64, error) {
	s.l.RLock()
	defer s.l.RUnlock()

	indexes := make([]uint64, 0, int(s.highIndex-s.lowIndex))
	for k := range s.logs {
		indexes = append(indexes, k)
	}
	return indexes, nil
}

func (s *OneFile) FirstIndex() (uint64, error) {
	s.l.RLock()
	defer s.l.RUnlock()
	return s.lowIndex, nil
}

func (s *OneFile) LastIndex() (uint64, error) {
	s.l.RLock()
	defer s.l.RUnlock()
	return s.highIndex, nil
}

func (s *OneFile) GetLog(index uint64, rlog *raft.Log) error {
	s.l.RLock()
	defer s.l.RUnlock()

	if elog, ok := s.logs[index]; ok {
		*rlog = *elog
		return nil
	} else {
		return raft.ErrLogNotFound
	}
}

func (s *OneFile) StoreLog(log *raft.Log) error {
	return s.StoreLogs([]*raft.Log{log})
}

func (s *OneFile) StoreLogs(logs []*raft.Log) error {
	s.l.Lock()
	defer s.l.Unlock()

	log.Printf("Writing %d logs to buffer", len(logs))
	b := bufPool.Get().(*bytes.Buffer)
	defer bufPool.Put(b)

	if err := json.NewEncoder(b).Encode(logs); err != nil {
		log.Printf("Could not encode logs: %v", err)
		return err
	}
	log.Printf("Committing %d bytes", b.Len())

	if err := binary.Write(s.file, binary.LittleEndian, uint32(b.Len())); err != nil {
		log.Printf("Could not write size of transaction: %v", err)
		return fmt.Errorf("could not write size of transaction: %v", err)
	}
	_, err := io.Copy(s.file, b)
	if err != nil {
		log.Printf("Could not write transaction data: %v", err)
		return fmt.Errorf("could not write transaction data: %v", err)
	}
	// We sync the file, just to be sure.
	//	if err = s.file.Sync(); err != nil {
	//		return err
	//	}

	// Add logs to in-memory cache
	log.Printf("Writing to in-memory store")
	for _, elog := range logs {
		s.logs[elog.Index] = elog
	}

	log.Printf("Done")
	return nil
}

func (s *OneFile) DeleteRange(min, max uint64) error {
	s.l.Lock()
	defer s.l.Unlock()

	var elogs []*raft.Log
	for _, elog := range s.logs {
		if elog.Index < min && elog.Index > max {
			elogs = append(elogs)
		}
	}

	f, err := os.OpenFile(filepath.Join(s.dir, "log.copy"), os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0700)
	if err != nil {
		return err
	}
	defer func() {
		os.Remove(filepath.Join(s.dir, "log.copy"))
		if err != nil {
			f.Close()
		}
	}()

	if len(elogs) > 0 {
		b := bufPool.Get().(*bytes.Buffer)
		defer bufPool.Put(b)
		if err := json.NewEncoder(b).Encode(elogs); err != nil {
			return err
		}
		if err := binary.Write(f, binary.LittleEndian, uint32(b.Len())); err != nil {
			return err
		}
		if _, err := io.Copy(f, b); err != nil {
			return err
		}
		//		if err := f.Sync(); err != nil {
		//			return err
		//		}
	}

	if err := os.Rename(filepath.Join(s.dir, "log.copy"), filepath.Join(s.dir, "log")); err != nil {
		return err
	}

	// From now on, no failing operation may happen, as the logs are already
	// deleted on disc. We update the in-memory data structures
	for i := min; i <= max; i++ {
		// If the key does not exist, delete is a no-op
		delete(s.logs, i)
	}
	s.lowIndex = max + 1
	if err := s.file.Close(); err != nil {
		// An error, when closing the old file, means, some data from a
		// previous write may be lost. We have overwritten this file anyway.
		// Log the error for good measure
		log.Printf("Ignored error when closing old log: %v", err)
	}
	s.file = f

	return nil
}

func (s *OneFile) DeleteAll() error {
	s.l.Lock()
	defer s.l.Unlock()

	f, err := os.OpenFile(filepath.Join(s.dir, "log.copy"), os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0700)
	if err != nil {
		return err
	}
	defer func() {
		os.Remove(filepath.Join(s.dir, "log.copy"))
		if err != nil {
			f.Close()
		}
	}()

	if err := os.Rename(filepath.Join(s.dir, "log.copy"), filepath.Join(s.dir, "log")); err != nil {
		return err
	}

	// From now on, no failing operation may happen, as the logs are already
	// deleted on disc. We update the in-memory data structures
	s.lowIndex = 0
	s.highIndex = 0
	s.logs = make(map[uint64]*raft.Log)
	if err := s.file.Close(); err != nil {
		// An error, when closing the old file, means, some data from a
		// previous write may be lost. We have overwritten this file anyway.
		// Log the error for good measure
		log.Printf("Ignored error when closing old log: %v", err)
	}
	s.file = f

	return nil
}
