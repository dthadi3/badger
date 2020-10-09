/*
 * Copyright 2018 Dgraph Labs, Inc. and Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package badger

import (
	"bytes"
	"context"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dgraph-io/badger/v2/fb"
	"github.com/dgraph-io/badger/v2/y"
	"github.com/dgraph-io/ristretto/z"
	humanize "github.com/dustin/go-humanize"
	fbs "github.com/google/flatbuffers/go"
	"github.com/pkg/errors"
)

const batchSize = 16 << 20 // 16 MB

// maxStreamSize is the maximum allowed size of a stream batch. This is a soft limit
// as a single list that is still over the limit will have to be sent as is since it
// cannot be split further. This limit prevents the framework from creating batches
// so big that sending them causes issues (e.g running into the max size gRPC limit).
var maxStreamSize = int(100 << 20) // 100MB

// Stream provides a framework to concurrently iterate over a snapshot of Badger, pick up
// key-values, batch them up and call Send. Stream does concurrent iteration over many smaller key
// ranges. It does NOT send keys in lexicographical sorted order. To get keys in sorted
// order, use Iterator.
type Stream struct {
	// Prefix to only iterate over certain range of keys. If set to nil (default), Stream would
	// iterate over the entire DB.
	Prefix []byte

	// Number of goroutines to use for iterating over key ranges. Defaults to 16.
	NumGo int

	// Badger would produce log entries in Infof to indicate the progress of Stream. LogPrefix can
	// be used to help differentiate them from other activities. Default is "Badger.Stream".
	LogPrefix string

	// ChooseKey is invoked each time a new key is encountered. Note that this is not called
	// on every version of the value, only the first encountered version (i.e. the highest version
	// of the value a key has). ChooseKey can be left nil to select all keys.
	//
	// Note: Calls to ChooseKey are concurrent.
	ChooseKey func(item *Item) bool

	// KeyToList, similar to ChooseKey, is only invoked on the highest version of the value. It
	// is upto the caller to iterate over the versions and generate zero, one or more KVs. It
	// is expected that the user would advance the iterator to go through the versions of the
	// values. However, the user MUST immediately return from this function on the first encounter
	// with a mismatching key. See example usage in ToList function. Can be left nil to use ToList
	// function by default.
	//
	// Note: Calls to KeyToList are concurrent.
	KeyToList func(key []byte, itr *Iterator) ([]byte, error)

	// This is the method where Stream sends the final output. All calls to Send are done by a
	// single goroutine, i.e. logic within Send method can expect single threaded execution.
	Send func(buf *z.Buffer) error

	readTs       uint64
	db           *DB
	rangeCh      chan keyRange
	kvChan       chan *z.Buffer
	nextStreamId uint32
}

// ToList is a default implementation of KeyToList. It picks up all valid versions of the key,
// skipping over deleted or expired keys.
func (st *Stream) ToList(key []byte, itr *Iterator) ([]byte, error) {
	var kvs []fbs.UOffsetT
	builder := fbs.NewBuilder(1024)
	k := builder.CreateByteVector(key)

	for ; itr.Valid(); itr.Next() {
		item := itr.Item()
		if item.IsDeletedOrExpired() {
			break
		}
		if !bytes.Equal(key, item.Key()) {
			// Break out on the first encounter with another key.
			break
		}

		var v fbs.UOffsetT
		if err := item.Value(func(val []byte) error {
			v = builder.CreateByteVector(val)
			return nil
		}); err != nil {
			return nil, err
		}

		fb.KVStart(builder)
		fb.KVAddKey(builder, k)
		fb.KVAddValue(builder, v)
		fb.KVAddUserMeta(builder, item.UserMeta())
		fb.KVAddVersion(builder, item.Version())
		fb.KVAddExpiresAt(builder, item.ExpiresAt())
		kvs = append(kvs, fb.KVEnd(builder))

		if st.db.opt.NumVersionsToKeep == 1 {
			break
		}

		if item.DiscardEarlierVersions() {
			break
		}
	}
	y.AddListAndFinish(builder, kvs)
	return builder.FinishedBytes(), nil
}

// keyRange is [start, end), including start, excluding end. Do ensure that the start,
// end byte slices are owned by keyRange struct.
func (st *Stream) produceRanges(ctx context.Context) {
	splits := st.db.KeySplits(st.Prefix)

	// We don't need to create more key ranges than NumGo goroutines. This way, we will have limited
	// number of "streams" coming out, which then helps limit the memory used by SSWriter.
	{
		pickEvery := int(math.Floor(float64(len(splits)) / float64(st.NumGo)))
		if pickEvery < 1 {
			pickEvery = 1
		}
		filtered := splits[:0]
		for i, split := range splits {
			if (i+1)%pickEvery == 0 {
				filtered = append(filtered, split)
			}
		}
		splits = filtered
	}

	start := y.SafeCopy(nil, st.Prefix)
	for _, key := range splits {
		st.rangeCh <- keyRange{left: start, right: y.SafeCopy(nil, []byte(key))}
		start = y.SafeCopy(nil, []byte(key))
	}
	// Edge case: prefix is empty and no splits exist. In that case, we should have at least one
	// keyRange output.
	st.rangeCh <- keyRange{left: start}
	close(st.rangeCh)
}

// produceKVs picks up ranges from rangeCh, generates KV lists and sends them to kvChan.
func (st *Stream) produceKVs(ctx context.Context, threadId int) error {
	var size int
	var txn *Txn
	if st.readTs > 0 {
		txn = st.db.NewTransactionAt(st.readTs, false)
	} else {
		txn = st.db.NewTransaction(false)
	}
	defer txn.Discard()

	iterate := func(kr keyRange) error {
		iterOpts := DefaultIteratorOptions
		iterOpts.AllVersions = true
		iterOpts.Prefix = st.Prefix
		iterOpts.PrefetchValues = false
		itr := txn.NewIterator(iterOpts)
		itr.ThreadId = threadId
		defer itr.Close()

		// This unique stream id is used to identify all the keys from this iteration.
		streamId := atomic.AddUint32(&st.nextStreamId, 1)

		bufSize := float64(batchSize) * 1.2
		outList := z.NewBuffer(int(bufSize))

		sendIt := func() error {
			select {
			case st.kvChan <- outList:
			case <-ctx.Done():
				return ctx.Err()
			}
			outList = z.NewBuffer(int(bufSize))
			size = 0
			return nil
		}
		var prevKey []byte
		for itr.Seek(kr.left); itr.Valid(); {
			// it.Valid would only return true for keys with the provided Prefix in iterOpts.
			item := itr.Item()
			if bytes.Equal(item.Key(), prevKey) {
				itr.Next()
				continue
			}
			prevKey = append(prevKey[:0], item.Key()...)

			// Check if we reached the end of the key range.
			if len(kr.right) > 0 && bytes.Compare(item.Key(), kr.right) >= 0 {
				break
			}
			// Check if we should pick this key.
			if st.ChooseKey != nil && !st.ChooseKey(item) {
				continue
			}

			// Now convert to key value.
			list, err := st.KeyToList(item.KeyCopy(nil), itr)
			if err != nil {
				return err
			}
			if len(list) == 0 {
				continue
			}
			kvs := fb.GetRootAsKVList(list, 0)
			var kv fb.KV
			for i := 0; i < kvs.KvsLength(); i++ {
				if !kvs.Kvs(&kv, i) {
					return errors.Errorf("Unable to parse fb.KV at pos: %d", i)
				}
				kv.MutateStreamId(streamId)
			}
			dst := outList.SliceAllocate(len(list))
			copy(dst, list)
			if outList.Len() < batchSize {
				continue
			}
			if err := sendIt(); err != nil {
				return err
			}
		}
		if !outList.IsEmpty() {
			// TODO: Think of a way to indicate that a stream is over.
			if err := sendIt(); err != nil {
				return err
			}
		}
		return nil
	}

	for {
		select {
		case kr, ok := <-st.rangeCh:
			if !ok {
				// Done with the keys.
				return nil
			}
			if err := iterate(kr); err != nil {
				return err
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (st *Stream) streamKVs(ctx context.Context) error {
	var count int
	var bytesSent uint64
	t := time.NewTicker(time.Second)
	defer t.Stop()
	now := time.Now()

	sendBatch := func(batch *z.Buffer) error {
		sz := uint64(batch.Len())
		bytesSent += sz

		batch.SliceIterate(func(slice []byte) error {
			kvs := fb.GetRootAsKVList(slice, 0)
			count += kvs.KvsLength()
			return nil
		})

		t := time.Now()
		if err := st.Send(batch); err != nil {
			return err
		}
		st.db.opt.Infof("%s Created batch of size: %s in %s.\n",
			st.LogPrefix, humanize.Bytes(sz), time.Since(t))
		return nil
	}

	slurp := func(batch *z.Buffer) error {
		defer batch.Release()
	loop:
		for {
			// Send the batch immediately if it already exceeds the maximum allowed size.
			// If the size of the batch exceeds maxStreamSize, break from the loop to
			// avoid creating a batch that is so big that certain limits are reached.
			if batch.Len() > maxStreamSize {
				break loop
			}
			select {
			case buf, ok := <-st.kvChan:
				if !ok {
					break loop
				}
				y.Check2(batch.Write(buf.Bytes()))
				buf.Release()
			default:
				break loop
			}
		}
		return sendBatch(batch)
	}

outer:
	for {
		var batch *z.Buffer
		select {
		case <-ctx.Done():
			return ctx.Err()

		case <-t.C:
			dur := time.Since(now)
			durSec := uint64(dur.Seconds())
			if durSec == 0 {
				continue
			}
			speed := bytesSent / durSec
			st.db.opt.Infof("%s Time elapsed: %s, bytes sent: %s, speed: %s/sec\n", st.LogPrefix,
				y.FixedDuration(dur), humanize.Bytes(bytesSent), humanize.Bytes(speed))

		case buf, ok := <-st.kvChan:
			if !ok {
				break outer
			}
			if buf.IsEmpty() {
				continue
			}
			batch = buf

			// Otherwise, slurp more keys into this batch.
			if err := slurp(batch); err != nil {
				return err
			}
		}
	}

	st.db.opt.Infof("%s Sent %d keys\n", st.LogPrefix, count)
	return nil
}

// Orchestrate runs Stream. It picks up ranges from the SSTables, then runs NumGo number of
// goroutines to iterate over these ranges and batch up KVs in lists. It concurrently runs a single
// goroutine to pick these lists, batch them up further and send to Output.Send. Orchestrate also
// spits logs out to Infof, using provided LogPrefix. Note that all calls to Output.Send
// are serial. In case any of these steps encounter an error, Orchestrate would stop execution and
// return that error. Orchestrate can be called multiple times, but in serial order.
func (st *Stream) Orchestrate(ctx context.Context) error {
	st.rangeCh = make(chan keyRange, 3) // Contains keys for posting lists.

	// kvChan should only have a small capacity to ensure that we don't buffer up too much data if
	// sending is slow. Page size is set to 4MB, which is used to lazily cap the size of each
	// KVList. To get 128MB buffer, we can set the channel size to 32.
	st.kvChan = make(chan *z.Buffer, 32)

	if st.KeyToList == nil {
		st.KeyToList = st.ToList
	}

	// Picks up ranges from Badger, and sends them to rangeCh.
	go st.produceRanges(ctx)

	errCh := make(chan error, 1) // Stores error by consumeKeys.
	var wg sync.WaitGroup
	for i := 0; i < st.NumGo; i++ {
		wg.Add(1)

		go func(threadId int) {
			defer wg.Done()
			// Picks up ranges from rangeCh, generates KV lists, and sends them to kvChan.
			if err := st.produceKVs(ctx, threadId); err != nil {
				select {
				case errCh <- err:
				default:
				}
			}
		}(i)
	}

	// Pick up key-values from kvChan and send to stream.
	kvErr := make(chan error, 1)
	go func() {
		// Picks up KV lists from kvChan, and sends them to Output.
		kvErr <- st.streamKVs(ctx)
	}()
	wg.Wait()        // Wait for produceKVs to be over.
	close(st.kvChan) // Now we can close kvChan.

	select {
	case err := <-errCh: // Check error from produceKVs.
		return err
	default:
	}

	// Wait for key streaming to be over.
	err := <-kvErr
	return err
}

func (db *DB) newStream() *Stream {
	return &Stream{db: db, NumGo: 16, LogPrefix: "Badger.Stream"}
}

// NewStream creates a new Stream.
func (db *DB) NewStream() *Stream {
	if db.opt.managedTxns {
		panic("This API can not be called in managed mode.")
	}
	return db.newStream()
}

// NewStreamAt creates a new Stream at a particular timestamp. Should only be used with managed DB.
func (db *DB) NewStreamAt(readTs uint64) *Stream {
	if !db.opt.managedTxns {
		panic("This API can only be called in managed mode.")
	}
	stream := db.newStream()
	stream.readTs = readTs
	return stream
}
