/*
 * Copyright 2017 Dgraph Labs, Inc. and Contributors
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
	"encoding/hex"
	"fmt"
	"log"
	"math"
	"sort"
	"sync"
	"time"

	"github.com/dgraph-io/badger/v2/table"
	"github.com/dgraph-io/badger/v2/y"
)

type keyRange struct {
	left  []byte
	right []byte
	inf   bool
	count int
}

var infRange = keyRange{inf: true}

func (r keyRange) String() string {
	return fmt.Sprintf("[left=%x, right=%x, inf=%v]", r.left, r.right, r.inf)
}

func (r keyRange) equals(dst keyRange) bool {
	return bytes.Equal(r.left, dst.left) &&
		bytes.Equal(r.right, dst.right) &&
		r.inf == dst.inf
}

func (r keyRange) contains(key []byte) int {
	left := y.CompareKeys(key, r.left)
	if left < 0 {
		// key is left of the r.left
		return -1
	}
	right := y.CompareKeys(key, r.right)
	if right >= 0 {
		// key is right of the right. Can't equal r.right
		return 1
	}
	return 0
}

func (r keyRange) overlapsWith(dst keyRange) bool {
	if r.inf || dst.inf {
		return true
	}

	// If my left is greater than dst right, we have no overlap.
	if y.CompareKeys(r.left, dst.right) > 0 {
		return false
	}
	// If my right is less than dst left, we have no overlap.
	if y.CompareKeys(r.right, dst.left) < 0 {
		return false
	}
	// We have overlap.
	return true
}

// getKeyRange returns the smallest and the biggest in the list of tables.
// TODO(naman): Write a test for this. The smallest and the biggest should
// be the smallest of the leftmost table and the biggest of the right most table.
func getKeyRange(tables ...*table.Table) keyRange {
	if len(tables) == 0 {
		return keyRange{}
	}
	smallest := tables[0].Smallest()
	biggest := tables[0].Biggest()
	for i := 1; i < len(tables); i++ {
		if y.CompareKeys(tables[i].Smallest(), smallest) < 0 {
			smallest = tables[i].Smallest()
		}
		if y.CompareKeys(tables[i].Biggest(), biggest) > 0 {
			biggest = tables[i].Biggest()
		}
	}

	// We pick all the versions of the smallest and the biggest key. Note that version zero would
	// be the rightmost key, considering versions are default sorted in descending order.
	return keyRange{
		left:  y.KeyWithTs(y.ParseKey(smallest), math.MaxUint64),
		right: y.KeyWithTs(y.ParseKey(biggest), 0),
	}
}

type levelCompactStatus struct {
	ranges  []keyRange
	delSize int64
}

func (lcs *levelCompactStatus) debug() string {
	var b bytes.Buffer
	for _, r := range lcs.ranges {
		b.WriteString(r.String())
	}
	return b.String()
}

func (lcs *levelCompactStatus) overlapsWith(dst keyRange) bool {
	for _, r := range lcs.ranges {
		if r.overlapsWith(dst) {
			return true
		}
	}
	return false
}

func (lcs *levelCompactStatus) remove(dst keyRange) bool {
	final := lcs.ranges[:0]
	var found bool
	for _, r := range lcs.ranges {
		if !r.equals(dst) {
			final = append(final, r)
		} else {
			found = true
		}
	}
	lcs.ranges = final
	return found
}

type compactStatus struct {
	sync.RWMutex
	levels []*levelCompactStatus
}

func (cs *compactStatus) overlapsWith(level int, this keyRange) bool {
	cs.RLock()
	defer cs.RUnlock()

	thisLevel := cs.levels[level]
	return thisLevel.overlapsWith(this)
}

func (cs *compactStatus) delSize(l int) int64 {
	cs.RLock()
	defer cs.RUnlock()
	return cs.levels[l].delSize
}

type thisAndNextLevelRLocked struct{}

// compareAndAdd will check whether we can run this compactDef. That it doesn't overlap with any
// other running compaction. If it can be run, it would store this run in the compactStatus state.
func (cs *compactStatus) compareAndAdd(_ thisAndNextLevelRLocked, cd compactDef) bool {
	cs.Lock()
	defer cs.Unlock()

	level := cd.thisLevel.level

	y.AssertTruef(level < len(cs.levels)-1, "Got level %d. Max levels: %d", level, len(cs.levels))
	thisLevel := cs.levels[level]
	nextLevel := cs.levels[level+1]

	if thisLevel.overlapsWith(cd.thisRange) {
		return false
	}
	if nextLevel.overlapsWith(cd.nextRange) {
		return false
	}
	// Check whether this level really needs compaction or not. Otherwise, we'll end up
	// running parallel compactions for the same level.
	// Update: We should not be checking size here. Compaction priority already did the size checks.
	// Here we should just be executing the wish of others.

	thisLevel.ranges = append(thisLevel.ranges, cd.thisRange)
	nextLevel.ranges = append(nextLevel.ranges, cd.nextRange)
	thisLevel.delSize += cd.thisSize
	return true
}

func (cs *compactStatus) delete(cd compactDef) {
	cs.Lock()
	defer cs.Unlock()

	level := cd.thisLevel.level
	y.AssertTruef(level < len(cs.levels)-1, "Got level %d. Max levels: %d", level, len(cs.levels))

	thisLevel := cs.levels[level]
	nextLevel := cs.levels[level+1]

	thisLevel.delSize -= cd.thisSize
	found := thisLevel.remove(cd.thisRange)
	found = nextLevel.remove(cd.nextRange) && found

	if !found {
		this := cd.thisRange
		next := cd.nextRange
		fmt.Printf("Looking for: [%q, %q, %v] in this level.\n", this.left, this.right, this.inf)
		fmt.Printf("This Level:\n%s\n", thisLevel.debug())
		fmt.Println()
		fmt.Printf("Looking for: [%q, %q, %v] in next level.\n", next.left, next.right, next.inf)
		fmt.Printf("Next Level:\n%s\n", nextLevel.debug())
		log.Fatal("keyRange not found")
	}
}

func (s *levelsController) runCompactDef(l int, cd compactDef) (err error) {
	timeStart := time.Now()

	thisLevel := cd.thisLevel
	nextLevel := cd.nextLevel

	// Table should never be moved directly between levels, always be rewritten to allow discarding
	// invalid versions.

	newTables, decr, err := s.compactBuildTables(l, cd)
	if err != nil {
		return err
	}
	defer func() {
		// Only assign to err, if it's not already nil.
		if decErr := decr(); err == nil {
			err = decErr
		}
	}()
	changeSet := buildChangeSet(&cd, newTables)

	// We write to the manifest _before_ we delete files (and after we created files)
	if err := s.kv.manifest.addChanges(changeSet.Changes); err != nil {
		return err
	}

	// See comment earlier in this function about the ordering of these ops, and the order in which
	// we access levels when reading.
	if err := nextLevel.replaceTables(cd.bot, newTables); err != nil {
		return err
	}
	if err := thisLevel.deleteTables(cd.top); err != nil {
		return err
	}

	// Note: For level 0, while doCompact is running, it is possible that new tables are added.
	// However, the tables are added only to the end, so it is ok to just delete the first table.

	if dur := time.Since(timeStart); dur > time.Millisecond {
		s.kv.opt.Infof("LOG Compact %d->%d, del %d tables, add %d tables, took %v\n",
			thisLevel.level, nextLevel.level, len(cd.top)+len(cd.bot),
			len(newTables), dur.Truncate(time.Second))
	}

	if cd.thisLevel.level != 0 && len(newTables) > 2*s.kv.opt.LevelSizeMultiplier {
		s.kv.opt.Infof("This Range (numTables: %d)\nLeft:\n%s\nRight:\n%s\n",
			len(cd.top), hex.Dump(cd.thisRange.left), hex.Dump(cd.thisRange.right))
		s.kv.opt.Infof("Next Range (numTables: %d)\nLeft:\n%s\nRight:\n%s\n",
			len(cd.bot), hex.Dump(cd.nextRange.left), hex.Dump(cd.nextRange.right))
	}
	return nil
}

// compactBuildTables merges topTables and botTables to form a list of new tables.
func (s *levelsController) compactBuildTables(
	lev int, cd compactDef) ([]*table.Table, func() error, error) {
	topTables := cd.top
	botTables := cd.bot

	numTables := int64(len(topTables) + len(botTables))
	y.NumCompactionTables.Add(numTables)
	defer y.NumCompactionTables.Add(-numTables)

	// Check overlap of the top level with the levels which are not being
	// compacted in this compaction.
	hasOverlap := s.checkOverlap(cd.allTables(), cd.nextLevel.level+1)

	// Try to collect stats so that we can inform value log about GC. That would help us find which
	// value log file should be GCed.
	discardStats := make(map[uint32]int64)
	updateStats := func(vs y.ValueStruct) {
		// We don't need to store/update discard stats when badger is running in Disk-less mode.
		if s.kv.opt.InMemory {
			return
		}
		if vs.Meta&bitValuePointer > 0 {
			var vp valuePointer
			vp.Decode(vs.Value)
			discardStats[vp.Fid] += int64(vp.Len)
		}
	}

	// Create iterators across all the tables involved first.
	var iters []y.Iterator
	switch {
	case lev == 0:
		iters = appendIteratorsReversed(iters, topTables, table.NOCACHE)
	case len(topTables) > 0:
		y.AssertTrue(len(topTables) == 1)
		iters = []y.Iterator{topTables[0].NewIterator(table.NOCACHE)}
	}

	analyze := func() {
		if len(iters) == 0 {
			return
		}
		it := table.NewMergeIterator(iters, false)
		it.Rewind()

		var ranges []*keyRange
		for _, table := range botTables {
			left := y.Copy(table.Smallest())
			for _, ks := range table.KeySplits(10000, nil) {
				kr := &keyRange{
					left:  left,
					right: []byte(ks),
				}
				left = y.Copy(kr.right)
				ranges = append(ranges, kr)
			}
			// kr := &keyRange{
			// 	left:  table.Smallest(),
			// 	right: table.Biggest(),
			// }
		}
		var rangeIdx int

		var numKeys int
		for it.Valid() {
			numKeys++
			if rangeIdx >= len(ranges) {
				it.Next()
				continue
			}
			kr := ranges[rangeIdx]
			c := kr.contains(it.Key())
			if c > 0 {
				numKeys--
				rangeIdx++
				continue
			}
			if c == 0 {
				kr.count++
			} else {
				// ignore the key to the left of the range.
			}
			it.Next()
		}

		var noop, changed int
		for _, kr := range ranges {
			if kr.count == 0 {
				noop++
			} else {
				changed++
				// fmt.Printf("left: %x right: %x count: %d\n",
				// 	kr.left[:8], kr.right[:8], kr.count)
			}
		}
		fmt.Printf("numKeys: %d. Num bot tables: %d [%d -> %d] num blocks NOOP: %d (%.2f %%) changed: %d\n\n",
			numKeys, len(cd.bot), cd.thisLevel.level, cd.nextLevel.level,
			noop, float64(noop*100)/float64(noop+changed), changed)
	}
	analyze()

	// Next level has level>=1 and we can use ConcatIterator as key ranges do not overlap.
	var valid []*table.Table

nextTable:
	for _, table := range botTables {
		if len(cd.dropPrefixes) > 0 {
			for _, prefix := range cd.dropPrefixes {
				if bytes.HasPrefix(table.Smallest(), prefix) &&
					bytes.HasPrefix(table.Biggest(), prefix) {
					// All the keys in this table have the dropPrefix. So, this
					// table does not need to be in the iterator and can be
					// dropped immediately.
					continue nextTable
				}
			}
		}
		valid = append(valid, table)
	}
	iters = append(iters, table.NewConcatIterator(valid, table.NOCACHE))
	it := table.NewMergeIterator(iters, false)
	defer it.Close() // Important to close the iterator to do ref counting.

	it.Rewind()

	// Pick a discard ts, so we can discard versions below this ts. We should
	// never discard any versions starting from above this timestamp, because
	// that would affect the snapshot view guarantee provided by transactions.
	discardTs := s.kv.orc.discardAtOrBelow()

	var numBuilds, numVersions int
	var lastKey, skipKey []byte
	var vp valuePointer
	var newTables []*table.Table
	mu := new(sync.Mutex) // Guards newTables

	inflightBuilders := y.NewThrottle(5)
	for it.Valid() {
		timeStart := time.Now()
		dk, err := s.kv.registry.latestDataKey()
		if err != nil {
			return nil, nil,
				y.Wrapf(err, "Error while retrieving datakey in levelsController.compactBuildTables")
		}
		bopts := buildTableOptions(s.kv.opt)
		bopts.DataKey = dk
		// Builder does not need cache but the same options are used for opening table.
		bopts.BlockCache = s.kv.blockCache
		bopts.IndexCache = s.kv.indexCache
		builder := table.NewTableBuilder(bopts)
		var numKeys, numSkips uint64
		for ; it.Valid(); it.Next() {
			// See if we need to skip the prefix.
			if len(cd.dropPrefixes) > 0 && hasAnyPrefixes(it.Key(), cd.dropPrefixes) {
				numSkips++
				updateStats(it.Value())
				continue
			}

			// See if we need to skip this key.
			if len(skipKey) > 0 {
				if y.SameKey(it.Key(), skipKey) {
					numSkips++
					updateStats(it.Value())
					continue
				} else {
					skipKey = skipKey[:0]
				}
			}

			if !y.SameKey(it.Key(), lastKey) {
				if builder.ReachedCapacity(uint64(float64(s.kv.opt.MaxTableSize) * 0.9)) {
					// Only break if we are on a different key, and have reached capacity. We want
					// to ensure that all versions of the key are stored in the same sstable, and
					// not divided across multiple tables at the same level.
					break
				}
				lastKey = y.SafeCopy(lastKey, it.Key())
				numVersions = 0
			}

			vs := it.Value()
			version := y.ParseTs(it.Key())
			// Do not discard entries inserted by merge operator. These entries will be
			// discarded once they're merged
			if version <= discardTs && vs.Meta&bitMergeEntry == 0 {
				// Keep track of the number of versions encountered for this key. Only consider the
				// versions which are below the minReadTs, otherwise, we might end up discarding the
				// only valid version for a running transaction.
				numVersions++

				// Keep the current version and discard all the next versions if
				// - The `discardEarlierVersions` bit is set OR
				// - We've already processed `NumVersionsToKeep` number of versions
				// (including the current item being processed)
				lastValidVersion := vs.Meta&bitDiscardEarlierVersions > 0 ||
					numVersions == s.kv.opt.NumVersionsToKeep

				isExpired := isDeletedOrExpired(vs.Meta, vs.ExpiresAt)

				if isExpired || lastValidVersion {
					// If this version of the key is deleted or expired, skip all the rest of the
					// versions. Ensure that we're only removing versions below readTs.
					skipKey = y.SafeCopy(skipKey, it.Key())

					switch {
					// Add the key to the table only if it has not expired.
					// We don't want to add the deleted/expired keys.
					case !isExpired && lastValidVersion:
						// Add this key. We have set skipKey, so the following key versions
						// would be skipped.
					case hasOverlap:
						// If this key range has overlap with lower levels, then keep the deletion
						// marker with the latest version, discarding the rest. We have set skipKey,
						// so the following key versions would be skipped.
					default:
						// If no overlap, we can skip all the versions, by continuing here.
						numSkips++
						updateStats(vs)
						continue // Skip adding this key.
					}
				}
			}
			numKeys++
			if vs.Meta&bitValuePointer > 0 {
				vp.Decode(vs.Value)
			}
			builder.Add(it.Key(), vs, vp.Len)
		}
		// It was true that it.Valid() at least once in the loop above, which means we
		// called Add() at least once, and builder is not Empty().
		s.kv.opt.Debugf("LOG Compact. Added %d keys. Skipped %d keys. Iteration took: %v",
			numKeys, numSkips, time.Since(timeStart))
		if builder.Empty() {
			// Cleanup builder resources:
			builder.Finish(false)
			builder.Close()
			continue
		}
		numBuilds++
		fileID := s.reserveFileID()
		if err := inflightBuilders.Do(); err != nil {
			// Can't return from here, until I decrRef all the tables that I built so far.
			break
		}
		go func(builder *table.Builder) {
			defer builder.Close()
			defer inflightBuilders.Done(err)

			build := func(fileID uint64) (*table.Table, error) {
				fname := table.NewFilename(fileID, s.kv.opt.Dir)
				return table.CreateTable(fname, builder.Finish(false), bopts)
			}

			var tbl *table.Table
			var err error
			if s.kv.opt.InMemory {
				tbl, err = table.OpenInMemoryTable(builder.Finish(true), fileID, &bopts)
			} else {
				tbl, err = build(fileID)
			}

			// If we couldn't build the table, return fast.
			if err != nil {
				return
			}

			mu.Lock()
			newTables = append(newTables, tbl)
			// num := atomic.LoadInt32(&table.NumBlocks)
			mu.Unlock()

			// TODO(ibrahim): When ristretto PR #186 merges, bring this back.
			// s.kv.opt.Debugf("Num Blocks: %d. Num Allocs (MB): %.2f\n", num, (z.NumAllocBytes() / 1 << 20))
		}(builder)
	}

	// Wait for all table builders to finish and also for newTables accumulator to finish.
	err := inflightBuilders.Finish()
	if err == nil {
		// Ensure created files' directory entries are visible.  We don't mind the extra latency
		// from not doing this ASAP after all file creation has finished because this is a
		// background operation.
		err = s.kv.syncDir(s.kv.opt.Dir)
	}

	if err != nil {
		// An error happened.  Delete all the newly created table files (by calling DecrRef
		// -- we're the only holders of a ref).
		_ = decrRefs(newTables)
		return nil, nil, y.Wrapf(err, "while running compactions for: %+v", cd)
	}

	sort.Slice(newTables, func(i, j int) bool {
		return y.CompareKeys(newTables[i].Biggest(), newTables[j].Biggest()) < 0
	})
	s.kv.vlog.updateDiscardStats(discardStats)
	s.kv.opt.Debugf("Discard stats: %v", discardStats)
	return newTables, func() error { return decrRefs(newTables) }, nil
}
