//  Copyright (c) 2014 Couchbase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// 		http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package collector

import (
	"context"
	"log"
	"reflect"
	"strconv"
	"sync"
	"time"

	"github.com/blevesearch/bleve/index"
	"github.com/blevesearch/bleve/search"
	"github.com/blevesearch/bleve/search/highlight"
	"github.com/blevesearch/bleve/size"
)

var reflectStaticSizeStreamCollector int

func init() {
	var coll StreamCollector
	reflectStaticSizeStreamCollector = int(reflect.TypeOf(coll).Size())
}

// TopNCollector collects the top N hits, optionally skipping some results
type StreamCollector struct {
	size     int
	skip     int
	total    uint64
	maxScore float64
	took     time.Duration
	sort     search.SortOrder
	//results       search.DocumentMatchCollection
	facetsBuilder *search.FacetsBuilder

	needDocIds    bool
	neededFields  []string
	cachedScoring []bool
	cachedDesc    []bool

	//lowestMatchOutsideResults *search.DocumentMatch
	updateFieldVisitor index.DocumentFieldTermVisitor
	dvReader           index.DocValueReader
	searchAfter        *search.DocumentMatch
}

// NewTopNCollector builds a collector to find the top 'size' hits
// skipping over the first 'skip' hits
// ordering hits by the provided sort order
func NewStreamCollectorAfter(size int, sort search.SortOrder, after []string) *StreamCollector {
	rv := newStreamCollector(size, 0, sort)
	rv.searchAfter = &search.DocumentMatch{
		Sort: after,
	}

	for pos, ss := range sort {
		if ss.RequiresDocID() {
			rv.searchAfter.ID = after[pos]
		}
		if ss.RequiresScoring() {
			if score, err := strconv.ParseFloat(after[pos], 64); err == nil {
				rv.searchAfter.Score = score
			}
		}
	}

	return rv
}

// NewStreamCollector builds a collector to find the top 'size' hits
// skipping over the first 'skip' hits
// ordering hits by the provided sort order
func NewStreamCollector(size int, skip int, sort search.SortOrder) *StreamCollector {
	return newStreamCollector(size, skip, sort)
}

func newStreamCollector(size int, skip int, sort search.SortOrder) *StreamCollector {
	hc := &StreamCollector{size: size, skip: skip, sort: sort}

	// these lookups traverse an interface, so do once up-front
	if sort.RequiresDocID() {
		hc.needDocIds = true
	}

	hc.needDocIds = false
	hc.neededFields = sort.RequiredFields()
	hc.cachedScoring = sort.CacheIsScore()
	hc.cachedDesc = sort.CacheDescending()

	return hc
}

func (hc *StreamCollector) Size() int {
	return reflectStaticSizeStreamCollector + size.SizeOfPtr
}

// Collect goes to the index to find the matching documents
func (hc *StreamCollector) Collect(ctx context.Context, searcher search.Searcher, reader index.IndexReader) error {
	startTime := time.Now()
	var err error
	var next *search.DocumentMatch

	// pre-allocate enough space in the DocumentMatchPool
	// unless the size + skip is too large, then cap it
	// everything should still work, just allocates DocumentMatches on demand
	hc.dvReader, err = reader.DocValueReader(hc.neededFields)
	if err != nil {
		return err
	}

	hc.updateFieldVisitor = func(field string, term []byte) {
		if hc.facetsBuilder != nil {
			hc.facetsBuilder.UpdateVisitor(field, term)
		}
		hc.sort.UpdateVisitor(field, term)
	}

	dmHandlerMaker := MakeTopNDocumentMatchHandler
	if cv := ctx.Value(search.MakeDocumentMatchHandlerKey); cv != nil {
		dmHandlerMaker = cv.(search.MakeDocumentMatchHandler)
	}

	backingSize := hc.size + hc.skip + 1
	if hc.size+hc.skip > PreAllocSizeSkipCap {
		backingSize = PreAllocSizeSkipCap + 1
	}
	searchContext := &search.SearchContext{
		PoolSize:     backingSize,
		SearcherSize: searcher.DocumentMatchPoolSize(),
		SortSize:     len(hc.sort),
		IndexReader:  reader,
	}

	// use the application given builder for making the custom document match
	// handler and perform callbacks/invocations on the newly made handler.
	dmHandler, loadID, err := dmHandlerMaker(searchContext)
	if err != nil {
		return err
	}

	hc.needDocIds = hc.needDocIds || loadID

	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		next, err = searcher.Next(searchContext)
	}
	for err == nil && next != nil {
		if hc.total%CheckDoneEvery == 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
		}

		err = hc.prepareDocumentMatch(searchContext, reader, next)
		if err != nil {
			break
		}

		err = dmHandler(next)
		if err != nil {
			break
		}

		next, err = searcher.Next(searchContext)
	}

	// help finalize/flush the results in case
	// of custom document match handlers.
	err = dmHandler(nil)
	if err != nil {
		return err
	}

	// compute search duration
	hc.took = time.Since(startTime)
	if err != nil {
		return err
	}
	/* finalize actual results
	err = hc.finalizeResults(reader)
	if err != nil {
		return err
	}*/
	return nil
}

func (hc *StreamCollector) prepareDocumentMatch(ctx *search.SearchContext,
	reader index.IndexReader, d *search.DocumentMatch) (err error) {

	// visit field terms for features that require it (sort, facets)
	if len(hc.neededFields) > 0 {
		err = hc.visitFieldTerms(reader, d)
		if err != nil {
			return err
		}
	}

	d.DocReader = reader

	// increment total hits
	hc.total++
	d.HitNumber = hc.total

	// update max score
	if d.Score > hc.maxScore {
		hc.maxScore = d.Score
	}

	// see if we need to load ID (at this early stage, for example to sort on it)
	//if hc.needDocIds {

	/*d.ID, err = reader.ExternalID(d.IndexInternalID)
	if err != nil {
		return err
	}
	log.Printf("loading docID %s", d.ID)*/
	//}

	// compute this hits sort value
	if len(hc.sort) == 1 && hc.cachedScoring[0] {
		d.Sort = sortByScoreOpt
	} else {
		hc.sort.Value(d)
	}

	return nil
}

// visitFieldTerms is responsible for visiting the field terms of the
// search hit, and passing visited terms to the sort and facet builder
func (hc *StreamCollector) visitFieldTerms(reader index.IndexReader, d *search.DocumentMatch) error {
	if hc.facetsBuilder != nil {
		hc.facetsBuilder.StartDoc()
	}

	err := hc.dvReader.VisitDocValues(d.IndexInternalID, hc.updateFieldVisitor)
	if hc.facetsBuilder != nil {
		hc.facetsBuilder.EndDoc()
	}

	return err
}

// SetFacetsBuilder registers a facet builder for this collector
func (hc *StreamCollector) SetFacetsBuilder(facetsBuilder *search.FacetsBuilder) {
	hc.facetsBuilder = facetsBuilder
	hc.neededFields = append(hc.neededFields, hc.facetsBuilder.RequiredFields()...)
}

// finalizeResults starts with the heap containing the final top size+skip
// it now throws away the results to be skipped
// and does final doc id lookup (if necessary)
/*func (hc *StreamCollector) finalizeResults(r index.IndexReader) error {
	/*var err error
	hc.results, err = hc.store.Final(hc.skip, func(doc *search.DocumentMatch) error {
		if doc.ID == "" {
			// look up the id since we need it for lookup
			var err error
			doc.ID, err = r.ExternalID(doc.IndexInternalID)
			if err != nil {
				return err
			}
		}
		doc.Complete(nil)
		return nil
	})

	return nil
}*/

// Results returns the collected hits
func (hc *StreamCollector) Results() search.DocumentMatchCollection {
	return nil
}

// Total returns the total number of hits
func (hc *StreamCollector) Total() uint64 {
	return hc.total
}

// MaxScore returns the maximum score seen across all the hits
func (hc *StreamCollector) MaxScore() float64 {
	return hc.maxScore
}

// Took returns the time spent collecting hits
func (hc *StreamCollector) Took() time.Duration {
	return hc.took
}

// FacetResults returns the computed facets results
func (hc *StreamCollector) FacetResults() search.FacetResults {
	if hc.facetsBuilder != nil {
		return hc.facetsBuilder.Results()
	}
	return nil
}

type SuperCollector struct {
	m           sync.Mutex
	ctx         *search.SearchContext
	highlighter highlight.Highlighter
	coll        *TopNCollector

	total    uint64
	maxScore float64
	readers  []index.IndexReader
}

func (sc *SuperCollector) MakeStreamingDocumentMatchHandler(
	ctx *search.SearchContext) (search.DocumentMatchHandler, bool, error) {
	sc.SetContext(ctx)
	return sc.DocumentMatchHandler, false, nil
}

func (sc *SuperCollector) SetContext(ctx *search.SearchContext) {
	sc.m.Lock()
	if sc.ctx != nil {
		sc.readers = append(sc.readers, ctx.IndexReader)
		ctx.DocumentMatchPool = sc.ctx.DocumentMatchPool
		sc.m.Unlock()
		return
	}
	sc.readers = append(sc.readers, ctx.IndexReader)
	sc.ctx = &search.SearchContext{DocumentMatchPool: search.NewDocumentMatchPool(ctx.PoolSize+ctx.SearcherSize*6, ctx.SortSize)}
	ctx.DocumentMatchPool = sc.ctx.DocumentMatchPool
	sc.m.Unlock()
}

func NewSuperCollector(sort search.SortOrder,
	hl highlight.Highlighter, from, size int, searchAfter []string) *SuperCollector {
	dmh := &SuperCollector{
		highlighter: hl,
	}

	if searchAfter != nil {
		dmh.coll = NewTopNCollectorAfter(size, sort, searchAfter)
	} else {
		dmh.coll = NewTopNCollector(size, from, sort)
	}

	return dmh
}

func (hc *SuperCollector) CloseReaders() {
	for _, r := range hc.readers {
		_ = r.Close()
	}
}

// Total returns the total number of hits
func (hc *SuperCollector) Total() uint64 {
	return hc.coll.total
}

// MaxScore returns the maximum score seen across all the hits
func (hc *SuperCollector) MaxScore() float64 {
	return hc.coll.maxScore
}

// Results returns the collected hits
func (hc *SuperCollector) Results() search.DocumentMatchCollection {
	err := hc.finalizeResults()
	if err != nil {
		log.Printf("results err %v\n", err)
	}
	return hc.coll.results
}

func (hc *SuperCollector) finalizeResults() error {
	var err error
	hc.coll.results, err = hc.coll.store.Final(0, func(doc *search.DocumentMatch) error {
		if doc.ID == "" {
			// look up the id since we need it for lookup
			var err error
			doc.ID, err = doc.DocReader.ExternalID(doc.IndexInternalID)
			if err != nil {
				return err
			}
		}
		doc.Complete(nil)
		return nil
	})

	return err
}

func (hc *SuperCollector) DocumentMatchHandler(d *search.DocumentMatch) error {
	if d == nil {
		return nil
	}

	hc.m.Lock()

	// support search after based pagination,
	// if this hit is <= the search after sort key
	// we should skip it
	if hc.coll.searchAfter != nil {
		// exact sort order matches use hit number to break tie
		// but we want to allow for exact match, so we pretend
		hc.coll.searchAfter.HitNumber = d.HitNumber
		if hc.coll.sort.Compare(hc.coll.cachedScoring, hc.coll.cachedDesc, d, hc.coll.searchAfter) <= 0 {
			hc.m.Unlock()
			return nil
		}
	}

	// optimization, we track lowest sorting hit already removed from heap
	// with this one comparison, we can avoid all heap operations if
	// this hit would have been added and then immediately removed
	if hc.coll.lowestMatchOutsideResults != nil {
		cmp := hc.coll.sort.Compare(hc.coll.cachedScoring, hc.coll.cachedDesc, d,
			hc.coll.lowestMatchOutsideResults)
		if cmp >= 0 {
			// this hit can't possibly be in the result set, so avoid heap ops
			hc.ctx.DocumentMatchPool.Put(d)
			hc.m.Unlock()
			return nil
		}
	}

	removed := hc.coll.store.AddNotExceedingSize(d, hc.coll.size+hc.coll.skip)
	if removed != nil {
		if hc.coll.lowestMatchOutsideResults == nil {
			hc.coll.lowestMatchOutsideResults = removed
		} else {
			cmp := hc.coll.sort.Compare(hc.coll.cachedScoring, hc.coll.cachedDesc,
				removed, hc.coll.lowestMatchOutsideResults)
			if cmp < 0 {
				tmp := hc.coll.lowestMatchOutsideResults
				hc.coll.lowestMatchOutsideResults = removed
				hc.ctx.DocumentMatchPool.Put(tmp)
			}
		}
	}
	hc.m.Unlock()
	return nil
}
