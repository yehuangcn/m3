// Copyright (c) 2018 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.
//

package m3

import (
	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/dbnode/ts"
	"github.com/m3db/m3/src/x/cost"
	xtime "github.com/m3db/m3x/time"
)

type AccountedSeriesIter struct {
	encoding.SeriesIterator

	enforcer    *cost.Enforcer
	enforcerErr error
}

func NewAccountedSeriesIter(wrapped encoding.SeriesIterator, enforcer *cost.Enforcer) *AccountedSeriesIter {
	return &AccountedSeriesIter{
		SeriesIterator: wrapped,
		enforcer:       enforcer,
	}
}

func (as *AccountedSeriesIter) Current() (ts.Datapoint, xtime.Unit, ts.Annotation) {
	dp, u, a := as.SeriesIterator.Current()

	_, err := as.enforcer.Add(cost.CostableFloat64(1.0))

	if err != nil {
		as.enforcerErr = err
	}

	return dp, u, a
}

func (as *AccountedSeriesIter) Err() error {
	if err := as.SeriesIterator.Err(); err != nil {
		return err
	}
	return as.enforcerErr
}

func (as *AccountedSeriesIter) Next() bool {
	return as.enforcerErr == nil && as.SeriesIterator.Next()
}

//
//func (AccountedSeriesIter) Close() {
//	panic("implement me")
//}
//
//func (AccountedSeriesIter) ID() ident.ID {
//	panic("implement me")
//}
//
//func (AccountedSeriesIter) Namespace() ident.ID {
//	panic("implement me")
//}
//
//func (AccountedSeriesIter) Tags() ident.TagIterator {
//	panic("implement me")
//}
//
//func (AccountedSeriesIter) Start() time.Time {
//	panic("implement me")
//}
//
//func (AccountedSeriesIter) End() time.Time {
//	panic("implement me")
//}
//
//func (AccountedSeriesIter) Reset(opts encoding.SeriesIteratorOptions) {
//	panic("implement me")
//}
//
//func (AccountedSeriesIter) SetIterateEqualTimestampStrategy(strategy encoding.IterateEqualTimestampStrategy) {
//	panic("implement me")
//}
//
//func (AccountedSeriesIter) Replicas() []encoding.MultiReaderIterator {
//	panic("implement me")
//}
//
