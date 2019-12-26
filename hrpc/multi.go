// Copyright (C) 2017  The GoHBase Authors.  All rights reserved.
// This file is part of GoHBase.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package hrpc

import (
	"context"
	"errors"
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/zhuyaoyhj/gohbase/pb"
	"sync"
)

var multiPool = sync.Pool{
	New: func() interface{} {
		return &Multi{
			resultch: make(chan RPCResult, 1),
		}
	},
}

func freeMulti(m *Multi) {
	m.calls = m.calls[:0]
	m.regions = m.regions[:0]
	m.size = 0
	multiPool.Put(m)
}

type Multi struct {
	size  int
	calls []Call
	// regions preserves the order of regions to match against RegionActionResults
	regions  []RegionInfo
	resultch chan RPCResult
}

func NewMultiStr() *Multi {
	return &Multi{}
}

func NewMulti(queueSize int) *Multi {
	m := multiPool.Get().(*Multi)
	m.size = queueSize
	return m
}

// Name returns the name of this RPC call.
func (m *Multi) Name() string {
	return "Multi"
}

func (m *Multi) GetCalls() []Call {
	return m.calls
}

func (m *Multi) SetRegions(region []RegionInfo) {
	m.regions = append(m.regions, region...)
	for k, v := range region {
		m.calls[k].SetRegion(v)
	}
}

// ToProto converts all request in multi batch to a protobuf message.
func (m *Multi) ToProto() proto.Message {
	// aggregate calls per region
	actionsPerReg := map[RegionInfo][]*pb.Action{}

	for i, c := range m.calls {
		select {
		case <-c.Context().Done():
			// context has expired, don't bother sending it
			m.calls[i] = nil
			continue
		default:
		}

		msg := c.ToProto()

		a := &pb.Action{
			Index: proto.Uint32(uint32(i) + 1), // +1 because 0 index means there's no index
		}

		switch r := msg.(type) {
		case *pb.GetRequest:
			a.Get = r.Get
		case *pb.MutateRequest:
			a.Mutation = r.Mutation
		default:
			panic(fmt.Sprintf("unsupported call type for Multi: %T", c))
		}

		actionsPerReg[c.Region()] = append(actionsPerReg[c.Region()], a)
	}

	// construct the multi proto
	ra := make([]*pb.RegionAction, len(actionsPerReg))
	m.regions = make([]RegionInfo, len(actionsPerReg))

	i := 0
	for r, as := range actionsPerReg {
		ra[i] = &pb.RegionAction{
			Region: &pb.RegionSpecifier{
				Type:  pb.RegionSpecifier_REGION_NAME.Enum(),
				Value: r.Name(),
			},
			Action: as,
		}
		// Track the order of RegionActions,
		// so that we can handle whole region exceptions.
		m.regions[i] = r
		i++
	}
	return &pb.MultiRequest{RegionAction: ra}
}

// NewResponse creates an empty protobuf message to read the response of this RPC.
func (m *Multi) NewResponse() proto.Message {
	return &pb.MultiResponse{}
}

// DeserializeCellBlocks deserializes action results from cell blocks.
func (m *Multi) DeserializeCellBlocks(msg proto.Message, b []byte) (uint32, error) {
	mr := msg.(*pb.MultiResponse)

	var nread uint32
	for _, rar := range mr.GetRegionActionResult() {
		if e := rar.GetException(); e != nil {
			if l := len(rar.GetResultOrException()); l != 0 {
				return 0, fmt.Errorf(
					"got exception for region, but still have %d result(s) returned from it", l)
			}
			continue
		}

		for _, roe := range rar.GetResultOrException() {
			e := roe.GetException()
			r := roe.GetResult()
			i := roe.GetIndex()

			if i == 0 {
				return 0, errors.New("no index for result in multi response")
			} else if r == nil && e == nil {
				return 0, errors.New("no result or exception for action in multi response")
			} else if r != nil && e != nil {
				return 0, errors.New("got result and exception for action in multi response")
			} else if e != nil {
				continue
			}

			c := m.get(i)                     // TODO: maybe return error if it's out-of-bounds
			d := c.(canDeserializeCellBlocks) // let it panic, because then it's our bug

			response := c.NewResponse()
			switch rsp := response.(type) {
			case *pb.GetResponse:
				rsp.Result = r
			case *pb.MutateResponse:
				rsp.Result = r
			default:
				panic(fmt.Sprintf("unsupported response type for Multi: %T", response))
			}

			// TODO: don't bother deserializing if the call's context has already expired
			n, err := d.DeserializeCellBlocks(response, b[nread:])
			if err != nil {
				return 0, fmt.Errorf(
					"error deserializing cellblocks for %q call as part of MultiResponse: %v",
					c.Name(), err)
			}
			nread += n
		}
	}
	return nread, nil
}

func (m *Multi) returnResults(msg proto.Message, err error) {
	defer freeMulti(m)

	if err != nil {
		for _, c := range m.calls {
			if c == nil {
				continue
			}
			c.ResultChan() <- RPCResult{Error: err}
		}
		return
	}

	mr := msg.(*pb.MultiResponse)

	// Here we can assume that everything has been deserialized correctly.
	// Dispatch results to appropriate calls.
	for i, rar := range mr.GetRegionActionResult() {
		if e := rar.GetException(); e != nil {
			// Got an exception for the whole region,
			// fail all the calls for that region.
			reg := m.regions[i]

			err := exceptionToError(*e.Name, string(e.Value))
			for _, c := range m.calls {
				if c == nil {
					continue
				}
				if c.Region() == reg {
					c.ResultChan() <- RPCResult{Error: err}
				}
			}
			continue
		}

		for _, roe := range rar.GetResultOrException() {
			i := roe.GetIndex()
			e := roe.GetException()
			r := roe.GetResult()

			c := m.get(i)

			// TODO: don't bother if the call's context has already expired

			if e != nil {
				c.ResultChan() <- RPCResult{
					Error: exceptionToError(*e.Name, string(e.Value)),
				}
				continue
			}

			response := c.NewResponse()
			switch rsp := response.(type) {
			case *pb.GetResponse:
				rsp.Result = r
			case *pb.MutateResponse:
				rsp.Result = r
			default:
				panic(fmt.Sprintf("unsupported response type for Multi: %T", response))
			}

			c.ResultChan() <- RPCResult{Msg: response}
		}
	}
}

// add adds the call and returns wether the batch is full.
func (m *Multi) add(call Call) bool {
	m.calls = append(m.calls, call)
	return len(m.calls) == m.size
}

var (
	getAction   = "get"
	putAction   = "put"
	delAction   = "del"
	actionError = errors.New("invalid action")
)

func (m *Multi) Add(ctx context.Context, table, key string,
	values map[string]map[string][]byte, action string, options ...func(Call) error) error {
	switch action {
	case getAction:
		{
			res, err := NewPutStr(ctx, table, key, values, options...)
			if err != nil {
				return err
			}
			m.add(res)
		}
	case putAction:
		{
			res, err := NewDelStr(ctx, table, key, values, options...)
			if err != nil {
				return err
			}
			m.add(res)
		}
	case delAction:
		{
			res, err := NewGetStr(ctx, table, key, options...)
			if err != nil {
				return err
			}
			m.add(res)
		}
	default:
		return actionError
	}
	//if action == putAction {
	//	res, err := NewPutStr(ctx, table, key, values, options...)
	//	if err != nil {
	//		return err
	//	}
	//	m.add(res)
	//} else if action == delAction {
	//	res, err := NewDelStr(ctx, table, key, values, options...)
	//	if err != nil {
	//		return err
	//	}
	//	m.add(res)
	//} else if action == getAction {
	//	res, err := NewGetStr(ctx, table, key, options...)
	//	if err != nil {
	//		return err
	//	}
	//	m.add(res)
	//} else  {
	//	return actionError
	//}
	return nil
}

// len returns number of batched calls.
func (m *Multi) len() int {
	return len(m.calls)
}

// get retruns an rpc at index. Indicies start from 1 since 0 means that
// region server didn't set an index for the action result.
func (m *Multi) get(i uint32) Call {
	if i == 0 {
		panic("index cannot be 0")
	}
	return m.calls[i-1]
}

// Table is not supported for Multi.
func (m *Multi) Table() []byte {
	panic("'Table' is not supported for 'Multi'")
}

// Reqion is not supported for Multi.
func (m *Multi) Region() RegionInfo {
	panic("'Region' is not supported for 'Multi'")
}

// SetRegion is not supported for Multi.
func (m *Multi) SetRegion(r RegionInfo) {
	panic("'SetRegion' is not supported for 'Multi'")
}

// ResultChan is not supported for Multi.
func (m *Multi) ResultChan() chan RPCResult {
	return m.resultch
}

// Context is not supported for Multi.
func (m *Multi) Context() context.Context {
	// TODO: maybe pick the one with the longest deadline and use a context that has that deadline?
	return context.Background()
}

// Key is not supported for Multi RPC.
func (m *Multi) Key() []byte {
	panic("'Key' is not supported for 'Multi'")
}

// RetryableError is an error that indicates the RPC should be retried after backoff
// because the error is transient (e.g. a region being momentarily unavailable).
type RetryableError struct {
	error
}

func (e RetryableError) Error() string {
	return formatErr(e, e.error)
}

// NotServingRegionError is an error that indicates the client should
// reestablish the region and retry the RPC potentially via a different client
type NotServingRegionError struct {
	error
}

func (e NotServingRegionError) Error() string {
	return formatErr(e, e.error)
}

type ServerError struct {
	error
}

func formatErr(e interface{}, err error) string {
	if err == nil {
		return fmt.Sprintf("%T", e)
	}
	return fmt.Sprintf("%T: %s", e, err.Error())
}

func (e ServerError) Error() string {
	return formatErr(e, e.error)
}

var (
	// ErrMissingCallID is used when HBase sends us a response message for a
	// request that we didn't send
	ErrMissingCallID = ServerError{errors.New("got a response with a nonsensical call ID")}

	// ErrClientClosed is returned to rpcs when Close() is called or when client
	// died because of failed send or receive
	ErrClientClosed = ServerError{errors.New("client is closed")}

	// If a Java exception listed here is returned by HBase, the client should
	// reestablish region and attempt to resend the RPC message, potentially via
	// a different region client.
	javaRegionExceptions = map[string]struct{}{
		"org.apache.hadoop.hbase.NotServingRegionException":       struct{}{},
		"org.apache.hadoop.hbase.exceptions.RegionMovedException": struct{}{},
	}

	// If a Java exception listed here is returned by HBase, the client should
	// backoff and resend the RPC message to the same region and region server
	javaRetryableExceptions = map[string]struct{}{
		"org.apache.hadoop.hbase.CallQueueTooBigException":          struct{}{},
		"org.apache.hadoop.hbase.exceptions.RegionOpeningException": struct{}{},
		"org.apache.hadoop.hbase.ipc.ServerNotRunningYetException":  struct{}{},
		"org.apache.hadoop.hbase.quotas.RpcThrottlingException":     struct{}{},
		"org.apache.hadoop.hbase.RetryImmediatelyException":         struct{}{},
		"org.apache.hadoop.hbase.RegionTooBusyException":            struct{}{},
	}

	// javaServerExceptions is a map where all Java exceptions that signify
	// the RPC should be sent again are listed (as keys). If a Java exception
	// listed here is returned by HBase, the RegionClient will be closed and a new
	// one should be established.
	javaServerExceptions = map[string]struct{}{
		"org.apache.hadoop.hbase.regionserver.RegionServerAbortedException": struct{}{},
		"org.apache.hadoop.hbase.regionserver.RegionServerStoppedException": struct{}{},
	}
)

type canDeserializeCellBlocks interface {
	// DeserializeCellBlocks populates passed protobuf message with results
	// deserialized from the reader and returns number of bytes read or error.
	DeserializeCellBlocks(proto.Message, []byte) (uint32, error)
}

func exceptionToError(class, stack string) error {
	err := fmt.Errorf("HBase Java exception %s:\n%s", class, stack)
	if _, ok := javaRetryableExceptions[class]; ok {
		return RetryableError{err}
	} else if _, ok := javaRegionExceptions[class]; ok {
		return NotServingRegionError{err}
	} else if _, ok := javaServerExceptions[class]; ok {
		return ServerError{err}
	}
	return err
}
