package rangehandler

import (
	"context"
	"errors"
	"io"
	"net/http"

	"github.com/felixge/httpsnoop"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-unixfsnode"
	"github.com/ipld/go-car/v2"
	"github.com/ipld/go-car/v2/storage"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	trustlessutils "github.com/ipld/go-trustless-utils"
	trustlesshttp "github.com/ipld/go-trustless-utils/http"
	"github.com/ipld/go-trustless-utils/traversal"
)

type rangeHandler struct {
	next http.HandlerFunc
}

func (rh rangeHandler) handler(res http.ResponseWriter, req *http.Request) {
	// check for byte range header
	byteRange, err := trustlesshttp.ParseByteRange(req)
	if err != nil {
		http.Error(res, err.Error(), http.StatusBadRequest)
	}
	if byteRange == nil {
		// if not present, just use default behavior
		rh.next(res, req)
		return
	}

	// parse the request of the request

	// cid+path
	rootCid, path, err := trustlesshttp.ParseUrlPath(req.URL.Path)
	if err != nil {
		if errors.Is(err, trustlesshttp.ErrPathNotFound) {
			http.Error(res, err.Error(), http.StatusNotFound)
		} else if errors.Is(err, trustlesshttp.ErrBadCid) {
			http.Error(res, err.Error(), http.StatusBadRequest)
		} else {
			http.Error(res, err.Error(), http.StatusInternalServerError)
		}
		return
	}

	// accept
	accept, err := trustlesshttp.CheckFormat(req)
	if err != nil {
		http.Error(res, err.Error(), http.StatusBadRequest)
		return
	}

	// scope
	dagScope, err := trustlesshttp.ParseScope(req)
	if err != nil {
		http.Error(res, err.Error(), http.StatusBadRequest)
		return
	}

	// setup a writable CARv1 link system for the traversal, that writes to the underlying
	// http.ResponseWriter
	writable, err := storage.NewWritable(res, []cid.Cid{rootCid}, car.WriteAsCarV1(true), car.AllowDuplicatePuts(accept.Duplicates))
	if err != nil {
		http.Error(res, err.Error(), http.StatusInternalServerError)
		return
	}
	linkSystem := cidlink.DefaultLinkSystem()
	linkSystem.SetWriteStorage(writable)
	linkSystem.TrustedStorage = true
	unixfsnode.AddUnixFSReificationToLinkSystem(&linkSystem)

	r, wr := io.Pipe()

	ctx, cancel := context.WithCancel(req.Context())

	// kick off a request execution w/o range header in a go routine
	// but have it write to the pipe
	go func() {
		wrappedWriter := httpsnoop.Wrap(res, httpsnoop.Hooks{
			Write: func(original httpsnoop.WriteFunc) httpsnoop.WriteFunc {
				return wr.Write
			},
		})
		clonedReq := req.Clone(ctx)
		q := clonedReq.URL.Query()
		q.Del("entity-bytes")
		clonedReq.URL.RawQuery = q.Encode()
		rh.next(wrappedWriter, clonedReq)
	}()

	defer cancel()

	// setup a trustless request
	request := trustlessutils.Request{
		Root:       rootCid,
		Path:       path.String(),
		Scope:      dagScope,
		Bytes:      byteRange,
		Duplicates: accept.Duplicates,
	}

	// run a traversal to extract just the relevant range
	_, _ = traversal.Config{
		Root:                 rootCid,
		Selector:             request.Selector(),
		ExpectDuplicatesIn:   accept.Duplicates,
		WriteDuplicatesOut:   accept.Duplicates,
		UnsafeSkipUnexpected: true,
	}.VerifyCar(req.Context(), r, linkSystem)
}

// HandleRanges handles byte range queries (entity-bytes)
func HandleRanges(next http.HandlerFunc) http.HandlerFunc {
	return rangeHandler{next: next}.handler
}
