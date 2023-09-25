package httpserver

import (
	"context"
	"fmt"
	"net"
	"net/http"

	"github.com/darkweak/souin/configurationtypes"
	"github.com/darkweak/souin/pkg/middleware"
	"github.com/dgraph-io/badger"
	"github.com/filecoin-project/lassie/pkg/lassie"
	lassiehttpserver "github.com/filecoin-project/lassie/pkg/server/http"
	"github.com/filecoin-saturn/cassiopeia/httpserver/rangehandler"
	"github.com/ipfs/go-log/v2"
	servertiming "github.com/mitchellh/go-server-timing"
)

var logger = log.Logger("cassiopeia/httpserver")

// HttpServer is a Lassie server for fetching data from the network via HTTP
type HttpServer struct {
	cancel   context.CancelFunc
	ctx      context.Context
	listener net.Listener
	server   *http.Server
}

type HttpServerConfig struct {
	Address             string
	Port                uint
	TempDir             string
	MaxBlocksPerRequest uint64
	AccessToken         string
}

type contextKey struct {
	key string
}

var connContextKey = &contextKey{"http-conn"}

func saveConnInCTX(ctx context.Context, c net.Conn) context.Context {
	return context.WithValue(ctx, connContextKey, c)
}

// NewHttpServer creates a new HttpServer
func NewHttpServer(ctx context.Context, lassie *lassie.Lassie, cfg HttpServerConfig) (*HttpServer, error) {
	addr := fmt.Sprintf("%s:%d", cfg.Address, cfg.Port)
	listener, err := net.Listen("tcp", addr) // assigns a port if port is 0
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(ctx)

	// create server
	mux := http.NewServeMux()

	badgerConf := badger.DefaultOptions(cfg.TempDir)

	cacheConf := middleware.BaseConfiguration{
		DefaultCache: &configurationtypes.DefaultCache{
			AllowedHTTPVerbs: []string{"GET", "POST", "HEAD"},
			Badger: configurationtypes.CacheProvider{
				Configuration: badgerConf,
			},
			CacheName:   "Saturn",
			Distributed: false,
			Key: configurationtypes.Key{
				DisableBody:   true,
				DisableHost:   true,
				DisableMethod: true,
				DisableQuery:  false,
				Headers:       []string{"Accept"},
				Hide:          true,
			},
			DefaultCacheControl: "public, max-age=31536000, immutable",
		},
	}
	cacher := middleware.NewHTTPCacheHandler(&cacheConf)

	handler := servertiming.Middleware(
		rangehandler.HandleRanges(
			http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				cacher.ServeHTTP(w, r, func(w http.ResponseWriter, r *http.Request) error {
					mux.ServeHTTP(w, r)
					return nil
				})
			}),
		),
		nil,
	)

	server := &http.Server{
		Addr:        fmt.Sprintf(":%d", cfg.Port),
		BaseContext: func(listener net.Listener) context.Context { return ctx },
		Handler:     handler,
		ConnContext: saveConnInCTX,
	}

	httpServer := &HttpServer{
		cancel:   cancel,
		ctx:      ctx,
		listener: listener,
		server:   server,
	}

	// Routes
	lassieCfg := lassiehttpserver.HttpServerConfig{
		Address:             cfg.Address,
		Port:                cfg.Port,
		TempDir:             cfg.TempDir,
		MaxBlocksPerRequest: cfg.MaxBlocksPerRequest,
		AccessToken:         cfg.AccessToken,
	}
	mux.HandleFunc("/ipfs/", lassiehttpserver.IpfsHandler(lassie, lassieCfg))

	return httpServer, nil
}

// Addr returns the listening address of the server
func (s HttpServer) Addr() string {
	return s.listener.Addr().String()
}

// Start starts the http server, returning an error if the server failed to start
func (s *HttpServer) Start() error {
	logger.Infow("starting http server", "listen_addr", s.listener.Addr())
	err := s.server.Serve(s.listener)
	if err != http.ErrServerClosed {
		logger.Errorw("failed to start http server", "err", err)
		return err
	}

	return nil
}

// Close shutsdown the server and cancels the server context
func (s *HttpServer) Close() error {
	logger.Info("closing http server")
	s.cancel()
	return s.server.Shutdown(context.Background())
}
