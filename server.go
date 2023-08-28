package main

import (
	"fmt"
	"net/url"

	"github.com/filecoin-saturn/cassiopeia/httpserver"

	"github.com/filecoin-project/lassie/pkg/aggregateeventrecorder"
	"github.com/filecoin-project/lassie/pkg/indexerlookup"
	"github.com/filecoin-project/lassie/pkg/lassie"
	"github.com/filecoin-project/lassie/pkg/net/host"
	"github.com/filecoin-project/lassie/pkg/retriever"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/config"
	"github.com/libp2p/go-libp2p/p2p/net/connmgr"
	"github.com/urfave/cli/v2"
)

func serveAction(cctx *cli.Context) error {
	// lassie config
	libp2pLowWater := cctx.Int("libp2p-conns-lowwater")
	libp2pHighWater := cctx.Int("libp2p-conns-highwater")
	concurrentSPRetrievals := cctx.Uint("concurrent-sp-retrievals")
	lassieOpts := []lassie.LassieOption{}

	if concurrentSPRetrievals > 0 {
		lassieOpts = append(lassieOpts, lassie.WithConcurrentSPRetrievals(concurrentSPRetrievals))
	}

	libp2pOpts := []config.Option{}
	if libp2pHighWater != 0 || libp2pLowWater != 0 {
		connManager, err := connmgr.NewConnManager(libp2pLowWater, libp2pHighWater)
		if err != nil {
			return cli.Exit(err, 1)
		}
		libp2pOpts = append(libp2pOpts, libp2p.ConnectionManager(connManager))
	}

	lassieCfg, err := buildLassieConfigFromCLIContext(cctx, lassieOpts, libp2pOpts)
	if err != nil {
		return cli.Exit(err, 1)
	}

	// http server config
	address := cctx.String("address")
	port := cctx.Uint("port")
	tempDir := cctx.String("tempdir")
	maxBlocks := cctx.Uint64("maxblocks")
	accessToken := cctx.String("access-token")
	httpServerCfg := httpserver.HttpServerConfig{
		Address:             address,
		Port:                port,
		TempDir:             tempDir,
		MaxBlocksPerRequest: maxBlocks,
		AccessToken:         accessToken,
	}

	// event recorder config
	eventRecorderURL := cctx.String("event-recorder-url")
	authToken := cctx.String("event-recorder-auth")
	instanceID := cctx.String("event-recorder-instance-id")
	eventRecorderCfg := &aggregateeventrecorder.EventRecorderConfig{
		InstanceID:            instanceID,
		EndpointURL:           eventRecorderURL,
		EndpointAuthorization: authToken,
	}

	lassie, err := lassie.NewLassieWithConfig(cctx.Context, lassieCfg)
	if err != nil {
		return cli.Exit(err, 1)
	}

	// create and subscribe an event recorder API if an endpoint URL is set
	if eventRecorderCfg.EndpointURL != "" {
		eventRecorder := aggregateeventrecorder.NewAggregateEventRecorder(cctx.Context, *eventRecorderCfg)
		lassie.RegisterSubscriber(eventRecorder.RetrievalEventSubscriber())
	}

	httpServer, err := httpserver.NewHttpServer(cctx.Context, lassie, httpServerCfg)
	if err != nil {
		logger.Errorw("failed to create http server", "err", err)
		return cli.Exit(err, 1)
	}

	serverErrChan := make(chan error, 1)
	go func() {
		fmt.Printf("Lassie daemon listening on address %s\n", httpServer.Addr())
		fmt.Println("Hit CTRL-C to stop the daemon")
		serverErrChan <- httpServer.Start()
	}()

	select {
	case <-cctx.Context.Done(): // command was cancelled
	case err = <-serverErrChan: // error from server
		logger.Errorw("failed to start http server", "err", err)
	}

	fmt.Println("Shutting down Lassie daemon")
	if err = httpServer.Close(); err != nil {
		logger.Errorw("failed to close http server", "err", err)
		return cli.Exit(err, 1)
	}

	fmt.Println("Lassie daemon stopped")

	return nil
}

func buildLassieConfigFromCLIContext(cctx *cli.Context, lassieOpts []lassie.LassieOption, libp2pOpts []config.Option) (*lassie.LassieConfig, error) {
	providerTimeout := cctx.Duration("provider-timeout")
	globalTimeout := cctx.Duration("global-timeout")
	bitswapConcurrency := cctx.Int("bitswap-concurrency")

	lassieOpts = append(lassieOpts, lassie.WithProviderTimeout(providerTimeout))

	if globalTimeout > 0 {
		lassieOpts = append(lassieOpts, lassie.WithGlobalTimeout(globalTimeout))
	}

	if len(protocols) > 0 {
		lassieOpts = append(lassieOpts, lassie.WithProtocols(protocols))
	}

	host, err := host.InitHost(cctx.Context, libp2pOpts)
	if err != nil {
		return nil, err
	}
	lassieOpts = append(lassieOpts, lassie.WithHost(host))

	if len(fetchProviderAddrInfos) > 0 {
		finderOpt := lassie.WithFinder(retriever.NewDirectCandidateFinder(host, fetchProviderAddrInfos))
		if cctx.IsSet("ipni-endpoint") {
			logger.Warn("Ignoring ipni-endpoint flag since direct provider is specified")
		}
		lassieOpts = append(lassieOpts, finderOpt)
	} else if cctx.IsSet("ipni-endpoint") {
		endpoint := cctx.String("ipni-endpoint")
		endpointUrl, err := url.ParseRequestURI(endpoint)
		if err != nil {
			logger.Errorw("Failed to parse IPNI endpoint as URL", "err", err)
			return nil, fmt.Errorf("cannot parse given IPNI endpoint %s as valid URL: %w", endpoint, err)
		}
		finder, err := indexerlookup.NewCandidateFinder(indexerlookup.WithHttpEndpoint(endpointUrl))
		if err != nil {
			logger.Errorw("Failed to instantiate IPNI candidate finder", "err", err)
			return nil, err
		}
		lassieOpts = append(lassieOpts, lassie.WithFinder(finder))
		logger.Debug("Using explicit IPNI endpoint to find candidates", "endpoint", endpoint)
	}

	if len(providerBlockList) > 0 {
		lassieOpts = append(lassieOpts, lassie.WithProviderBlockList(providerBlockList))
	}

	if bitswapConcurrency > 0 {
		lassieOpts = append(lassieOpts, lassie.WithBitswapConcurrency(bitswapConcurrency))
	}

	return lassie.NewLassieConfig(lassieOpts...), nil
}
