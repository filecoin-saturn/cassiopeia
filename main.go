package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/filecoin-project/lassie/pkg/types"
	"github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multicodec"
	"github.com/urfave/cli/v2"
)

var logger = log.Logger("cassiopeia")

func main() {
	// set up a context that is canceled when a command is interrupted
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// set up a signal handler to cancel the context
	go func() {
		interrupt := make(chan os.Signal, 1)
		signal.Notify(interrupt, syscall.SIGTERM, syscall.SIGINT)

		select {
		case <-interrupt:
			fmt.Println()
			logger.Info("received interrupt signal")
			cancel()
		case <-ctx.Done():
		}

		// Allow any further SIGTERM or SIGINT to kill process
		signal.Stop(interrupt)
	}()

	app := &cli.App{
		Name:    "cassiopeia",
		Usage:   "Utility for retrieving content from the Filecoin network",
		Suggest: true,
		Flags:   daemonFlags,
		Action:  serveAction,
	}

	if err := app.RunContext(ctx, os.Args); err != nil {
		logger.Fatal(err)
	}
}

var daemonFlags = []cli.Flag{
	&cli.StringFlag{
		Name:        "address",
		Aliases:     []string{"a"},
		Usage:       "the address the http server listens on",
		Value:       "127.0.0.1",
		DefaultText: "127.0.0.1",
		EnvVars:     []string{"LASSIE_ADDRESS"},
	},
	&cli.UintFlag{
		Name:        "port",
		Aliases:     []string{"p"},
		Usage:       "the port the http server listens on",
		Value:       0,
		DefaultText: "random",
		EnvVars:     []string{"LASSIE_PORT"},
	},
	&cli.Uint64Flag{
		Name:        "maxblocks",
		Aliases:     []string{"mb"},
		Usage:       "maximum number of blocks sent before closing connection",
		Value:       0,
		DefaultText: "no limit",
		EnvVars:     []string{"LASSIE_MAX_BLOCKS_PER_REQUEST"},
	},
	&cli.IntFlag{
		Name:        "libp2p-conns-lowwater",
		Aliases:     []string{"lw"},
		Usage:       "lower limit of libp2p connections",
		Value:       0,
		DefaultText: "libp2p default",
		EnvVars:     []string{"LASSIE_LIBP2P_CONNECTIONS_LOWWATER"},
	},
	&cli.IntFlag{
		Name:        "libp2p-conns-highwater",
		Aliases:     []string{"hw"},
		Usage:       "upper limit of libp2p connections",
		Value:       0,
		DefaultText: "libp2p default",
		EnvVars:     []string{"LASSIE_LIBP2P_CONNECTIONS_HIGHWATER"},
	},
	&cli.UintFlag{
		Name:        "concurrent-sp-retrievals",
		Aliases:     []string{"cr"},
		Usage:       "max number of simultaneous SP retrievals",
		Value:       0,
		DefaultText: "no limit",
		EnvVars:     []string{"LASSIE_CONCURRENT_SP_RETRIEVALS"},
	},
	FlagEventRecorderAuth,
	FlagEventRecorderInstanceId,
	FlagEventRecorderUrl,
	FlagVerbose,
	FlagVeryVerbose,
	FlagProtocols,
	FlagAllowProviders,
	FlagExcludeProviders,
	FlagTempDir,
	FlagBitswapConcurrency,
	FlagGlobalTimeout,
	FlagProviderTimeout,
}

const (
	defaultBitswapConcurrency int           = 6                // 6 concurrent requests
	defaultProviderTimeout    time.Duration = 20 * time.Second // 20 seconds
)

var (
	defaultTempDirectory     string   = os.TempDir() // use the system default temp dir
	verboseLoggingSubsystems []string = []string{    // verbose logging is enabled for these subsystems when using the verbose or very-verbose flags
		"lassie",
		"lassie/retriever",
		"lassie/httpserver",
		"lassie/indexerlookup",
		"lassie/bitswap",
	}
)

// FlagVerbose enables verbose mode, which shows info information about
// operations invoked in the CLI.
var FlagVerbose = &cli.BoolFlag{
	Name:    "verbose",
	Aliases: []string{"v"},
	Usage:   "enable verbose mode for logging",
	Action:  setLogLevel("INFO"),
}

// FlagVeryVerbose enables very verbose mode, which shows debug information about
// operations invoked in the CLI.
var FlagVeryVerbose = &cli.BoolFlag{
	Name:    "very-verbose",
	Aliases: []string{"vv"},
	Usage:   "enable very verbose mode for debugging",
	Action:  setLogLevel("DEBUG"),
}

// setLogLevel returns a CLI Action function that sets the
// logging level for the given subsystems to the given level.
// It is used as an action for the verbose and very-verbose flags.
func setLogLevel(level string) func(*cli.Context, bool) error {
	return func(cctx *cli.Context, _ bool) error {
		// don't override logging if set in the environment.
		if os.Getenv("GOLOG_LOG_LEVEL") != "" {
			return nil
		}
		// set the logging level for the given subsystems
		for _, name := range verboseLoggingSubsystems {
			_ = log.SetLogLevel(name, level)
		}
		return nil
	}
}

// FlagEventRecorderAuth asks for and provides the authorization token for
// sending metrics to an event recorder API via a Basic auth Authorization
// HTTP header. Value will formatted as "Basic <value>" if provided.
var FlagEventRecorderAuth = &cli.StringFlag{
	Name:        "event-recorder-auth",
	Usage:       "the authorization token for an event recorder API",
	DefaultText: "no authorization token will be used",
	EnvVars:     []string{"LASSIE_EVENT_RECORDER_AUTH"},
}

// FlagEventRecorderUrl asks for and provides the URL for an event recorder API
// to send metrics to.
var FlagEventRecorderInstanceId = &cli.StringFlag{
	Name:        "event-recorder-instance-id",
	Usage:       "the instance ID to use for an event recorder API request",
	DefaultText: "a random v4 uuid",
	EnvVars:     []string{"LASSIE_EVENT_RECORDER_INSTANCE_ID"},
}

// FlagEventRecorderUrl asks for and provides the URL for an event recorder API
// to send metrics to.
var FlagEventRecorderUrl = &cli.StringFlag{
	Name:        "event-recorder-url",
	Usage:       "the url of an event recorder API",
	DefaultText: "no event recorder API will be used",
	EnvVars:     []string{"LASSIE_EVENT_RECORDER_URL"},
}

var providerBlockList map[peer.ID]bool
var FlagExcludeProviders = &cli.StringFlag{
	Name:        "exclude-providers",
	DefaultText: "All providers allowed",
	Usage:       "Provider peer IDs, seperated by a comma. Example: 12D3KooWBSTEYMLSu5FnQjshEVah9LFGEZoQt26eacCEVYfedWA4",
	EnvVars:     []string{"LASSIE_EXCLUDE_PROVIDERS"},
	Action: func(cctx *cli.Context, v string) error {
		// Do nothing if given an empty string
		if v == "" {
			return nil
		}

		providerBlockList = make(map[peer.ID]bool)
		vs := strings.Split(v, ",")
		for _, v := range vs {
			peerID, err := peer.Decode(v)
			if err != nil {
				return err
			}
			providerBlockList[peerID] = true
		}
		return nil
	},
}

var fetchProviderAddrInfos []peer.AddrInfo

var FlagAllowProviders = &cli.StringFlag{
	Name:        "providers",
	Aliases:     []string{"provider"},
	DefaultText: "Providers will be discovered automatically",
	Usage:       "Addresses of providers, including peer IDs, to use instead of automatic discovery, seperated by a comma. All protocols will be attempted when connecting to these providers. Example: /ip4/1.2.3.4/tcp/1234/p2p/12D3KooWBSTEYMLSu5FnQjshEVah9LFGEZoQt26eacCEVYfedWA4",
	EnvVars:     []string{"LASSIE_ALLOW_PROVIDERS"},
	Action: func(cctx *cli.Context, v string) error {
		// Do nothing if given an empty string
		if v == "" {
			return nil
		}

		var err error
		fetchProviderAddrInfos, err = types.ParseProviderStrings(v)
		return err
	},
}

var protocols []multicodec.Code
var FlagProtocols = &cli.StringFlag{
	Name:        "protocols",
	DefaultText: "bitswap,graphsync,http",
	Usage:       "List of retrieval protocols to use, seperated by a comma",
	EnvVars:     []string{"LASSIE_SUPPORTED_PROTOCOLS"},
	Action: func(cctx *cli.Context, v string) error {
		// Do nothing if given an empty string
		if v == "" {
			return nil
		}

		var err error
		protocols, err = types.ParseProtocolsString(v)
		return err
	},
}

var FlagTempDir = &cli.StringFlag{
	Name:        "tempdir",
	Aliases:     []string{"td"},
	Usage:       "directory to store temporary files while downloading",
	Value:       defaultTempDirectory,
	DefaultText: "os temp directory",
	EnvVars:     []string{"LASSIE_TEMP_DIRECTORY"},
}

var FlagBitswapConcurrency = &cli.IntFlag{
	Name:    "bitswap-concurrency",
	Usage:   "maximum number of concurrent bitswap requests per retrieval",
	Value:   defaultBitswapConcurrency,
	EnvVars: []string{"LASSIE_BITSWAP_CONCURRENCY"},
}

var FlagGlobalTimeout = &cli.DurationFlag{
	Name:    "global-timeout",
	Aliases: []string{"gt"},
	Usage:   "consider it an error after not completing a retrieval after this amount of time",
	EnvVars: []string{"LASSIE_GLOBAL_TIMEOUT"},
}

var FlagProviderTimeout = &cli.DurationFlag{
	Name:    "provider-timeout",
	Aliases: []string{"pt"},
	Usage:   "consider it an error after not receiving a response from a storage provider after this amount of time",
	Value:   defaultProviderTimeout,
	EnvVars: []string{"LASSIE_PROVIDER_TIMEOUT"},
}

var FlagIPNIEndpoint = &cli.StringFlag{
	Name:        "ipni-endpoint",
	Aliases:     []string{"ipni"},
	DefaultText: "Defaults to https://cid.contact",
	Usage:       "HTTP endpoint of the IPNI instance used to discover providers.",
}
