package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"net/http"
	"os"
	"os/signal"
	"runtime"
	"sync"
	"syscall"
	"time"

	"github.com/bugsnag/bugsnag-go"

	"github.com/rudderlabs/rudder-server/admin"
	"github.com/rudderlabs/rudder-server/app"
	"github.com/rudderlabs/rudder-server/config"
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/processor"
	"github.com/rudderlabs/rudder-server/router"
	"github.com/rudderlabs/rudder-server/router/batchrouter"
	"github.com/rudderlabs/rudder-server/rruntime"
	"github.com/rudderlabs/rudder-server/services/apptype"
	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/utils"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/utils/types"
	"github.com/rudderlabs/rudder-server/warehouse"

	// This is necessary for compatibility with enterprise features
	_ "github.com/rudderlabs/rudder-server/imports"
)

var (
	application                      app.Interface
	warehouseMode                    string
	maxProcess                       int
	gwDBRetention, routerDBRetention time.Duration
	enableProcessor, enableRouter    bool
	enabledDestinations              []backendconfig.DestinationT
	configSubscriberLock             sync.RWMutex
	objectStorageDestinations        []string
	warehouseDestinations            []string
	moduleLoadLock                   sync.Mutex
	routerLoaded                     bool
	processorLoaded                  bool
	enableSuppressUserFeature        bool
	appType                          AppTypeHandler
)

var version = "Not an official release. Get the latest release from the github repo."
var major, minor, commit, buildDate, builtBy, gitURL, patch string

const (
	gatewayAppType   = "GATEWAY"
	processorAppType = "PROCESSOR"
	monolithAppType  = "MONOLITH"
)

//AppTypeHandler to be implemented by different app type objects.
type AppTypeHandler interface {
	GetAppType() string
	HandleRecovery(*app.Options)
	StartRudderCore(*app.Options)
}

func getAppType(appType string) AppTypeHandler {
	var handler AppTypeHandler
	switch appType {
	case gatewayAppType:
		handler = &apptype.GatewayAppType{}
	case processorAppType:
		handler = &apptype.ProcessorAppType{}
	case monolithAppType:
		handler = &apptype.MonolithAppType{}
	default:
		panic(errors.New("invalid app type"))
	}

	return handler
}

func loadConfig() {
	maxProcess = config.GetInt("maxProcess", 12)
	gwDBRetention = config.GetDuration("gwDBRetentionInHr", 0) * time.Hour
	routerDBRetention = config.GetDuration("routerDBRetention", 0)
	enableProcessor = config.GetBool("enableProcessor", true)
	enableRouter = config.GetBool("enableRouter", true)
	objectStorageDestinations = []string{"S3", "GCS", "AZURE_BLOB", "MINIO", "DIGITAL_OCEAN_SPACES"}
	warehouseDestinations = []string{"RS", "BQ", "SNOWFLAKE", "POSTGRES", "CLICKHOUSE"}
	warehouseMode = config.GetString("Warehouse.mode", "embedded")
	// Enable suppress user feature. false by default
	enableSuppressUserFeature = config.GetBool("Gateway.enableSuppressUserFeature", false)
}

// Test Function
func readIOforResume(router router.HandleT) {
	for {
		var u string
		_, err := fmt.Scanf("%v", &u)
		fmt.Println("from stdin ", u)
		if err != nil {
			panic(err)
		}
		router.ResetSleep()
	}
}

// Gets the config from config backend and extracts enabled writekeys
func monitorDestRouters(routerDB, batchRouterDB *jobsdb.HandleT) {
	ch := make(chan utils.DataEvent)
	backendconfig.Subscribe(ch, backendconfig.TopicBackendConfig)
	dstToRouter := make(map[string]*router.HandleT)
	dstToBatchRouter := make(map[string]*batchrouter.HandleT)
	// dstToWhRouter := make(map[string]*warehouse.HandleT)

	for {
		config := <-ch
		sources := config.Data.(backendconfig.SourcesT)
		enabledDestinations := make(map[string]bool)
		for _, source := range sources.Sources {
			for _, destination := range source.Destinations {
				enabledDestinations[destination.DestinationDefinition.Name] = true
				//For batch router destinations
				if misc.Contains(objectStorageDestinations, destination.DestinationDefinition.Name) || misc.Contains(warehouseDestinations, destination.DestinationDefinition.Name) {
					_, ok := dstToBatchRouter[destination.DestinationDefinition.Name]
					if !ok {
						logger.Info("Starting a new Batch Destination Router", destination.DestinationDefinition.Name)
						var brt batchrouter.HandleT
						brt.Setup(batchRouterDB, destination.DestinationDefinition.Name)
						dstToBatchRouter[destination.DestinationDefinition.Name] = &brt
					}
				} else {
					_, ok := dstToRouter[destination.DestinationDefinition.Name]
					if !ok {
						logger.Info("Starting a new Destination", destination.DestinationDefinition.Name)
						var router router.HandleT
						router.Setup(routerDB, destination.DestinationDefinition.Name)
						dstToRouter[destination.DestinationDefinition.Name] = &router
					}
				}
			}
		}
	}
}

func init() {
	loadConfig()
}

func versionInfo() map[string]interface{} {
	return map[string]interface{}{"Version": version, "Major": major, "Minor": minor, "Patch": patch, "Commit": commit, "BuildDate": buildDate, "BuiltBy": builtBy, "GitUrl": gitURL}
}

func versionHandler(w http.ResponseWriter, r *http.Request) {
	var version = versionInfo()
	versionFormatted, _ := json.Marshal(&version)
	w.Write(versionFormatted)
}

func printVersion() {
	version := versionInfo()
	versionFormatted, _ := json.MarshalIndent(&version, "", " ")
	fmt.Printf("Version Info %s\n", versionFormatted)
}

func startWarehouseService() {
	warehouse.Start()
}

//StartRouter atomically starts router process if not already started
func StartRouter(enableRouter bool, routerDB, batchRouterDB *jobsdb.HandleT) {
	moduleLoadLock.Lock()
	defer moduleLoadLock.Unlock()

	if routerLoaded {
		return
	}

	if enableRouter {
		go monitorDestRouters(routerDB, batchRouterDB)
		routerLoaded = true
	}
}

//StartProcessor atomically starts processor process if not already started
func StartProcessor(enableProcessor bool, gatewayDB, routerDB, batchRouterDB *jobsdb.HandleT, procErrorDB *jobsdb.HandleT) {
	moduleLoadLock.Lock()
	defer moduleLoadLock.Unlock()

	if processorLoaded {
		return
	}

	if enableProcessor {
		var processor = processor.NewProcessor()
		processor.Setup(backendconfig.DefaultBackendConfig, gatewayDB, routerDB, batchRouterDB, procErrorDB, stats.DefaultStats)
		processor.Start()

		processorLoaded = true
	}
}

func canStartServer() bool {
	return warehouseMode == config.EmbeddedMode || warehouseMode == config.OffMode
}

func canStartWarehouse() bool {
	return warehouseMode != config.OffMode
}

func main() {
	appType = getAppType(config.GetEnv("APP_TYPE", ""))

	version := versionInfo()

	bugsnag.Configure(bugsnag.Configuration{
		APIKey:       config.GetEnv("BUGSNAG_KEY", ""),
		ReleaseStage: config.GetEnv("GO_ENV", "development"),
		// The import paths for the Go packages containing your source files
		ProjectPackages: []string{"main", "github.com/rudderlabs/rudder-server"},
		// more configuration options
		AppType:      appType.GetAppType(),
		AppVersion:   version["Version"].(string),
		PanicHandler: func() {},
	})
	ctx := bugsnag.StartSession(context.Background())
	defer func() {
		if r := recover(); r != nil {
			defer bugsnag.AutoNotify(ctx, bugsnag.SeverityError, bugsnag.MetaData{
				"GoRoutines": {
					"Number": runtime.NumGoroutine(),
				}})

			misc.RecordAppError(fmt.Errorf("%v", r))
			logger.Fatal(r)
			panic(r)
		}
	}()

	logger.Setup()

	//Creating Stats Client should be done right after setting up logger and before setting up other modules.
	stats.Setup()

	options := app.LoadOptions()
	if options.VersionFlag {
		printVersion()
		return
	}
	application = app.New(options)

	http.HandleFunc("/version", versionHandler)

	//application & backend setup should be done before starting any new goroutines.
	application.Setup()

	var pollRegulations bool
	if enableSuppressUserFeature {
		if application.Features().SuppressUser != nil {
			pollRegulations = true
		} else {
			logger.Info("Suppress User feature is enterprise only. Unable to poll regulations.")
		}
	}

	var configEnvHandler types.ConfigEnvI
	if application.Features().ConfigEnv != nil {
		configEnvHandler = application.Features().ConfigEnv.Setup()
	}

	backendconfig.Setup(pollRegulations, configEnvHandler)

	c := make(chan os.Signal)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		application.Stop()
		// clearing zap Log buffer to std output
		if logger.Log != nil {
			logger.Log.Sync()
		}
		stats.StopRuntimeStats()
		os.Exit(1)
	}()

	misc.AppStartTime = time.Now().Unix()
	if canStartServer() {
		appType.HandleRecovery(options)
		rruntime.Go(func() {
			appType.StartRudderCore(options)
		})
	}

	//TODO: Handle warehouse later
	/*// initialize warehouse service after core to handle non-normal recovery modes
	if canStartWarehouse() {
		rruntime.Go(func() {
			startWarehouseService()
		})
	}
	*/

	rruntime.Go(admin.StartServer)

	misc.KeepProcessAlive()
}
