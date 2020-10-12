package main

import (
	"errors"
	"fmt"

	"runtime"
	"time"

	"github.com/rudderlabs/rudder-server/app"
	"github.com/rudderlabs/rudder-server/config"
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/gateway"
	"github.com/rudderlabs/rudder-server/jobsdb"
	ratelimiter "github.com/rudderlabs/rudder-server/rate-limiter"
	"github.com/rudderlabs/rudder-server/rruntime"
	"github.com/rudderlabs/rudder-server/services/db"
	destinationdebugger "github.com/rudderlabs/rudder-server/services/destination-debugger"
	"github.com/rudderlabs/rudder-server/services/diagnostics"
	sourcedebugger "github.com/rudderlabs/rudder-server/services/source-debugger"
	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/services/validators"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"

	// This is necessary for compatibility with enterprise features
	_ "github.com/rudderlabs/rudder-server/imports"
)

//MonolithAppType is the type for monolith type implemention
type MonolithAppType struct {
}

func (monolith *MonolithAppType) GetAppType() string {
	return "rudder-server"
}

func (monolith *MonolithAppType) StartRudderCore(options *app.Options) {
	logger.Info("Main starting")

	if !validators.ValidateEnv() {
		panic(errors.New("Failed to start rudder-server"))
	}
	validators.InitializeEnv()

	// Check if there is a probable inconsistent state of Data
	if diagnostics.EnableServerStartMetric {
		diagnostics.Track(diagnostics.ServerStart, map[string]interface{}{
			diagnostics.ServerStart: fmt.Sprint(time.Unix(misc.AppStartTime, 0)),
		})
	}

	//Reload Config
	loadConfig()

	var gatewayDB jobsdb.HandleT
	var routerDB jobsdb.HandleT
	var batchRouterDB jobsdb.HandleT
	var procErrorDB jobsdb.HandleT

	runtime.GOMAXPROCS(maxProcess)
	logger.Info("Clearing DB ", options.ClearDB)

	destinationdebugger.Setup()
	sourcedebugger.Setup()

	migrationMode := application.Options().MigrationMode
	gatewayDB.Setup(options.ClearDB, "gw", gwDBRetention, migrationMode, false)
	routerDB.Setup(options.ClearDB, "rt", routerDBRetention, migrationMode, true)
	batchRouterDB.Setup(options.ClearDB, "batch_rt", routerDBRetention, migrationMode, true)
	procErrorDB.Setup(options.ClearDB, "proc_error", routerDBRetention, migrationMode, false)

	enableGateway := true

	if application.Features().Migrator != nil {
		if migrationMode == db.IMPORT || migrationMode == db.EXPORT || migrationMode == db.IMPORT_EXPORT {
			startRouterFunc := func() {
				StartRouter(enableRouter, &routerDB, &batchRouterDB)
			}
			startProcessorFunc := func() {
				StartProcessor(enableProcessor, &gatewayDB, &routerDB, &batchRouterDB, &procErrorDB)
			}
			enableRouter = false
			enableProcessor = false
			enableGateway = (migrationMode != db.EXPORT)
			application.Features().Migrator.Setup(&gatewayDB, &routerDB, &batchRouterDB, startProcessorFunc, startRouterFunc)
		}
	}

	StartRouter(enableRouter, &routerDB, &batchRouterDB)
	StartProcessor(enableProcessor, &gatewayDB, &routerDB, &batchRouterDB, &procErrorDB)

	if enableGateway {
		var gateway gateway.HandleT
		var rateLimiter ratelimiter.HandleT

		rateLimiter.SetUp()
		gateway.Setup(application, backendconfig.DefaultBackendConfig, &gatewayDB, &rateLimiter, stats.DefaultStats, &options.ClearDB, versionHandler)
		gateway.StartWebHandler()
	}
	//go readIOforResume(router) //keeping it as input from IO, to be replaced by UI
}

func (monolith *MonolithAppType) HandleRecovery(options *app.Options) {
	HandleRecovery(options.NormalMode, options.DegradedMode, options.MigrationMode, misc.AppStartTime)
}

// HandleRecovery decides the recovery Mode in which app should run based on earlier crashes
func HandleRecovery(forceNormal bool, forceDegraded bool, forceMigrationMode string, currTime int64) {

	enabled := config.GetBool("recovery.enabled", false)
	if !enabled {
		return
	}

	var forceMode string
	isForced := false

	//If MIGRATION_MODE environment variable is present and is equal to "import", "export", "import-export", then server mode is forced to be Migration.
	if db.IsValidMigrationMode(forceMigrationMode) {
		logger.Info("Setting server mode to Migration. If this is not intended remove environment variables related to Migration.")
		forceMode = db.MigrationMode
	} else {
		forceMode = db.GetForceRecoveryMode(forceNormal, forceDegraded)
	}

	recoveryData := db.GetRecoveryData(config.GetString("recovery.storagePath", "/tmp/recovery_data.json"))
	if forceMode != "" {
		isForced = true
		recoveryData.Mode = forceMode
	} else {
		//If no mode is forced (through env or cli) and if previous mode is migration then setting server mode to normal.
		if recoveryData.Mode == db.MigrationMode {
			recoveryData.Mode = db.NormalMode
		}
	}
	recoveryHandler := db.NewRecoveryHandler(&recoveryData)

	if !isForced && recoveryHandler.HasThresholdReached() {
		logger.Info("DB Recovery: Moving to next State. Threshold reached for " + recoveryData.Mode)
		nextMode := db.GetNextMode(recoveryData.Mode)
		if nextMode == "" {
			logger.Fatal("Threshold reached for degraded mode")
			panic("Not a valid mode")
		} else {
			recoveryData.Mode = nextMode
			recoveryHandler = db.NewRecoveryHandler(&recoveryData)
			db.AlertOps(recoveryData.Mode)
		}
	}

	recoveryHandler.RecordAppStart(currTime)
	db.SaveRecoveryData(recoveryData, config.GetString("recovery.storagePath", "/tmp/recovery_data.json"))
	recoveryHandler.Handle()
	logger.Infof("Starting in %s mode", recoveryData.Mode)
	db.CurrentMode = recoveryData.Mode
	rruntime.Go(func() {
		db.SendRecoveryModeStat()
	})
}
