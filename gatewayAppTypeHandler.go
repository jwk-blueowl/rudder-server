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
	"github.com/rudderlabs/rudder-server/services/diagnostics"
	sourcedebugger "github.com/rudderlabs/rudder-server/services/source-debugger"
	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/services/validators"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"

	// This is necessary for compatibility with enterprise features
	_ "github.com/rudderlabs/rudder-server/imports"
)

//GatewayAppType is the type for Gateway type implemention
type GatewayAppType struct {
}

func (gatewayApp *GatewayAppType) GetAppType() string {
	return "rudder-server-gateway"
}

func (gatewayApp *GatewayAppType) StartRudderCore(options *app.Options) {
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

	runtime.GOMAXPROCS(maxProcess)
	logger.Info("Clearing DB ", options.ClearDB)

	sourcedebugger.Setup()

	migrationMode := application.Options().MigrationMode
	gatewayDB.Setup(options.ClearDB, "gw", gwDBRetention, migrationMode, false)

	enableGateway := true

	if application.Features().Migrator != nil {
		if migrationMode == db.IMPORT || migrationMode == db.EXPORT || migrationMode == db.IMPORT_EXPORT {
			enableGateway = (migrationMode != db.EXPORT)
		}
	}

	if enableGateway {
		var gateway gateway.HandleT
		var rateLimiter ratelimiter.HandleT

		rateLimiter.SetUp()
		gateway.Setup(application, backendconfig.DefaultBackendConfig, &gatewayDB, &rateLimiter, stats.DefaultStats, &options.ClearDB, versionHandler)
		gateway.StartWebHandler()
	}
	//go readIOforResume(router) //keeping it as input from IO, to be replaced by UI
}

func (gateway *GatewayAppType) HandleRecovery(options *app.Options) {
	fmt.Println("gatway handle recovery")
}

// HandleOnlyGatewayRecovery decides the recovery Mode in which app should run based on earlier crashes
func HandleOnlyGatewayRecovery(forceMigrationMode string, currTime int64) {
	enabled := config.GetBool("recovery.enabled", false)
	if !enabled {
		return
	}

	var forceMode string

	//If MIGRATION_MODE environment variable is present and is equal to "import", "export", "import-export", then server mode is forced to be Migration.
	if db.IsValidMigrationMode(forceMigrationMode) {
		logger.Info("Setting server mode to Migration. If this is not intended remove environment variables related to Migration.")
		forceMode = db.MigrationMode
	}

	recoveryData := db.GetRecoveryData(config.GetString("recovery.storagePath", "/tmp/recovery_data.json"))
	if forceMode != "" {
		recoveryData.Mode = forceMode
	} else {
		//If no mode is forced (through env or cli) and if previous mode is migration then setting server mode to normal.
		if recoveryData.Mode != db.NormalMode {
			recoveryData.Mode = db.NormalMode
		}
	}

	recoveryHandler := db.NewRecoveryHandler(&recoveryData)

	recoveryHandler.RecordAppStart(currTime)
	db.SaveRecoveryData(recoveryData, config.GetString("recovery.storagePath", "/tmp/recovery_data.json"))
	recoveryHandler.Handle()
	logger.Infof("Starting in %s mode", recoveryData.Mode)
	db.CurrentMode = recoveryData.Mode
	rruntime.Go(func() {
		db.SendRecoveryModeStat()
	})
}
