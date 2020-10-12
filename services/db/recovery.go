package db

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"time"

	"github.com/rudderlabs/rudder-server/services/alert"
	"github.com/rudderlabs/rudder-server/services/stats"

	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/utils/logger"
)

const (
	NormalMode    = "normal"
	DegradedMode  = "degraded"
	MigrationMode = "migration"
)

type RecoveryHandler interface {
	RecordAppStart(int64)
	HasThresholdReached() bool
	Handle()
}

var CurrentMode string = NormalMode // default mode

// RecoveryDataT : DS to store the recovery process data
type RecoveryDataT struct {
	StartTimes                      []int64
	ReadableStartTimes              []string
	DegradedModeStartTimes          []int64
	ReadableDegradedModeStartTimes  []string
	MigrationModeStartTimes         []int64
	ReadableMigrationModeStartTimes []string
	Mode                            string
}

func GetRecoveryData(storagePath string) RecoveryDataT {
	data, err := ioutil.ReadFile(storagePath)
	if os.IsNotExist(err) {
		defaultRecoveryJSON := "{\"mode\":\"" + NormalMode + "\"}"
		data = []byte(defaultRecoveryJSON)
	} else {
		if err != nil {
			panic(err)
		}
	}

	var recoveryData RecoveryDataT
	err = json.Unmarshal(data, &recoveryData)
	if err != nil {
		panic(err)
	}

	return recoveryData
}

func SaveRecoveryData(recoveryData RecoveryDataT, storagePath string) {
	recoveryDataJSON, err := json.MarshalIndent(&recoveryData, "", " ")
	err = ioutil.WriteFile(storagePath, recoveryDataJSON, 0644)
	if err != nil {
		panic(err)
	}
}

// IsNormalMode checks if the current mode is normal
func IsNormalMode() bool {
	return CurrentMode == NormalMode
}

/*
CheckOccurences : check if this occurred numTimes times in numSecs seconds
*/
func CheckOccurences(occurences []int64, numTimes int, numSecs int) (occurred bool) {

	sort.Slice(occurences, func(i, j int) bool {
		return occurences[i] < occurences[j]
	})

	recentOccurences := 0
	checkPointTime := time.Now().Unix() - int64(numSecs)

	for i := len(occurences) - 1; i >= 0; i-- {
		if occurences[i] < checkPointTime {
			break
		}
		recentOccurences++
	}
	if recentOccurences >= numTimes {
		occurred = true
	}
	return
}

func GetForceRecoveryMode(forceNormal bool, forceDegraded bool) string {
	switch {
	case forceNormal:
		return NormalMode
	case forceDegraded:
		return DegradedMode
	}
	return ""

}

func GetNextMode(currentMode string) string {
	switch currentMode {
	case NormalMode:
		return DegradedMode
	case DegradedMode:
		return ""
	case MigrationMode: //Staying in the MigrationMode forever on repeated restarts.
		return MigrationMode
	}
	return ""
}

func NewRecoveryHandler(recoveryData *RecoveryDataT) RecoveryHandler {
	var recoveryHandler RecoveryHandler
	switch recoveryData.Mode {
	case NormalMode:
		recoveryHandler = &NormalModeHandler{recoveryData: recoveryData}
	case DegradedMode:
		recoveryHandler = &DegradedModeHandler{recoveryData: recoveryData}
	case MigrationMode:
		recoveryHandler = &MigrationModeHandler{recoveryData: recoveryData}
	default:
		panic("Invalid Recovery Mode " + recoveryData.Mode)
	}
	return recoveryHandler
}

func AlertOps(mode string) {
	instanceName := config.GetEnv("INSTANCE_ID", "")

	alertManager, err := alert.New()
	if err != nil {
		logger.Errorf("Unable to initialize the alertManager: %s", err.Error())
	} else {
		alertManager.Alert(fmt.Sprintf("Dataplane server %s entered %s mode", instanceName, mode))
	}
}

// sendRecoveryModeStat sends the recovery mode metric every 10 seconds
func SendRecoveryModeStat() {
	recoveryModeStat := stats.NewStat("recovery.mode_normal", stats.GaugeType)
	for {
		time.Sleep(10 * time.Second)
		switch CurrentMode {
		case NormalMode:
			recoveryModeStat.Gauge(1)
		case DegradedMode:
			recoveryModeStat.Gauge(2)
		case MigrationMode:
			recoveryModeStat.Gauge(4)
		}
	}
}
