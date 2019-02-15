package storage

import (
	"context"
	"log"
	"os"
	"strconv"

	lbcf "github.com/lidstromberg/config"
)

var (
	//EnvDebugOn controls verbose logging
	EnvDebugOn bool
	//EnvClientPool is the size of the client pool
	EnvClientPool int
)

//preflight config checks
func preflight(ctx context.Context, bc lbcf.ConfigSetting) {
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds | log.LUTC)
	log.Println("Started Storage preflight..")

	//get the session config and apply it to the config
	bc.LoadConfigMap(ctx, preflightConfigLoader())

	//then check that we have everything we need
	if bc.GetConfigValue(ctx, "EnvDebugOn") == "" {
		log.Fatal("Could not parse environment variable EnvDebugOn")
	}

	if bc.GetConfigValue(ctx, "EnvStorClientPool") == "" {
		log.Fatal("Could not parse environment variable EnvStorClientPool")
	}

	//set the debug value
	constlog, err := strconv.ParseBool(bc.GetConfigValue(ctx, "EnvDebugOn"))

	if err != nil {
		log.Fatal("Could not parse environment variable EnvDebugOn")
	}

	EnvDebugOn = constlog

	//set the poolsize
	pl, err := strconv.ParseInt(bc.GetConfigValue(ctx, "EnvStorClientPool"), 10, 64)

	if err != nil {
		log.Fatal("Could not parse environment variable EnvStorClientPool")
	}

	EnvClientPool = int(pl)

	log.Println("..Finished Storage preflight.")
}

//preflightConfigLoader loads the config vars
func preflightConfigLoader() map[string]string {
	cfm := make(map[string]string)

	//EnvDebugOn controls verbose logging
	cfm["EnvDebugOn"] = os.Getenv("LB_DEBUGON")
	//EnvStorClientPool is the client poolsize
	cfm["EnvStorClientPool"] = os.Getenv("STOR_CLIPOOL")

	if cfm["EnvDebugOn"] == "" {
		log.Fatal("Could not parse environment variable EnvDebugOn")
	}

	if cfm["EnvStorClientPool"] == "" {
		log.Fatal("Could not parse environment variable EnvStorClientPool")
	}

	return cfm
}
