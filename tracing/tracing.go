package tracing

import (
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"
	"gopkg.in/DataDog/dd-trace-go.v1/profiler"
	"log"
	"os"
	"strconv"
)

const DdTraceEnabledEnv = "DD_TRACE_ENABLED"
const DdProfilingEnabledEnv = "DD_PROFILING_ENABLED"

var (
	tracingEnabled   bool
	profilingEnabled bool
)

// init initializes the tracing and profiling flags
func init() {
	tracingEnabled, _ = strconv.ParseBool(os.Getenv(DdTraceEnabledEnv))
	profilingEnabled, _ = strconv.ParseBool(os.Getenv(DdProfilingEnabledEnv))
}

// Start starts the tracing and profiling
func Start() {
	if tracingEnabled {
		log.Println("Starting tracer...")
		tracer.Start()
	}

	if profilingEnabled {
		log.Println("Starting profiler...")
		err := profiler.Start(
			profiler.WithProfileTypes(
				profiler.CPUProfile,
				profiler.HeapProfile,
			),
		)

		if err != nil {
			log.Printf("Failed to start profiler: %s", err)
		}
	}
}

// Stop stops the tracing and profiling
func Stop() {
	if tracingEnabled {
		tracer.Stop()
	}

	if profilingEnabled {
		profiler.Stop()
	}
}
