package grpc

import (
	"context"
	"fmt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"
	grpctrace "gopkg.in/DataDog/dd-trace-go.v1/contrib/google.golang.org/grpc"
	"log"
	"net"
	"os"
	"strconv"
)

// ServiceRegistrar is an interface that should be implemented by any service that wants to register itself with the grpc server
type ServiceRegistrar interface {
	RegisterServer(server *grpc.Server)
}

const DdTraceEnabledEnv = "DD_TRACE_ENABLED"

// isTracingEnabled checks if tracing is enabled
func isTracingEnabled() bool {
	traceEnabled, _ := strconv.ParseBool(os.Getenv(DdTraceEnabledEnv))
	return traceEnabled
}

// ListenForConnections starts a grpc server and listens for connections
func ListenForConnections(ctx context.Context, registrar ServiceRegistrar, addr, serviceName string, enableReflection bool, traceServiceName string) error {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to listen on address %s: %v", addr, err)
	}
	defer lis.Close()

	var opts []grpc.ServerOption

	if isTracingEnabled() {
		ui := grpctrace.UnaryServerInterceptor(grpctrace.WithServiceName(traceServiceName))
		opts = append(opts, grpc.UnaryInterceptor(ui))
	}

	srv := grpc.NewServer(opts...)

	healthServer := health.NewServer()
	grpc_health_v1.RegisterHealthServer(srv, healthServer)

	healthServer.SetServingStatus(serviceName, grpc_health_v1.HealthCheckResponse_SERVING)

	registrar.RegisterServer(srv)

	if enableReflection {
		reflection.Register(srv)
	}

	log.Printf("%s listening at %s", serviceName, addr)

	go listenForStopped(ctx, srv, serviceName)

	if err = srv.Serve(lis); err != nil {
		return fmt.Errorf("failed to serve: %v", err)
	}
	return nil
}

// CreateClientConnection creates a grpc client connection
func CreateClientConnection(addr string, traceServiceName string) (*grpc.ClientConn, error) {
	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}

	if isTracingEnabled() {
		unaryInterceptor := grpc.WithUnaryInterceptor(grpctrace.UnaryClientInterceptor(grpctrace.WithServiceName(traceServiceName)))
		opts = append(opts, unaryInterceptor)
	}

	conn, err := grpc.Dial(addr, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create client connection to %s: %v", addr, err)
	}
	return conn, nil
}

// listenForStopped listens for a context to be done and stops the grpc server
func listenForStopped(ctx context.Context, grpcServer *grpc.Server, serviceName string) {
	<-ctx.Done()
	log.Printf("%s stopped", serviceName)
	grpcServer.Stop()
}
