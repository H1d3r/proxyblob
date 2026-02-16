// Package main implements the SOCKS proxy agent.
package main

import (
	"context"
	"encoding/base64"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"os/user"
	"strings"
	"syscall"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	proxy "proxyblob/pkg/proxy/socks"

	"github.com/atsika/aznet"
)

// Exit codes.
const (
	Success                  = 0 // success
	ErrContextCanceled       = 1 // context canceled
	ErrNoConnectionString    = 2 // missing connection string
	ErrConnectionStringError = 3 // invalid connection string
)

// ConnString holds the Azure connection string.
// Can be set at compile time or via command line flag.
var ConnString string

// Agent manages proxy operations and aznet communication.
type Agent struct {
	Handler *proxy.SocksHandler // SOCKS handler
}

// NewAgent creates an agent from a connection string.
func NewAgent(ctx context.Context, connString string) (*Agent, int) {
	// Decode and parse the connection string
	driver, address, errCode := ParseConnectionString(connString)
	if errCode != Success {
		return nil, errCode
	}

	// Dial to the proxy using aznet with ULTRA-aggressive polling (WARNING: High API costs!)
	conn, err := aznet.Dial(driver, address, aznet.WithContext(ctx), aznet.WithFastPoll(time.Millisecond))
	if err != nil {
		log.Error().Err(err).Msg("Failed to dial proxy")
		return nil, ErrConnectionStringError
	}
	log.Info().Msg("Connected to proxy")

	// Send identity (user@host) as first raw message before protocol starts
	identity := getIdentity()
	if _, err := conn.Write([]byte(identity)); err != nil {
		log.Warn().Err(err).Msg("Failed to send identity")
	}

	// Create SOCKS handler with direct connection (no transport wrapper)
	handler := proxy.NewSocksHandler(ctx, conn)

	agent := &Agent{
		Handler: handler,
	}

	return agent, Success
}

// Start begins processing proxy requests.
func (a *Agent) Start(ctx context.Context) int {
	// Start the handler
	a.Handler.Start("")

	// Wait for handler context to be done
	<-a.Handler.Ctx.Done()

	// Check if context was canceled externally
	if errors.Is(ctx.Err(), context.Canceled) {
		return ErrContextCanceled
	}

	return Success
}

// Stop terminates agent operations.
func (a *Agent) Stop() {
	a.Handler.Stop()
}

// ParseConnectionString extracts the driver and aznet address from a connection string.
func ParseConnectionString(connString string) (string, string, int) {
	// Check for empty string first
	if connString == "" {
		return "", "", ErrNoConnectionString
	}

	// Try to decode the base64 encoded string
	decoded, err := base64.RawStdEncoding.DecodeString(connString)
	if err != nil {
		return "", "", ErrConnectionStringError
	}

	// Parse the string - it might be in the new format: driver|address
	// or the old format: address (where driver is extracted from scheme)
	str := string(decoded)
	if strings.Contains(str, "|") {
		parts := strings.SplitN(str, "|", 2)
		return parts[0], parts[1], Success
	}

	return "", "", ErrConnectionStringError
}

// getIdentity returns "user@hostname" for agent identification.
func getIdentity() string {
	username := "unknown"
	if u, err := user.Current(); err == nil {
		username = u.Username
	}
	hostname, err := os.Hostname()
	if err != nil {
		hostname = "unknown"
	}
	return fmt.Sprintf("%s@%s", username, hostname)
}

// init configures logging with zerolog
// Sets up console output and INFO level logging
func init() {
	// Configure logging
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnix
	zerolog.SetGlobalLevel(zerolog.InfoLevel)

	// Use a more human-friendly output for console
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})
}

// main is the entry point for the agent process
// Handles command-line flags, signal management, and agent lifecycle
func main() {
	// Parse command line flags
	flag.StringVar(&ConnString, "c", ConnString, "Connection string")
	flag.Parse()

	if ConnString == "" {
		os.Exit(ErrNoConnectionString)
	}

	// Create context that can be cancelled with CTRL+C
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle SIGINT (CTRL+C) and SIGTERM
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sig
		cancel()
	}()

	// Create the agent
	agent, err := NewAgent(ctx, ConnString)
	if err != Success {
		os.Exit(err)
	}

	// Start the agent
	ErrCode := agent.Start(ctx)
	os.Exit(ErrCode)
}
