// This file defines CLI application wiring, flags, and command dispatch.
package cli

import (
	"context"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
)

// CLI is the ocs command-line runner.
type CLI struct {
	Stdout     io.Writer
	Stderr     io.Writer
	Stdin      io.Reader
	Endpoint   string
	JSON       bool
	Quiet      bool
	HTTPClient *http.Client
	client     *APIClient
}

// New creates a new CLI runner.
func New(stdout, stderr io.Writer, stdin io.Reader) *CLI {
	if stdout == nil {
		stdout = os.Stdout
	}
	if stderr == nil {
		stderr = os.Stderr
	}
	if stdin == nil {
		stdin = os.Stdin
	}

	endpoint := os.Getenv("OCS_ENDPOINT")
	if endpoint == "" {
		endpoint = "http://localhost:8080"
	}

	return &CLI{
		Stdout:   stdout,
		Stderr:   stderr,
		Stdin:    stdin,
		Endpoint: endpoint,
	}
}

func (c *CLI) bindGlobalFlags(fs *flag.FlagSet) {
	fs.StringVar(&c.Endpoint, "endpoint", c.Endpoint, "controller endpoint URL")
	fs.BoolVar(&c.JSON, "json", c.JSON, "output in raw JSON format")
	fs.BoolVar(&c.Quiet, "quiet", c.Quiet, "suppress progress indicators and informational messages")
}

var flagsWithValue = map[string]bool{
	"endpoint":     true,
	"meta":         true,
	"content-type": true,
}

func reorderArgs(args []string) []string {
	var flags []string
	var pos []string
	for i := 0; i < len(args); i++ {
		arg := args[i]
		if strings.HasPrefix(arg, "-") {
			flags = append(flags, arg)
			flagName := strings.TrimLeft(arg, "-")
			if strings.Contains(flagName, "=") {
				continue
			}
			if flagsWithValue[flagName] && i+1 < len(args) {
				flags = append(flags, args[i+1])
				i++
			}
		} else {
			pos = append(pos, arg)
		}
	}
	return append(flags, pos...)
}

func (c *CLI) errorf(code, format string, args ...any) {
	msg := fmt.Sprintf(format, args...)
	fmt.Fprintf(c.Stderr, "error: %s: %s\n", code, msg)
}

func (c *CLI) handleError(err error) {
	if err == nil {
		return
	}
	s := err.Error()
	parts := strings.SplitN(s, ": ", 2)
	if len(parts) == 2 && parts[0] == strings.ToUpper(parts[0]) {
		c.errorf(parts[0], "%s", parts[1])
	} else {
		c.errorf("INTERNAL", "%s", s)
	}
}

// Run executes the CLI command with the given argument slice.
func (c *CLI) Run(args []string) int {
	return c.RunWithContext(context.Background(), args)
}

// RunWithContext executes the CLI command with the given context and argument slice.
func (c *CLI) RunWithContext(ctx context.Context, args []string) int {
	c.JSON = false
	c.Quiet = false

	// Check for help flags
	for _, arg := range args {
		if arg == "-h" || arg == "--help" || arg == "help" {
			c.printUsage()
			return 0
		}
	}

	// Parse initial global flags before subcommand if any
	subcmdIdx := -1
	for i := 0; i < len(args); i++ {
		arg := args[i]
		if !strings.HasPrefix(arg, "-") {
			subcmdIdx = i
			break
		}
		if arg == "--endpoint" || arg == "-endpoint" {
			if i+1 < len(args) {
				c.Endpoint = args[i+1]
				i++
			}
		} else if strings.HasPrefix(arg, "--endpoint=") || strings.HasPrefix(arg, "-endpoint=") {
			parts := strings.SplitN(arg, "=", 2)
			c.Endpoint = parts[1]
		} else if arg == "--json" || arg == "-json" {
			c.JSON = true
		} else if arg == "--quiet" || arg == "-quiet" {
			c.Quiet = true
		}
	}

	if subcmdIdx == -1 {
		c.printUsage()
		return 2
	}

	subcmd := args[subcmdIdx]
	subArgs := args[subcmdIdx+1:]

	c.client = newAPIClient(c.Endpoint, c.HTTPClient)

	switch subcmd {
	case "cp":
		return c.handleCP(ctx, subArgs)
	case "cat":
		return c.handleCat(ctx, subArgs)
	case "ls":
		return c.handleLS(ctx, subArgs)
	case "stat":
		return c.handleStat(ctx, subArgs)
	case "meta":
		return c.handleMeta(ctx, subArgs)
	case "rm":
		return c.handleRM(ctx, subArgs)
	case "status":
		return c.handleStatus(ctx, subArgs)
	case "verify":
		return c.handleVerify(ctx, subArgs)
	case "help", "-h", "--help":
		c.printUsage()
		return 0
	default:
		c.errorf("INVALID_USAGE", "unknown command %q", subcmd)
		c.printUsage()
		return 2
	}
}

func (c *CLI) printUsage() {
	fmt.Fprintln(c.Stderr, `Usage: ocs <command> [arguments] [flags]

Commands:
  cp <local> obj:<name> [--overwrite] [--meta k=v] [--content-type T]   Upload file
  cp obj:<name> <local|->                                                Download file ('-' for stdout)
  cat obj:<name>                                                         Stream object to stdout
  ls [obj:<prefix>] [-l] [-R]                                            List objects
  stat obj:<name>                                                        Object details & chunk map
  meta get|set|rm obj:<name> [k=v | k]                                   Manage custom metadata
  rm obj:<name> [-r]                                                     Delete object (or recursively)
  status                                                                 Cluster status & worker health
  verify obj:<name> <local-file>                                         Verify sha256 checksums

Global Flags:
  --endpoint <url>    Controller base URL (default: http://localhost:8080 or $OCS_ENDPOINT)
  --json              Output raw JSON
  --quiet             Suppress progress and informational logs`)
}
