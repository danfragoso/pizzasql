package kvmanager

import (
	"fmt"
	"net"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
	"time"

	pizzaruntime "github.com/danfragoso/pizzasql-next/pkg/runtime"
)

// KVInfo is an alias for the runtime package type.
type KVInfo = pizzaruntime.KVInfo

// Manager handles the lifecycle of a PizzaKV process
type Manager struct {
	cmd  *exec.Cmd
	info *KVInfo
}

// NewManager creates a new KVManager
func NewManager() *Manager {
	return &Manager{}
}

// Start launches pizzakv with the given flags using a Unix socket by default.
func (m *Manager) Start(kvFlags string) (*KVInfo, error) {
	sockPath := ".pizzakv.sock"
	args := []string{"-unix"}

	// Parse and add custom flags if provided
	if kvFlags != "" {
		customArgs := parseFlags(kvFlags)
		args = append(args, customArgs...)
	}

	// Create the command
	cmd := exec.Command("pizzakv", args...)

	// Set up process group to allow clean shutdown
	cmd.SysProcAttr = &syscall.SysProcAttr{
		Setpgid: true,
	}

	// Redirect output to /dev/null or capture it
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	// Start the process
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("failed to start pizzakv: %w", err)
	}

	m.cmd = cmd
	m.info = &KVInfo{
		PID:  cmd.Process.Pid,
		Addr: "unix:" + sockPath,
	}

	time.Sleep(500 * time.Millisecond)

	if !m.IsRunning() {
		return nil, fmt.Errorf("pizzakv process exited immediately after starting")
	}

	fmt.Println("Waiting for PizzaKV to be ready...")

	if err := m.waitForReady(m.info.Addr, 30*time.Second); err != nil {
		m.Stop()
		return nil, fmt.Errorf("pizzakv did not become ready: %w", err)
	}

	if err := pizzaruntime.WriteKV(m.info); err != nil {
		m.Stop()
		return nil, fmt.Errorf("failed to write runtime file: %w", err)
	}

	return m.info, nil
}

// Stop stops the pizzakv process
func (m *Manager) Stop() error {
	if m.cmd == nil || m.cmd.Process == nil {
		return nil
	}

	if err := m.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		if err := m.cmd.Process.Kill(); err != nil {
			return fmt.Errorf("failed to kill process: %w", err)
		}
	}

	done := make(chan error, 1)
	go func() {
		_, err := m.cmd.Process.Wait()
		done <- err
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		m.cmd.Process.Kill()
	}

	return nil
}

// IsRunning checks if the pizzakv process is still running
func (m *Manager) IsRunning() bool {
	if m.cmd == nil || m.cmd.Process == nil {
		return false
	}

	// Send signal 0 to check if process exists
	err := m.cmd.Process.Signal(syscall.Signal(0))
	return err == nil
}

// waitForReady waits for PizzaKV to be ready to accept connections.
func (m *Manager) waitForReady(addr string, timeout time.Duration) error {
	network, target := parseKVAddr(addr)
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		if !m.IsRunning() {
			return fmt.Errorf("process died while waiting for ready")
		}

		conn, err := net.DialTimeout(network, target, 500*time.Millisecond)
		if err == nil {
			conn.Close()
			return nil
		}

		time.Sleep(100 * time.Millisecond)
	}

	return fmt.Errorf("timeout waiting for PizzaKV to become ready at %s", addr)
}

func parseKVAddr(addr string) (network, target string) {
	if strings.HasPrefix(addr, "unix:") {
		return "unix", strings.TrimPrefix(addr, "unix:")
	}
	return "tcp", addr
}

// GetInfo returns the KVInfo for the running instance
func (m *Manager) GetInfo() *KVInfo {
	return m.info
}

// LoadInfo loads KVInfo from the runtime file
func (m *Manager) LoadInfo() (*KVInfo, error) {
	info, err := pizzaruntime.Load()
	if err != nil {
		return nil, err
	}
	if info.PizzaKV == nil {
		return nil, fmt.Errorf("no pizzakv info in runtime file")
	}
	return info.PizzaKV, nil
}

// parseFlags parses a flag string like "-iwal -port=9090" into a slice of strings
func parseFlags(flags string) []string {
	// Trim whitespace
	flags = strings.TrimSpace(flags)
	if flags == "" {
		return nil
	}

	var result []string
	var current strings.Builder
	inQuote := false

	for i, r := range flags {
		switch r {
		case '"', '\'':
			inQuote = !inQuote
		case ' ':
			if !inQuote {
				if current.Len() > 0 {
					result = append(result, current.String())
					current.Reset()
				}
			} else {
				current.WriteRune(r)
			}
		default:
			current.WriteRune(r)
		}

		// Handle last character
		if i == len(flags)-1 && current.Len() > 0 {
			result = append(result, current.String())
		}
	}

	return result
}

// ParsePort parses a port from a string (e.g., "localhost:8085" -> 8085)
func ParsePort(addr string) (int, error) {
	parts := strings.Split(addr, ":")
	if len(parts) != 2 {
		return 0, fmt.Errorf("invalid address format: %s", addr)
	}

	port, err := strconv.Atoi(parts[1])
	if err != nil {
		return 0, fmt.Errorf("invalid port: %s", parts[1])
	}

	return port, nil
}
