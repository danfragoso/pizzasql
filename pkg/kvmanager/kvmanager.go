package kvmanager

import (
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/goccy/go-json"
)

// KVInfo contains information about the running PizzaKV instance
type KVInfo struct {
	PID  int    `json:"pid"`
	Port int    `json:"port"`
	Addr string `json:"addr"`
}

// Manager handles the lifecycle of a PizzaKV process
type Manager struct {
	cmd      *exec.Cmd
	infoFile string
	info     *KVInfo
}

// NewManager creates a new KVManager
func NewManager() *Manager {
	return &Manager{
		infoFile: ".pizzakv.json",
	}
}

// SetInfoFile sets a custom path for the info file
func (m *Manager) SetInfoFile(path string) {
	m.infoFile = path
}

// Start launches pizzakv with the given flags on a random available port
func (m *Manager) Start(kvFlags string) (*KVInfo, error) {
	// Find an available port between 1024-9999
	port, err := findAvailablePortInRange(1024, 9999)
	if err != nil {
		return nil, fmt.Errorf("failed to find available port: %w", err)
	}

	// Build the command arguments
	// PizzaKV uses -port=XXXX format (single dash)
	args := []string{fmt.Sprintf("-port=%d", port)}

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
		Port: port,
		Addr: fmt.Sprintf("localhost:%d", port),
	}

	// Wait for the process to start and begin listening
	// We need to wait longer to ensure PizzaKV is actually listening
	time.Sleep(500 * time.Millisecond)

	// Check if process is still running
	if !m.IsRunning() {
		return nil, fmt.Errorf("pizzakv process exited immediately after starting")
	}

	fmt.Println("Waiting for PizzaKV to be ready...")

	// Wait for PizzaKV to be ready (finish restoring records, etc.)
	if err := m.waitForReady(port, 30*time.Second); err != nil {
		m.Stop()
		return nil, fmt.Errorf("pizzakv did not become ready: %w", err)
	}

	// Write info to file
	if err := m.writeInfoFile(); err != nil {
		m.Stop()
		return nil, fmt.Errorf("failed to write info file: %w", err)
	}

	return m.info, nil
}

// Stop stops the pizzakv process
func (m *Manager) Stop() error {
	if m.cmd == nil || m.cmd.Process == nil {
		return nil
	}

	// Try graceful shutdown first
	if err := m.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		// If SIGTERM fails, try SIGKILL
		if err := m.cmd.Process.Kill(); err != nil {
			return fmt.Errorf("failed to kill process: %w", err)
		}
	}

	// Wait for process to exit with timeout
	done := make(chan error, 1)
	go func() {
		_, err := m.cmd.Process.Wait()
		done <- err
	}()

	select {
	case <-done:
		// Process exited
	case <-time.After(5 * time.Second):
		// Timeout, force kill
		m.cmd.Process.Kill()
	}

	// Clean up info file
	os.Remove(m.infoFile)

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

// waitForReady waits for PizzaKV to be ready to accept connections
func (m *Manager) waitForReady(port int, timeout time.Duration) error {
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		// Check if process is still running
		if !m.IsRunning() {
			return fmt.Errorf("process died while waiting for ready")
		}

		// Try to connect
		conn, err := net.DialTimeout("tcp", addr, 500*time.Millisecond)
		if err == nil {
			conn.Close()
			// Successfully connected, PizzaKV is ready
			return nil
		}

		// Wait a bit before retrying
		time.Sleep(100 * time.Millisecond)
	}

	return fmt.Errorf("timeout waiting for PizzaKV to become ready on port %d", port)
}

// GetInfo returns the KVInfo for the running instance
func (m *Manager) GetInfo() *KVInfo {
	return m.info
}

// LoadInfo loads KVInfo from the info file
func (m *Manager) LoadInfo() (*KVInfo, error) {
	data, err := os.ReadFile(m.infoFile)
	if err != nil {
		return nil, fmt.Errorf("failed to read info file: %w", err)
	}

	var info KVInfo
	if err := json.Unmarshal(data, &info); err != nil {
		return nil, fmt.Errorf("failed to parse info file: %w", err)
	}

	return &info, nil
}

// writeInfoFile writes the KVInfo to a file
func (m *Manager) writeInfoFile() error {
	data, err := json.MarshalIndent(m.info, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal info: %w", err)
	}

	// Create directory if it doesn't exist
	dir := filepath.Dir(m.infoFile)
	if dir != "." {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return fmt.Errorf("failed to create directory: %w", err)
		}
	}

	if err := os.WriteFile(m.infoFile, data, 0644); err != nil {
		return fmt.Errorf("failed to write info file: %w", err)
	}

	return nil
}

// findAvailablePort finds a random available port (kept for compatibility)
func findAvailablePort() (int, error) {
	return findAvailablePortInRange(1024, 65535)
}

// findAvailablePortInRange finds a random available port within the specified range
func findAvailablePortInRange(minPort, maxPort int) (int, error) {
	// Try up to 100 times to find an available port
	for i := 0; i < 100; i++ {
		// Generate random port in range
		port := minPort + (int(time.Now().UnixNano()) % (maxPort - minPort + 1))

		// Try to listen on this port
		addr := fmt.Sprintf("127.0.0.1:%d", port)
		listener, err := net.Listen("tcp", addr)
		if err != nil {
			// Port is in use, try another
			continue
		}
		defer listener.Close()

		// Port is available
		return port, nil
	}

	return 0, fmt.Errorf("could not find available port in range %d-%d after 100 attempts", minPort, maxPort)
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

// CleanupStaleProcess checks if there's a stale PID file and cleans it up
func CleanupStaleProcess(infoFile string) error {
	data, err := os.ReadFile(infoFile)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // No file, nothing to clean
		}
		return err
	}

	var info KVInfo
	if err := json.Unmarshal(data, &info); err != nil {
		// Invalid file, just remove it
		return os.Remove(infoFile)
	}

	// Check if process is still running
	process, err := os.FindProcess(info.PID)
	if err != nil {
		// Process doesn't exist, remove file
		return os.Remove(infoFile)
	}

	// Try to signal the process
	err = process.Signal(syscall.Signal(0))
	if err != nil {
		// Process is dead, remove file
		return os.Remove(infoFile)
	}

	// Process exists, but is it actually PizzaKV responding on that port?
	// Try to connect to the port
	addr := fmt.Sprintf("127.0.0.1:%d", info.Port)
	conn, err := net.DialTimeout("tcp", addr, 1*time.Second)
	if err != nil {
		// Port is not responding, process might be stale or not PizzaKV
		// Remove the file and let user launch a new instance
		return os.Remove(infoFile)
	}
	conn.Close()

	// Process is still running and responding on the port
	return fmt.Errorf("pizzakv process (PID %d) is already running on port %d", info.PID, info.Port)
}

// KillExisting kills an existing pizzakv process based on the info file
func KillExisting(infoFile string) error {
	data, err := os.ReadFile(infoFile)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // No file, nothing to kill
		}
		return err
	}

	var info KVInfo
	if err := json.Unmarshal(data, &info); err != nil {
		// Invalid file, just remove it
		return os.Remove(infoFile)
	}

	// Try to kill the process
	process, err := os.FindProcess(info.PID)
	if err != nil {
		// Process doesn't exist, remove file
		return os.Remove(infoFile)
	}

	// Try SIGTERM first
	if err := process.Signal(syscall.SIGTERM); err == nil {
		// Wait a bit for graceful shutdown
		time.Sleep(1 * time.Second)

		// Check if still running
		if err := process.Signal(syscall.Signal(0)); err == nil {
			// Still running, force kill
			process.Kill()
		}
	} else {
		// SIGTERM failed, try SIGKILL
		process.Kill()
	}

	// Remove the info file
	return os.Remove(infoFile)
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
