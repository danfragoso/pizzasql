package runtime

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/goccy/go-json"
)

var BaseDir = filepath.Join(os.TempDir(), "pizzasql")

type KVInfo struct {
	PID  int    `json:"pid"`
	Port int    `json:"port"`
	Addr string `json:"addr"`
}

type ProcessInfo struct {
	PID      int `json:"pid"`
	HTTPPort int `json:"http_port,omitempty"`
	PGPort   int `json:"pg_port,omitempty"`
}

type Info struct {
	PizzaSQL *ProcessInfo `json:"pizzasql,omitempty"`
	PizzaKV  *KVInfo      `json:"pizzakv,omitempty"`
}

func instanceDir(pid int) string {
	return filepath.Join(BaseDir, strconv.Itoa(pid))
}

func instanceFile(pid int) string {
	return filepath.Join(instanceDir(pid), "runtime.json")
}

// File returns the runtime file path for the current process.
var File = instanceFile(os.Getpid())

func loadFile(path string) (*Info, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return &Info{}, nil
		}
		return nil, err
	}
	var info Info
	if err := json.Unmarshal(data, &info); err != nil {
		os.Remove(path)
		return &Info{}, nil
	}
	return &info, nil
}

func Load() (*Info, error) {
	return loadFile(instanceFile(os.Getpid()))
}

func write(info *Info) error {
	dir := instanceDir(os.Getpid())
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(info, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(instanceFile(os.Getpid()), data, 0644)
}

func pidAlive(pid int) bool {
	p, err := os.FindProcess(pid)
	if err != nil {
		return false
	}
	return p.Signal(syscall.Signal(0)) == nil
}

func addrResponds(addr string) bool {
	network, target := "tcp", addr
	if strings.HasPrefix(addr, "unix:") {
		network, target = "unix", strings.TrimPrefix(addr, "unix:")
	}
	conn, err := net.DialTimeout(network, target, time.Second)
	if err != nil {
		return false
	}
	conn.Close()
	return true
}

// LiveInstances returns all runtime files from other instances that have a live pizzasql PID.
func LiveInstances() []*Info {
	entries, err := os.ReadDir(BaseDir)
	if err != nil {
		return nil
	}
	selfPID := os.Getpid()
	var live []*Info
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		pid, err := strconv.Atoi(e.Name())
		if err != nil || pid == selfPID {
			continue
		}
		path := filepath.Join(BaseDir, e.Name(), "runtime.json")
		info, err := loadFile(path)
		if err != nil || info.PizzaSQL == nil {
			continue
		}
		if pidAlive(info.PizzaSQL.PID) {
			live = append(live, info)
		} else {
			os.RemoveAll(filepath.Join(BaseDir, e.Name()))
		}
	}
	return live
}

// CheckExistingInstances warns about live instances and prompts the user.
// Returns an error only if the user declines to continue.
func CheckExistingInstances() error {
	live := LiveInstances()
	if len(live) == 0 {
		return nil
	}

	fmt.Fprintf(os.Stderr, "Warning: %d pizzasql instance(s) already running:\n", len(live))
	for _, info := range live {
		extra := ""
		if info.PizzaSQL.HTTPPort != 0 {
			extra += fmt.Sprintf(" http=:%d", info.PizzaSQL.HTTPPort)
		}
		if info.PizzaSQL.PGPort != 0 {
			extra += fmt.Sprintf(" pg=:%d", info.PizzaSQL.PGPort)
		}
		if info.PizzaKV != nil {
			extra += fmt.Sprintf(" kv=%s", info.PizzaKV.Addr)
		}
		fmt.Fprintf(os.Stderr, "  PID %d%s\n", info.PizzaSQL.PID, extra)
	}
	fmt.Fprintf(os.Stderr, "Continue anyway? [y/N] ")

	reader := bufio.NewReader(os.Stdin)
	line, _ := reader.ReadString('\n')
	if line != "y\n" && line != "Y\n" {
		return fmt.Errorf("aborted")
	}
	return nil
}

// WritePizzaSQL records the pizzasql process in this instance's runtime file.
func WritePizzaSQL(pid, httpPort, pgPort int) error {
	info, err := Load()
	if err != nil {
		info = &Info{}
	}
	info.PizzaSQL = &ProcessInfo{PID: pid, HTTPPort: httpPort, PGPort: pgPort}
	return write(info)
}

// WriteKV records the pizzakv process in this instance's runtime file.
func WriteKV(kv *KVInfo) error {
	info, err := Load()
	if err != nil {
		info = &Info{}
	}
	info.PizzaKV = kv
	return write(info)
}

// Cleanup removes this instance's runtime directory.
func Cleanup() {
	os.RemoveAll(instanceDir(os.Getpid()))
}
