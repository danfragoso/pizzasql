package kvmanager

import "testing"

func TestParseKVAddr(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		wantNetwork string
		wantTarget  string
	}{
		{
			name:        "tcp address",
			input:       "localhost:8085",
			wantNetwork: "tcp",
			wantTarget:  "localhost:8085",
		},
		{
			name:        "unix socket",
			input:       "unix:.pizzakv.sock",
			wantNetwork: "unix",
			wantTarget:  ".pizzakv.sock",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			network, target := parseKVAddr(tt.input)
			if network != tt.wantNetwork {
				t.Errorf("Expected network %q, got %q", tt.wantNetwork, network)
			}
			if target != tt.wantTarget {
				t.Errorf("Expected target %q, got %q", tt.wantTarget, target)
			}
		})
	}
}

func TestParseFlags(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected []string
	}{
		{
			name:     "empty string",
			input:    "",
			expected: nil,
		},
		{
			name:     "single flag",
			input:    "-iwal",
			expected: []string{"-iwal"},
		},
		{
			name:     "multiple flags",
			input:    "-iwal -port=9090",
			expected: []string{"-iwal", "-port=9090"},
		},
		{
			name:     "flags with quotes",
			input:    "-path=\"/tmp/my path\" -verbose",
			expected: []string{"-path=/tmp/my path", "-verbose"},
		},
		{
			name:     "flags with extra spaces",
			input:    "  -iwal   -port=9090  ",
			expected: []string{"-iwal", "-port=9090"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := parseFlags(tt.input)

			if len(result) != len(tt.expected) {
				t.Errorf("Expected %d args, got %d", len(tt.expected), len(result))
				return
			}

			for i := range result {
				if result[i] != tt.expected[i] {
					t.Errorf("Arg %d: expected %q, got %q", i, tt.expected[i], result[i])
				}
			}
		})
	}
}

func TestParsePort(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected int
		wantErr  bool
	}{
		{
			name:     "valid address",
			input:    "localhost:8085",
			expected: 8085,
			wantErr:  false,
		},
		{
			name:     "valid IP address",
			input:    "127.0.0.1:9090",
			expected: 9090,
			wantErr:  false,
		},
		{
			name:     "invalid format",
			input:    "localhost",
			expected: 0,
			wantErr:  true,
		},
		{
			name:     "invalid port",
			input:    "localhost:abc",
			expected: 0,
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := ParsePort(tt.input)

			if tt.wantErr {
				if err == nil {
					t.Error("Expected error but got none")
				}
				return
			}

			if err != nil {
				t.Errorf("Unexpected error: %v", err)
				return
			}

			if result != tt.expected {
				t.Errorf("Expected port %d, got %d", tt.expected, result)
			}
		})
	}
}

func TestKVInfo(t *testing.T) {
	info := &KVInfo{
		PID:  12345,
		Addr: "unix:.pizzakv.sock",
	}

	if info.PID != 12345 {
		t.Errorf("Expected PID 12345, got %d", info.PID)
	}

	if info.Addr != "unix:.pizzakv.sock" {
		t.Errorf("Expected Addr 'unix:.pizzakv.sock', got %s", info.Addr)
	}
}
