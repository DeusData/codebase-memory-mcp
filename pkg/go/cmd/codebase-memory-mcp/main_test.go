package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

func TestExecutionPathForOSUsesAdjacentWindowsLauncher(t *testing.T) {
	payload := filepath.Join("cache", version, windowsPayloadName)
	want := filepath.Join("cache", version, windowsLauncherName)

	if got := executionPathForOS(payload, "windows"); got != want {
		t.Fatalf("Windows execution path = %q, want adjacent launcher %q", got, want)
	}
	if got := executionPathForOS(payload, "linux"); got != payload {
		t.Fatalf("non-Windows execution path = %q, want payload %q", got, payload)
	}
}

func TestPortableMutationActionPreservesPackageManagerRefusal(t *testing.T) {
	cases := []struct {
		args []string
		want string
	}{
		{args: []string{"update", "--yes"}, want: "update"},
		{args: []string{"uninstall", "--yes"}, want: "uninstall"},
		{args: []string{"install", "--yes"}, want: ""},
		{args: []string{"cli", "update"}, want: ""},
	}

	for _, testCase := range cases {
		if got := portableMutationAction(testCase.args); got != testCase.want {
			t.Errorf("portableMutationAction(%q) = %q, want %q", testCase.args, got, testCase.want)
		}
	}
}

func writeTestWindowsPair(t *testing.T, directory, tag string) {
	t.Helper()
	if err := os.MkdirAll(directory, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(
		filepath.Join(directory, windowsLauncherName),
		[]byte("launcher:"+tag), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(
		filepath.Join(directory, windowsPayloadName),
		[]byte("payload:"+tag), 0755); err != nil {
		t.Fatal(err)
	}
}

func verifyTestWindowsPair(launcherPath string) error {
	launcher, err := os.ReadFile(launcherPath)
	if err != nil {
		return err
	}
	payload, err := os.ReadFile(
		filepath.Join(filepath.Dir(launcherPath), windowsPayloadName))
	if err != nil {
		return err
	}
	const prefix = "launcher:"
	if !strings.HasPrefix(string(launcher), prefix) {
		return fmt.Errorf("invalid launcher test fixture")
	}
	tag := strings.TrimPrefix(string(launcher), prefix)
	if string(payload) != "payload:"+tag {
		return fmt.Errorf("launcher/payload test fixture mismatch")
	}
	return nil
}

func assertTestWindowsPair(t *testing.T, directory, tag string) {
	t.Helper()
	launcher, err := os.ReadFile(filepath.Join(directory, windowsLauncherName))
	if err != nil {
		t.Fatal(err)
	}
	payload, err := os.ReadFile(filepath.Join(directory, windowsPayloadName))
	if err != nil {
		t.Fatal(err)
	}
	if string(launcher) != "launcher:"+tag || string(payload) != "payload:"+tag {
		t.Fatalf("pair = %q / %q, want coherent tag %q", launcher, payload, tag)
	}
}

func TestWindowsPairPublicationRepairsCorruptAndPartialCaches(t *testing.T) {
	tests := []struct {
		name  string
		setup func(t *testing.T, destination string)
	}{
		{
			name: "corrupt launcher",
			setup: func(t *testing.T, destination string) {
				writeTestWindowsPair(t, destination, "old")
				if err := os.WriteFile(
					filepath.Join(destination, windowsLauncherName),
					[]byte("corrupt"), 0755); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "corrupt payload",
			setup: func(t *testing.T, destination string) {
				writeTestWindowsPair(t, destination, "old")
				if err := os.WriteFile(
					filepath.Join(destination, windowsPayloadName),
					[]byte("corrupt"), 0755); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "launcher only",
			setup: func(t *testing.T, destination string) {
				if err := os.MkdirAll(destination, 0755); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(
					filepath.Join(destination, windowsLauncherName),
					[]byte("launcher:partial"), 0755); err != nil {
					t.Fatal(err)
				}
			},
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			root := t.TempDir()
			source := filepath.Join(root, "source")
			destination := filepath.Join(root, "destination")
			writeTestWindowsPair(t, source, "candidate")
			testCase.setup(t, destination)

			if err := installWindowsPairAtomicallyWithVerifier(
				source, destination, verifyTestWindowsPair); err != nil {
				t.Fatal(err)
			}
			assertTestWindowsPair(t, destination, "candidate")
		})
	}
}

func TestWindowsPairPublicationSerializesConcurrentCandidates(t *testing.T) {
	root := t.TempDir()
	destination := filepath.Join(root, "destination")
	if err := os.MkdirAll(destination, 0755); err != nil {
		t.Fatal(err)
	}
	sources := []string{filepath.Join(root, "source-a"), filepath.Join(root, "source-b")}
	writeTestWindowsPair(t, sources[0], "a")
	writeTestWindowsPair(t, sources[1], "b")

	start := make(chan struct{})
	errors := make(chan error, len(sources))
	var wait sync.WaitGroup
	for _, source := range sources {
		wait.Add(1)
		go func(candidate string) {
			defer wait.Done()
			<-start
			errors <- installWindowsPairAtomicallyWithVerifier(
				candidate, destination, verifyTestWindowsPair)
		}(source)
	}
	close(start)
	wait.Wait()
	close(errors)
	for err := range errors {
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := verifyTestWindowsPair(
		filepath.Join(destination, windowsLauncherName)); err != nil {
		t.Fatal(err)
	}
	launcher, err := os.ReadFile(filepath.Join(destination, windowsLauncherName))
	if err != nil {
		t.Fatal(err)
	}
	tag := strings.TrimPrefix(string(launcher), "launcher:")
	if tag != "a" && tag != "b" {
		t.Fatalf("unexpected concurrent winner tag %q", tag)
	}
	assertTestWindowsPair(t, destination, tag)
}
