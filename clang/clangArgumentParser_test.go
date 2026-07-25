package clang

import (
	"testing"
)

func TestParseClangCommandStringUsesArguments(t *testing.T) {
	entry := &DatabaseEntry{
		Arguments: []string{
			"./bin/clang++",
			"-DNOMINMAX",
			"-c", "src/main.cpp",
			"-o", "main.cpp.o",
		},
	}

	cmd, err := ParseClangCommandString(entry)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// the leading "./" is stripped by the normalization
	if cmd.Compiler != "bin/clang++" {
		t.Errorf("compiler = %q, want %q", cmd.Compiler, "bin/clang++")
	}
	if cmd.InputPath != "src/main.cpp" {
		t.Errorf("input path = %q, want %q", cmd.InputPath, "src/main.cpp")
	}
	if cmd.OutputPath != "main.cpp.o" {
		t.Errorf("output path = %q, want %q", cmd.OutputPath, "main.cpp.o")
	}
	if len(cmd.Arguments) != 1 || cmd.Arguments[0] != "-DNOMINMAX" {
		t.Errorf("arguments = %q, want [-DNOMINMAX]", cmd.Arguments)
	}
}

func TestParseClangCommandStringWithoutWords(t *testing.T) {
	if _, err := ParseClangCommandString(&DatabaseEntry{}); err == nil {
		t.Error("expected an error for an entry without a command or arguments")
	}
}

func TestParseClangCommandStringWithoutInputOrOutput(t *testing.T) {
	entry := &DatabaseEntry{Arguments: []string{"clang++", "-DNOMINMAX"}}

	if _, err := ParseClangCommandString(entry); err == nil {
		t.Error("expected an error when the input and output paths are missing")
	}
}
