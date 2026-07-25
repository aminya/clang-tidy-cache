//go:build !windows

package clang

import (
	"testing"
)

func TestSplitCommandStringHonoursPosixQuoting(t *testing.T) {
	words, err := splitCommandString(
		`/usr/bin/clang++ -DCMAKE_INTDIR=\"Debug\" '-DGREETING=hello world' -c src/main.cpp -o main.cpp.o`,
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	expected := []string{
		"/usr/bin/clang++",
		`-DCMAKE_INTDIR="Debug"`,
		"-DGREETING=hello world",
		"-c",
		"src/main.cpp",
		"-o",
		"main.cpp.o",
	}

	if len(words) != len(expected) {
		t.Fatalf("words = %q, want %q", words, expected)
	}
	for i := range expected {
		if words[i] != expected[i] {
			t.Errorf("words[%d] = %q, want %q", i, words[i], expected[i])
		}
	}
}
