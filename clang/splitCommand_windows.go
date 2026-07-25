//go:build windows

package clang

import (
	"golang.org/x/sys/windows"
)

// splitCommandString splits the "command" field of a compilation database
// entry into individual words.
//
// On Windows the command line follows the CommandLineToArgvW rules, where "\"
// is a path separator and is only an escape character when it precedes a
// quote. Using a POSIX tokenizer here would strip the separators out of paths
// such as C:\Users\me\llvm\bin\clang++.exe.
//
//	@param {string} command Command line to split
//
//	@returns {[]string} The individual words of the command line
func splitCommandString(command string) ([]string, error) {
	return windows.DecomposeCommandLine(command)
}
