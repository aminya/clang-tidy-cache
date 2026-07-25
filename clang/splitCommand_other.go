//go:build !windows

package clang

import (
	"github.com/google/shlex"
)

// splitCommandString splits the "command" field of a compilation database
// entry into individual words using the POSIX shell quoting rules.
//
//	@param {string} command Command line to split
//
//	@returns {[]string} The individual words of the command line
func splitCommandString(command string) ([]string, error) {
	return shlex.Split(command)
}
