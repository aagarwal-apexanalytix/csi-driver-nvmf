/* Copyright 2021 The Kubernetes Authors.
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package utils

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strings"
)

// IsFileExisting checks if a file exists.
// Returns true only if the file exists and is accessible.
// Returns false if it does not exist or on other errors (e.g., permission denied).
func IsFileExisting(filename string) bool {
	_, err := os.Stat(filename)
	return err == nil
}

// ParseEndpoint parses a CSI endpoint string (unix://path or tcp://addr:port).
// Returns protocol and address, or error if invalid.
func ParseEndpoint(ep string) (string, string, error) {
	epLower := strings.ToLower(ep)
	if strings.HasPrefix(epLower, "unix://") || strings.HasPrefix(epLower, "tcp://") {
		s := strings.SplitN(ep, "://", 2)
		if len(s) == 2 && s[1] != "" {
			return s[0], s[1], nil
		}
	}
	return "", "", fmt.Errorf("invalid endpoint: %v", ep)
}

// WriteStringToFile writes a string to an open file and flushes.
func WriteStringToFile(file *os.File, data string) error {
	writer := bufio.NewWriter(file)
	size, err := writer.WriteString(data + "\n") // Add newline for fabrics convention
	if err != nil {
		return err
	}
	if size == 0 {
		return fmt.Errorf("wrote zero bytes")
	}
	return writer.Flush()
}

// ReadLinesFromFile reads all lines from an open file (e.g., sysfs attributes).
// Trims whitespace and stops on EOF.
func ReadLinesFromFile(file *os.File) ([]string, error) {
	var lines []string
	reader := bufio.NewReader(file)

	for {
		line, err := reader.ReadString('\n')
		if line != "" {
			lines = append(lines, strings.TrimSpace(line))
		}
		if err != nil {
			if err == io.EOF {
				return lines, nil
			}
			return lines, err
		}
	}
}
