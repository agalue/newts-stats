package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"log"
	"strings"
	"testing"

	"gotest.tools/v3/assert"
)

func TestGetKeys(t *testing.T) {
	data := map[string]int{
		"d": 1,
		"c": 2,
		"a": 3,
		"b": 4,
	}
	keys := getKeys(data)
	assert.Equal(t, "a", keys[0])
	assert.Equal(t, "b", keys[1])
	assert.Equal(t, "c", keys[2])
	assert.Equal(t, "d", keys[3])
}

func TestShowCardinality(t *testing.T) {
	var buf bytes.Buffer
	log.SetOutput(io.Writer(&buf))
	data := map[string]int{
		"cpuUsed":       2,
		"memUsed":       2,
		"threads":       100,
		"ifHCOutOctets": 800,
		"ifHCInOctets":  1000,
		"diskUsed":      30,
	}
	showCardinality(data, 3)
	scanner := bufio.NewScanner(&buf)
	line := 0
	for scanner.Scan() {
		if line > 0 {
			text := scanner.Text()
			fmt.Println(scanner.Text())
			switch line {
			case 1:
				assert.Assert(t, strings.Contains(text, "ifHCInOctets"))
			case 2:
				assert.Assert(t, strings.Contains(text, "ifHCOutOctets"))
			case 3:
				assert.Assert(t, strings.Contains(text, "threads"))
			}
		}
		line++
	}
	assert.Equal(t, 4, line) // Includes title
}
