package main

import (
	"bufio"
	"encoding/json"
	"log"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"
)

var userHostAttributesRe = regexp.MustCompile(`\b(User@Host: [\w\[\]]+ @ (?:)(\w+)?)|(Id:.+)`)
var attributesRe = regexp.MustCompile(`\b([\w_]+:\s+[^\s]+)\b`)

const (
	attributeTypeFloat = iota
	attributeTypeInt
	attributeTypeString
	attributeTypeBool
)

var attributeTypes = map[string]int{
	"Thread_id":             attributeTypeInt,
	"Schema":                attributeTypeString,
	"Last_errno":            attributeTypeInt,
	"Killed":                attributeTypeInt,
	"Query_time":            attributeTypeFloat,
	"Lock_time":             attributeTypeFloat,
	"Rows_sent":             attributeTypeInt,
	"Rows_examined":         attributeTypeInt,
	"Rows_affected":         attributeTypeInt,
	"Rows_read":             attributeTypeInt,
	"Bytes_sent":            attributeTypeInt,
	"Tmp_tables":            attributeTypeInt,
	"Tmp_disk_tables":       attributeTypeInt,
	"Tmp_table_sizes":       attributeTypeInt,
	"InnoDB_trx_id":         attributeTypeString,
	"QC_Hit":                attributeTypeBool,
	"Full_scan":             attributeTypeBool,
	"Full_join":             attributeTypeBool,
	"Tmp_table":             attributeTypeBool,
	"Tmp_table_on_disk":     attributeTypeBool,
	"Filesort":              attributeTypeBool,
	"Filesort_on_disk":      attributeTypeBool,
	"Merge_passes":          attributeTypeInt,
	"InnoDB_IO_r_ops":       attributeTypeInt,
	"InnoDB_IO_r_bytes":     attributeTypeInt,
	"InnoDB_IO_r_wait":      attributeTypeFloat,
	"InnoDB_rec_lock_wait":  attributeTypeFloat,
	"InnoDB_queue_wait":     attributeTypeFloat,
	"InnoDB_pages_distinct": attributeTypeInt,
}

type Parser struct {
	inHeader bool
	inQuery  bool
	lines    []string
}

func (p *Parser) ConsumeLine(line string) *LogEvent {
	if strings.HasPrefix(line, "#") {
		// Comment line
		if p.inQuery {
			// We're in a new section
			event := parseEntry(p.lines)
			p.lines = append(p.lines[:0], line)
			p.inQuery = false
			p.inHeader = true
			return &event
		}
		p.inHeader = true
		p.lines = append(p.lines, line)
		return nil
	}

	// Not a comment line
	if p.inHeader {
		p.inHeader = false
		p.inQuery = true
		p.lines = append(p.lines, line)
		return nil
	}
	if p.inQuery {
		// Keep consuming query lines
		p.lines = append(p.lines, line)
	}

	return nil
}

func (p *Parser) Flush() *LogEvent {
	if !p.inQuery {
		return nil
	}
	event := parseEntry(p.lines)
	p.lines = p.lines[:0]
	return &event
}

func parseEntry(lines []string) LogEvent {
	event := LogEvent{}
	var i int
	var line string
	for i, line = range lines {
		if line[0] != '#' {
			break
		}
		if strings.HasPrefix(line, "# User@Host") {
			matches := userHostAttributesRe.FindAllString(line, -1)
			for _, match := range matches {
				parts := strings.Split(match, ": ")
				switch parts[0] {
				case "User@Host":
					userHostParts := strings.Split(parts[1], "@")
					event["User"] = strings.TrimSpace(strings.Split(userHostParts[0], "[")[0])
					event["Host"] = strings.TrimSpace(strings.Split(userHostParts[1], "[")[0])
				}
			}
			continue
		}
		matches := attributesRe.FindAllString(line, -1)
		for _, match := range matches {
			parts := strings.Split(match, ": ")
			var attributeValue interface{}
			switch attributeTypes[parts[0]] {
			case attributeTypeString:
				attributeValue = parts[1]
			case attributeTypeBool:
				v, err := strconv.ParseBool(parts[1])
				if err == nil {
					attributeValue = v
				}
			case attributeTypeFloat:
				v, err := strconv.ParseFloat(parts[1], 64)
				if err == nil {
					attributeValue = v
				}
			case attributeTypeInt:
				v, err := strconv.ParseInt(parts[1], 10, 64)
				if err == nil {
					attributeValue = v
				}
			}

			if attributeValue == nil {
				continue
			}

			event[parts[0]] = attributeValue
		}
	}

	// See if we have lines to skip

	for ; i < len(lines); i++ {
		if strings.HasPrefix(lines[i], "use ") {
			db := strings.TrimRight(strings.Split(lines[i], " ")[1], ";\n")
			event["database"] = db
			continue
		}
		if strings.HasPrefix(lines[i], "SET ") {
			if strings.HasPrefix(lines[i], "SET timestamp=") {
				unixTimestampString := strings.TrimRight(strings.Split(lines[i], "=")[1], ";\n")
				i, err := strconv.ParseInt(unixTimestampString, 10, 64)
				if err == nil {
					event["Timestamp"] = time.Unix(i, 0)
				}
			}
			continue
		}
		break
	}

	queryLines := []string{}
	for ; i < len(lines); i++ {
		if strings.HasSuffix(lines[i], "started with:\n") {
			// Rolled over to a new log file
			break
		}
		queryLines = append(queryLines, lines[i])
	}

	event["Statement"] = strings.TrimSpace(strings.Join(queryLines, "\n"))
	return event
}

type LogEvent map[string]interface{}

func main() {
	p := &Parser{}

	reader := bufio.NewReader(os.Stdin)
	for line, err := reader.ReadString('\n'); err == nil; line, err = reader.ReadString('\n') {
		event := p.ConsumeLine(line)
		if event != nil {
			b, _ := json.Marshal(event)
			log.Printf("%s", b)
		}
	}
}
