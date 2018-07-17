package mysqllog

import (
	"bufio"
	"bytes"
	"encoding/json"
	"io/ioutil"
	"reflect"
	"strings"
	"testing"
	"time"
)

func jsonPrint(v interface{}) string {
	b, _ := json.Marshal(v)
	return string(b)
}

var content = `# Time: 2017-12-24T02:42:00.126000Z
# User@Host: rdsadmin[rdsadmin] @ localhost [127.0.0.1]  Id:     3
# Query_time: 0.020363  Lock_time: 0.018450 Rows_sent: 0  Rows_examined: 1
SET timestamp=1514083320;
use foo;
SELECT count(*) from mysql.rds_replication_status WHERE master_host IS NOT NULL and master_port IS NOT NULL GROUP BY action_timestamp,called_by_user,action,mysql_version,master_host,master_port ORDER BY action_timestamp LIMIT 1;
#
`

func TestParseSingleEvent(t *testing.T) {
	p := &Parser{}
	r := strings.NewReader(content)
	reader := bufio.NewReader(r)
	var parsedEvent LogEvent
	for line, err := reader.ReadString('\n'); err == nil; line, err = reader.ReadString('\n') {
		event := p.ConsumeLine(line)
		if event != nil {
			parsedEvent = event
		}
	}
	if parsedEvent == nil {
		t.Fatal("expected to parse an event")
	}

	expectedEvent := LogEvent{
		"User":          "rdsadmin",
		"Host":          "localhost",
		"IP":            "127.0.0.1",
		"Database":      "foo",
		"Query_time":    float64(0.020363),
		"Lock_time":     float64(0.018450),
		"Rows_sent":     int64(0),
		"Rows_examined": int64(1),
		"Timestamp":     time.Unix(1514083320, 0).UTC(),
		"Statement":     "SELECT count(*) from mysql.rds_replication_status WHERE master_host IS NOT NULL and master_port IS NOT NULL GROUP BY action_timestamp,called_by_user,action,mysql_version,master_host,master_port ORDER BY action_timestamp LIMIT 1;",
	}

	if !reflect.DeepEqual(parsedEvent, expectedEvent) {
		t.Errorf("expected event\n%v\n, got\n%v", jsonPrint(expectedEvent), jsonPrint(parsedEvent))
	}
}

func TestParseUserHostLine(t *testing.T) {
	type TestCase struct {
		Line     string
		Expected map[string]string
	}

	cases := []TestCase{
		{
			Line: "# User@Host: rdsadmin[rdsadmin] @ localhost [127.0.0.1]  Id:     3",
			Expected: map[string]string{
				"User": "rdsadmin",
				"Host": "localhost",
				"IP":   "127.0.0.1",
			},
		},
		{
			Line: "# User@Host: rdsadmin[rdsadmin] @ localhost []  Id:     3",
			Expected: map[string]string{
				"User": "rdsadmin",
				"Host": "localhost",
			},
		},
		{
			Line: "# User@Host: rdsadmin[rdsadmin] @  [127.0.0.1]  Id:     3",
			Expected: map[string]string{
				"User": "rdsadmin",
				"Host": "127.0.0.1",
				"IP":   "127.0.0.1",
			},
		},
	}

	for _, c := range cases {
		result := parseUserHostLine(c.Line)
		if !reflect.DeepEqual(result, c.Expected) {
			t.Errorf("expected %v, got %v", c.Expected, result)
		}
	}
}

func TestParseRDSFile(t *testing.T) {
	p := &Parser{}
	b, err := ioutil.ReadFile("./_test/rds.txt")
	if err != nil {
		t.Fatal(err)
	}
	reader := bufio.NewReader(bytes.NewReader(b))
	parsedEvents := []LogEvent{}
	for line, err := reader.ReadString('\n'); err == nil; line, err = reader.ReadString('\n') {
		event := p.ConsumeLine(line)
		if event != nil {
			parsedEvents = append(parsedEvents, event)
		}
	}
	lastEvent := p.Flush()
	if lastEvent != nil {
		parsedEvents = append(parsedEvents, lastEvent)
	}

	numExpectedEvents := 231
	if len(parsedEvents) != numExpectedEvents {
		t.Errorf("expected %d events but got %d", numExpectedEvents, len(parsedEvents))
	}

	expectedSelects := 191
	seenSelects := 0
	for _, e := range parsedEvents {
		if strings.HasPrefix(strings.ToLower(e["Statement"].(string)), "select ") {
			seenSelects++
		}
	}

	if expectedSelects != seenSelects {
		t.Errorf("expected %d selects but got %d", expectedSelects, seenSelects)
	}
}

func BenchmarkParse(b *testing.B) {
	for i := 0; i < b.N; i++ {
		p := &Parser{}
		r := strings.NewReader(content)
		reader := bufio.NewReader(r)
		for line, err := reader.ReadString('\n'); err == nil; line, err = reader.ReadString('\n') {
			event := p.ConsumeLine(line)
			if event != nil {
			}
		}
	}
}
