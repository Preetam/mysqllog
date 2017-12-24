package mysqllog

import (
	"bufio"
	"encoding/json"
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

func TestParser(t *testing.T) {
	p := &Parser{}
	r := strings.NewReader(content)
	reader := bufio.NewReader(r)
	var parsedEvent *LogEvent
	for line, err := reader.ReadString('\n'); err == nil; line, err = reader.ReadString('\n') {
		event := p.ConsumeLine(line)
		if event != nil {
			parsedEvent = event
		}
	}
	if parsedEvent == nil {
		t.Fatal("expected to parse an event")
	}

	expectedEvent := &LogEvent{
		"User":          "rdsadmin",
		"Host":          "localhost",
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
