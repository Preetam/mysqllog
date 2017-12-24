package mysqllog

import (
	"bufio"
	"strings"
	"testing"
)

var content = `# Time: 2017-12-24T02:42:00.126000Z
# User@Host: rdsadmin[rdsadmin] @ localhost [127.0.0.1]  Id:     3
# Query_time: 0.020363  Lock_time: 0.018450 Rows_sent: 0  Rows_examined: 1
SET timestamp=1514083320;
use foo;
SELECT count(*) from mysql.rds_replication_status WHERE master_host IS NOT NULL and master_port IS NOT NULL GROUP BY action_timestamp,called_by_user,action,mysql_version,master_host,master_port ORDER BY action_timestamp LIMIT 1;
#
`

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
