package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"

	"github.com/Preetam/mysqllog"
)

func main() {
	p := &mysqllog.Parser{}

	reader := bufio.NewReader(os.Stdin)
	for line, err := reader.ReadString('\n'); err == nil; line, err = reader.ReadString('\n') {
		event := p.ConsumeLine(line)
		if event != nil {
			b, _ := json.Marshal(event)
			fmt.Printf("%s\n", b)
		}
	}
}
