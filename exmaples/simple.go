package main

import (
	"github.com/jhalickman/lumberjackserver"
	"log"
)

func main() {
	server := lumberjackserver.NewServer("./lumberjack.crt", "./lumberjack.key", "5043")
	server.EventHandler = func(event *lumberjackserver.FileEvent) {
		log.Println(event)
	}
	log.Panic(server.Serve())

}
