package main

import (
	"flag"
	"fmt"
	"time"

	"main.go/repository/cloudwatch"
)

func main() {
	flag.Parse()
	args := flag.Args()
	targetTargetLogGroup := args[0]

	now := time.Now()
	from := now.Add(-2 * time.Hour)
	to := now.Add(-1 * time.Hour)

	insightRepo, err := cloudwatch.NewInsightRepoImpl()
	if err != nil {
		panic(err)
	}

	result, err := insightRepo.FindLogByRange(targetTargetLogGroup, from.Unix(), to.Unix())
	if err != nil {
		panic(err)
	}

	for _, item := range result {
		fmt.Printf("ID: %s, Type: %s, Message: %s", item.RequestId, item.Type, item.Message)
	}
}
