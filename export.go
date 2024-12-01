package main

import (
	bq "cloud.google.com/go/bigquery"
	"context"
	"fmt"
)

func exportTable(ctx context.Context, client *bq.Client) {

	projectID := ""
	datasetID := ""
	table := ""

	gcsRef := bq.NewGCSReference("")

	// 指定したGCSのURIへデータをエクスポート
	extractor := client.DatasetInProject(projectID, datasetID).Table(table).ExtractorTo(gcsRef)
	extractor.DisableHeader = true

	job, err := extractor.Run(ctx)

	if err != nil {
		fmt.Println("Failed to extract job:%v", err)
	}

	// ジョブが終了するまで待つ
	status, err := job.Wait(ctx)
	if err != nil {
		fmt.Println("Failed to extract job:%v", err)
	}
	if err := status.Err(); err != nil {
		fmt.Println("Failed to extract job:%v", err)
	}

	fmt.Println("extract fin.")
}
