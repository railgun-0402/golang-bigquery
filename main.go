package main

import (
	bq "cloud.google.com/go/bigquery"
	"context"
	"fmt"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"os"
)

func main() {
	ctx := context.Background()
	//projectID := os.Getenv("HANDSON_BIG_QUERY_ID")
	projectID := "handsonpractice-443012"

	key := os.Getenv("BIGQUERY_JSON")
	// 認証情報をJSONファイルから読み込む
	client, err := bq.NewClient(ctx, projectID, option.WithCredentialsFile(key))

	if err != nil {
		fmt.Println("Failed to create client:%v", err)
	}
	defer client.Close()

	query(ctx, client)
	copyTable(ctx, client)
}

func query(ctx context.Context, client *bq.Client) {
	q := "SELECT purchased_at, store, item_name, amount, member_id FROM `sample_ice_cream.ice_cream_sales`"

	// クエリ実行
	it, err := client.Query(q).Read(ctx)

	if err != nil {
		fmt.Println("Failed to Read Query:%v", err)
	}

	for {
		var values []bq.Value

		err := it.Next(&values)

		if err == iterator.Done {
			break
		}

		if err != nil {
			fmt.Println("Failed to Iterate Query:%v", err)
		}

		fmt.Println(values)
	}
}

func copyTable(ctx context.Context, client *bq.Client) {

	dataset := client.Dataset("{input_data_set}")

	// コピー元のテーブルから、新テーブルを作成
	copier := dataset.Table("{input_new_table}").CopierFrom(dataset.Table("{input_copy_table}"))
	copier.WriteDisposition = bq.WriteTruncate

	job, err := copier.Run(ctx)
	if err != nil {
		fmt.Println("Failed to Copy Job:%v", err)
	}

	// ジョブが完了まで待機
	status, err := job.Wait(ctx)
	if err != nil {
		fmt.Println("Failed to Copy Job:%v", err)
	}

	if err := status.Err(); err != nil {
		fmt.Println("Failed to copy job:%v", err)
	}

	fmt.Println("copy fin.")
}
