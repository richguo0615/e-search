package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/olivere/elastic"
	"reflect"
	"strconv"
)

var (
	productIndex  = "product"
	typeName      = "_doc"
	servers       = []string{"http://localhost:9200/"}
	elasticClient *elastic.Client
	ctx           = context.Background()
)

type Product struct {
	ID          int      `json:"id"`
	Title       string   `json:"title"`
	Summary     string   `json:"summary"`
	Description string   `json:"description"`
	Tags        []string `json:"tags"`
}

const productMapping = `
	{
		"mappings": {
			"_doc": {
				"properties": {
					"id": {
						"type": "long"
					},
					"title": {
						"type": "text"
					},
					"summary": {
						"type": "text"
					},
					"description": {
						"type": "text"
					},
					"tags": {
						"type": "keyword"
					}
				}
			}
		}
	}`

func main() {
	client, err := elastic.NewClient(elastic.SetURL(servers...), elastic.SetHealthcheck(false), elastic.SetSniff(false))
	if err != nil {
		panic(err)
	}
	elasticClient = client

	createIndex(productIndex, productMapping)
	writeData()
	getData(productIndex, 1)

	search(elasticClient, ctx, productIndex, "金奈整體浴室")
}

func createIndex(indexName string, mapping string) {
	exists, err := elasticClient.IndexExists(indexName).Do(ctx)
	if err != nil {
		panic(err)
	}
	if !exists {
		_, err := elasticClient.CreateIndex(indexName).BodyString(mapping).Do(ctx)
		if err != nil {
			panic(err)
		}
	}
}

func writeData() {
	product := Product{
		ID:     1,
		Title:  "整體浴室-乾濕分離系列1521AT",
		Tags: []string{"金奈整體浴室", "整座浴室", "139000"},
	}

	// 写入
	doc, err := elasticClient.Index().
		Index(productIndex).
		Type(typeName).
		Id(strconv.Itoa(product.ID)).
		BodyJson(product).
		Refresh("wait_for").
		Do(ctx)

	if err != nil {
		panic(err)
	}
	fmt.Printf("Indexed with id=%v, type=%s\n", doc.Id, doc.Type)
}

func getData(index string, id int) {
	result, err := elasticClient.Get().
		Index(index).
		Type(typeName).
		Id(strconv.Itoa(id)).
		Do(ctx)
	if err != nil {
		panic(err)
	}
	if result.Found {
		fmt.Printf("Got document %v (version=%d, index=%s, type=%s)\n",
			result.Id, result.Version, result.Index, result.Type)

		var product *Product
		err := json.Unmarshal(*result.Source, &product)
		if err != nil {
			panic(err)
		}
		fmt.Println(product.ID, product.Title, product.Tags)
	}
}

func search(client *elastic.Client, ctx context.Context, index string, tags string) {
	fmt.Printf("Search: %s\n", tags)

	// Term搜索
	termQuery := elastic.NewTermQuery("tags", tags)

	searchResult, err := client.Search().
		Index(index).
		Type(typeName).
		Query(termQuery).
		Sort("id", true). // 按id升序排序
		From(0).Size(10). // 拿前10个结果
		Pretty(true).
		Do(ctx) // 执行

	if err != nil {
		panic(err)
	}
	total := searchResult.TotalHits()

	fmt.Printf("Found %d products\n", total)

	if total > 0 {
		var product *Product
		for _, item := range searchResult.Each(reflect.TypeOf(product)) {
			if t, ok := item.(Product); ok {
				fmt.Printf("Found: Product(id=%d, title=%s)\n", t.ID, t.Title)
			}
		}

	} else {
		fmt.Println("Not found!")
	}
}
