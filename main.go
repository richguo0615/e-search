package main

import (
	"context"
	"fmt"
	"github.com/olivere/elastic"
	"reflect"
	"strconv"
)

var (
	subject       Subject
	indexName     = "subject"
	typeName      = "_doc"
	servers       = []string{"http://localhost:9200/"}
	elasticClient *elastic.Client
	ctx           = context.Background()
)

type Subject struct {
	ID     int      `json:"id"`
	Title  string   `json:"title"`
	Genres []string `json:"genres"`
}

const mapping = `
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
					"genres": {
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

	createIndex()
	writeData()
	//getData()
	//
	search(elasticClient, ctx, "剧情")
	//fmt.Println("****")
	search(elasticClient, ctx, "犯罪")
}

func createIndex() {
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
	subject = Subject{
		ID:     1,
		Title:  "肖恩克的救赎",
		Genres: []string{"犯罪", "剧情"},
	}

	// 写入
	doc, err := elasticClient.Index().
		Index(indexName).
		Type(typeName).
		Id(strconv.Itoa(subject.ID)).
		BodyJson(subject).
		Refresh("wait_for").
		Do(ctx)

	if err != nil {
		panic(err)
	}
	fmt.Printf("Indexed with id=%v, type=%s\n", doc.Id, doc.Type)
	subject = Subject{
		ID:     2,
		Title:  "千与千寻",
		Genres: []string{"剧情", "喜剧", "爱情", "战争"},
	}
	fmt.Println(string(subject.ID))
	doc, err = elasticClient.Index().
		Index(indexName).
		Type(typeName).
		Id(strconv.Itoa(subject.ID)).
		BodyJson(subject).
		Refresh("wait_for").
		Do(ctx)

	if err != nil {
		panic(err)
	}
}

func getData() {
	result, err := elasticClient.Get().
		Index(indexName).
		Type(typeName).
		Id(strconv.Itoa(subject.ID)).
		Do(ctx)
	if err != nil {
		panic(err)
	}
	if result.Found {
		fmt.Printf("Got document %v (version=%d, index=%s, type=%s, source=%v)\n",
			result.Id, result.Version, result.Index, result.Type, result.Source)
		//err := json.Unmarshal(result.Source, &subject)
		//if err != nil {
		//	panic(err)
		//}
		//fmt.Println(subject.ID, subject.Title, subject.Genres)
	}
}

func search(client *elastic.Client, ctx context.Context, genre string) {
	fmt.Printf("Search: %s", genre)
	// Term搜索
	termQuery := elastic.NewTermQuery("genres", genre)
	searchResult, err := client.Search().
		Index(indexName).
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
	fmt.Printf("Found %d subjects\n", total)
	if total > 0 {
		for _, item := range searchResult.Each(reflect.TypeOf(subject)) {
			if t, ok := item.(Subject); ok {
				fmt.Printf("Found: Subject(id=%d, title=%s)\n", t.ID, t.Title)
			}
		}

	} else {
		fmt.Println("Not found!")
	}
}
