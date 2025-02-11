package main

import (
	"github.com/darwinOrg/smss-client"
	"log"
	"time"
)

func main() {
	create()
	//getMqList()
	//delete()
}

func create() {
	pc, err := client.NewPubClient("localhost", 12301, time.Second*5, 3)
	if err != nil {
		log.Printf("%v\n", err)
		return
	}
	defer pc.Close()

	//mqName := "audience-audience-audience-audience-audience-audience-audience-audience-audience-audience-audience-audience-123abcdefg-00900000008"
	mqName := "order"

	//expireAt := time.Now().Add(time.Minute * 2).UnixMilli()
	err = pc.CreateTopic(mqName, 0, "tid-2209991")

	log.Println(err)
}

func delete() {
	pc, err := client.NewPubClient("localhost", 12301, time.Second*500, 3)
	if err != nil {
		log.Printf("%v\n", err)
		return
	}
	defer pc.Close()

	err = pc.DeleteTopic("temp_mq", "tid-9999del33")

	log.Println(err)
}

func getMqList() {
	pc, err := client.NewPubClient("localhost", 12302, time.Second*5, 3)
	if err != nil {
		log.Printf("%v\n", err)
		return
	}
	defer pc.Close()

	var j string
	j, err = pc.GetTopicList("tid-99yymm009")

	log.Println(j, err)
}
