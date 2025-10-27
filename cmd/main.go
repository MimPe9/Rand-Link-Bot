package main

import (
	"context"
	"fmt"
	"log"

	tgClient "bot/clients/telegram"
	"bot/config"
	event_consumer "bot/consumer/event-consumer"
	"bot/events/telegram"
	"bot/pkg/systems"

	storage "bot/storage/postgres"
)

const (
	tgBotHost = "api.telegram.org"
	batchSize = 100
)

func main() {
	token := systems.BotToken()
	s := setupStorage()
	defer s.Close()

	eventsProcessor := telegram.New(
		tgClient.New(tgBotHost, token),
		s,
	)

	log.Print("service started")

	consumer := event_consumer.New(eventsProcessor, eventsProcessor, batchSize)
	if err := consumer.Start(); err != nil {
		log.Fatal("service is stopped", err)
	}
}

func setupStorage() *storage.PostgresStorage {
	connStr := fmt.Sprintf("user=%s dbname=%s password=%s host=%s port=%s sslmode=%s",
		config.GetDBUser(), config.GetDBName(), config.GetDBPass(),
		config.GetDBHost(), config.GetDBPort(), config.GetDBSSLMode())

	log.Printf("Connecting to database: %s@%s:%s", config.GetDBUser(), config.GetDBHost(), config.GetDBPort())

	s, err := storage.NewPostgresStorage(connStr)
	if err != nil {
		log.Fatal("can't connect to storage: ", err)
	}

	if err := s.Init(context.TODO()); err != nil {
		log.Fatal("can't init storage: ", err)
	}

	return s
}

//can't handle event: can't process message: can't do command: save page: can't check if page exists: pq: syntax error at or near
