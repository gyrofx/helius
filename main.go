package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	mqtt "github.com/eclipse/paho.mqtt.golang"

	"helius/internal/collector"
	"helius/internal/config"
	"helius/internal/db"
)

func main() {
	cfg, err := config.Load("config.yaml")
	if err != nil {
		log.Fatalf("load config: %v", err)
	}

	ctx := context.Background()

	pool, err := db.Connect(ctx, cfg.Database.DSN)
	if err != nil {
		log.Fatalf("connect db: %v", err)
	}
	defer pool.Close()

	if err := db.ApplySchema(ctx, pool); err != nil {
		log.Fatalf("apply schema: %v", err)
	}

	for _, s := range cfg.Sensors {
		_, err := pool.Exec(ctx,
			`INSERT INTO sensors (id, name, type) VALUES ($1, $2, $3)
			 ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name, type = EXCLUDED.type`,
			s.ID, s.Name, s.Type,
		)
		if err != nil {
			log.Fatalf("upsert sensor %s: %v", s.ID, err)
		}
	}

	opts := mqtt.NewClientOptions().
		AddBroker(cfg.MQTT.Broker).
		SetClientID(cfg.MQTT.ClientID).
		SetAutoReconnect(true)

	client := mqtt.NewClient(opts)
	if tok := client.Connect(); tok.Wait() && tok.Error() != nil {
		log.Fatalf("mqtt connect: %v", tok.Error())
	}
	defer client.Disconnect(250)

	for _, h := range cfg.Handlers {
		handler := collector.MakeHandler(pool, h)
		if tok := client.Subscribe(h.Topic, 1, handler); tok.Wait() && tok.Error() != nil {
			log.Fatalf("subscribe %s: %v", h.Topic, tok.Error())
		}
		log.Printf("subscribed: %s → %s", h.Topic, h.Table)
	}

	log.Println("helius running — waiting for messages")

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig

	log.Println("shutting down")
}
