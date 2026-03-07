package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/jackc/pgx/v5/pgxpool"

	"helius/internal/aggregator"
	"helius/internal/collector"
	"helius/internal/config"
	"helius/internal/db"
)

func main() {
	if len(os.Args) < 2 {
		log.Fatalf("usage: helius <serve|aggregate>")
	}

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

	switch os.Args[1] {
	case "serve":
		cmdServe(ctx, cfg, pool)
	case "aggregate":
		if err := aggregator.Run(ctx, pool); err != nil {
			log.Fatalf("aggregate: %v", err)
		}
	default:
		log.Fatalf("unknown command %q — use serve or aggregate", os.Args[1])
	}
}

func cmdServe(ctx context.Context, cfg *config.Config, pool *pgxpool.Pool) {
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
		SetAutoReconnect(true).
		SetOnConnectHandler(func(c mqtt.Client) {
			for _, h := range cfg.Handlers {
				handler := collector.MakeHandler(pool, h)
				if tok := c.Subscribe(h.Topic, 1, handler); tok.Wait() && tok.Error() != nil {
					log.Printf("subscribe %s: %v", h.Topic, tok.Error())
					continue
				}
				log.Printf("subscribed: %s → %s", h.Topic, h.Table)
			}
		})

	client := mqtt.NewClient(opts)
	if tok := client.Connect(); tok.Wait() && tok.Error() != nil {
		log.Fatalf("mqtt connect: %v", tok.Error())
	}
	defer client.Disconnect(250)

	log.Println("helius running — waiting for messages")

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig

	log.Println("shutting down")
}
