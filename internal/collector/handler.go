package collector

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"strings"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/jackc/pgx/v5/pgxpool"

	"helius/internal/config"
)

func MakeHandler(pool *pgxpool.Pool, hcfg config.HandlerConfig) mqtt.MessageHandler {
	return func(_ mqtt.Client, msg mqtt.Message) {
		log.Printf("topic %s: received payload: %s", hcfg.Topic, msg.Payload())
		cols := []string{"sensor_id"}
		params := []any{hcfg.SensorID}

		if hcfg.Channel != nil {
			cols = append(cols, "channel")
			params = append(params, *hcfg.Channel)
		}

		if hcfg.ValueColumn != "" {
			// Raw float payload (telegraf data_format = "value")
			val, err := strconv.ParseFloat(strings.TrimSpace(string(msg.Payload())), 64)
			if err != nil {
				log.Printf("topic %s: bad float: %v", hcfg.Topic, err)
				return
			}
			cols = append(cols, hcfg.ValueColumn)
			params = append(params, val)
		} else {
			// JSON payload
			var payload map[string]any
			if err := json.Unmarshal(msg.Payload(), &payload); err != nil {
				log.Printf("topic %s: bad JSON: %v", hcfg.Topic, err)
				return
			}
			for jsonKey, dbCol := range hcfg.Fields {
				val, ok := payload[jsonKey]
				if !ok {
					continue
				}
				cols = append(cols, dbCol)
				params = append(params, val)
			}
		}

		placeholders := make([]string, len(params))
		for i := range params {
			placeholders[i] = fmt.Sprintf("$%d", i+1)
		}

		query := fmt.Sprintf(
			"INSERT INTO %s (%s) VALUES (%s)",
			hcfg.Table,
			strings.Join(cols, ", "),
			strings.Join(placeholders, ", "),
		)

		if _, err := pool.Exec(context.Background(), query, params...); err != nil {
			log.Printf("topic %s: insert failed: %v", hcfg.Topic, err)
		}
	}
}
