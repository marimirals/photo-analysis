// Package kafka — отправка сообщений в Kafka (библиотека franz-go).
package kafka

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/marimirals/photo-analysis/internal/store"
)

type Producer struct {
	client *kgo.Client
}

// NewProducer создаёт клиента. brokers — через запятую, например "localhost:9092".
// По умолчанию franz-go ждёт подтверждения от всех реплик и включает
// идемпотентную отправку, так что при повторах внутри клиента дублей нет.
func NewProducer(brokers string) (*Producer, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(strings.Split(brokers, ",")...),
	)
	if err != nil {
		return nil, fmt.Errorf("kafka client: %w", err)
	}
	return &Producer{client: client}, nil
}

func (p *Producer) Close() { p.client.Close() }

func (p *Producer) Ping(ctx context.Context) error { return p.client.Ping(ctx) }

// SendOutbox отправляет пачку событий и ждёт, пока Kafka подтвердит все.
// Ключ сообщения — user_id: фото одного пользователя попадают в одну партицию.
func (p *Producer) SendOutbox(ctx context.Context, batch []store.OutboxRecord) error {
	records := make([]*kgo.Record, len(batch))
	for i, r := range batch {
		records[i] = &kgo.Record{
			Topic: r.Topic,
			Key:   []byte(r.Key),
			Value: r.Payload,
		}
	}
	// Без таймаута franz-go будет повторять отправку бесконечно, если Kafka лежит,
	// а строки outbox всё это время останутся заблокированными в транзакции.
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	return p.client.ProduceSync(ctx, records...).FirstErr()
}
