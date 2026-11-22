package worker

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	analyzerv1 "github.com/marimirals/photo-analysis/gen/analyzer/v1"
)

// AnalyzerClient — обёртка над gRPC-клиентом сервиса анализа.
type AnalyzerClient struct {
	conn    *grpc.ClientConn
	client  analyzerv1.AnalyzerClient
	timeout time.Duration
}

func NewAnalyzerClient(addr string, timeout time.Duration) (*AnalyzerClient, error) {
	// Соединение устанавливается лениво, при первом вызове, и само переподключается.
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("analyzer client: %w", err)
	}
	return &AnalyzerClient{conn: conn, client: analyzerv1.NewAnalyzerClient(conn), timeout: timeout}, nil
}

func (c *AnalyzerClient) Close() error { return c.conn.Close() }

func (c *AnalyzerClient) Analyze(ctx context.Context, photoID, bucket, key string) (*analyzerv1.AnalyzeResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	return c.client.Analyze(ctx, &analyzerv1.AnalyzeRequest{PhotoId: photoID, Bucket: bucket, ObjectKey: key})
}

// retryable — стоит ли повторять вызов после такой ошибки.
// Коды описаны в proto/analyzer/v1/analyzer.proto.
func retryable(err error) bool {
	switch status.Code(err) {
	case codes.InvalidArgument, codes.NotFound:
		return false // файл не картинка или его нет — повтор не поможет
	default:
		return true // Unavailable, DeadlineExceeded, Internal и прочее — может пройти позже
	}
}
