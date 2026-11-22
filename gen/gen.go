// Package gen — код, сгенерированный из proto-файлов.
//
// Пересоздать (нужны protoc, protoc-gen-go и protoc-gen-go-grpc):
//
//	go generate ./gen
package gen

//go:generate protoc -I ../proto --go_out=.. --go_opt=module=github.com/marimirals/photo-analysis --go-grpc_out=.. --go-grpc_opt=module=github.com/marimirals/photo-analysis ../proto/analyzer/v1/analyzer.proto
