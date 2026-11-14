// Package migrations встраивает SQL-миграции в бинарник,
// чтобы api-service мог накатить их сам при старте.
package migrations

import "embed"

//go:embed *.sql
var FS embed.FS
