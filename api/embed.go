// Package apispec встраивает описание API (openapi.yaml) в бинарник api-service,
// чтобы его можно было отдать по адресу /openapi.yaml и показать в Swagger UI.
package apispec

import _ "embed"

//go:embed openapi.yaml
var OpenAPI []byte
