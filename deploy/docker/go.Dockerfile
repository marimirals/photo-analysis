# Образ для api-service и worker (один файл на оба, сервис выбирается аргументом):
#   docker build -f deploy/docker/go.Dockerfile --build-arg SERVICE=api    -t photo-api:dev .
#   docker build -f deploy/docker/go.Dockerfile --build-arg SERVICE=worker -t photo-worker:dev .
#
# Две стадии: в первой собираем бинарник в образе с Go (~300 МБ),
# во второй кладём только бинарник в distroless (~2 МБ, без shell и пакетов).

FROM golang:1.26-alpine AS build
ARG SERVICE
WORKDIR /src
ENV CGO_ENABLED=0 \
    GOFLAGS=-mod=vendor

# Зависимости берутся из папки vendor/ (её делает up.ps1 командой `go mod vendor`
# из кэша Go на ноутбуке). Так сборке не нужна сеть: proxy.golang.org из
# контейнеров отвечает с таймаутами. vendor/ в .gitignore, в репозиторий не идёт.
COPY . .
RUN --mount=type=cache,target=/root/.cache/go-build \
    if [ -z "$SERVICE" ]; then echo "set --build-arg SERVICE=api|worker"; exit 1; fi && \
    if [ ! -f vendor/modules.txt ]; then echo "no vendor/: run 'go mod vendor' first"; exit 1; fi && \
    go build -trimpath -ldflags="-s -w" -o /out/app ./cmd/${SERVICE}

# Тег latest — именно он скачан на ноутбук (Docker Hub недоступен, gcr.io — да).
FROM gcr.io/distroless/static-debian12
COPY --from=build /out/app /app
# 65532 — непривилегированный пользователь nonroot из distroless.
USER 65532:65532
ENTRYPOINT ["/app"]
