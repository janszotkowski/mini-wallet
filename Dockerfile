FROM golang:1.26 AS build
WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 go build -o /out/api ./cmd/api
RUN CGO_ENABLED=0 go build -o /out/publisher ./cmd/publisher
RUN CGO_ENABLED=0 go build -o /out/grpc ./cmd/grpc
RUN CGO_ENABLED=0 GOBIN=/out go install github.com/pressly/goose/v3/cmd/goose@v3.29.0

FROM gcr.io/distroless/static-debian12
COPY --from=build /out/ /usr/local/bin/
COPY migrations /migrations
