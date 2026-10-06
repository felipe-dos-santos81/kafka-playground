FROM golang:1.27.1-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /producer .

FROM scratch
COPY --from=build /producer /producer
USER 65534
EXPOSE 8081
ENTRYPOINT ["/producer"]
