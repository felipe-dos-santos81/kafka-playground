# UI: Vite build → /ui/dist
FROM node:24.21.0-alpine AS ui
WORKDIR /ui
COPY ui/package.json ui/package-lock.json ./
RUN --mount=type=cache,target=/root/.npm npm ci
COPY ui/ ./
RUN npm run build

# Go: static binary with the UI embedded
FROM golang:1.27.1-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY *.go ./
COPY --from=ui /ui/dist ./ui/dist
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /studio .

FROM scratch
COPY --from=build /studio /studio
USER 65534
EXPOSE 8082
ENTRYPOINT ["/studio"]
