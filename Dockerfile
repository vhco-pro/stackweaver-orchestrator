FROM golang:1.27.3-alpine@sha256:d200d2dd76b03a99b52651fd1325aff7dcd35422c35c01bac56571b0507f3deb AS builder
ENV GOPRIVATE=github.com/michielvha/stackweaver

ARG TARGETARCH
ARG TARGETOS=linux

WORKDIR /build
RUN apk add --no-cache git

COPY backend/go.mod backend/go.sum ./
RUN --mount=type=secret,id=netrc,target=/root/.netrc go mod download

COPY backend/ .

RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -o stackweaver-orchestrator ./cmd/orchestrator

# Runtime stage — distroless:nonroot eliminates all OS-level CVEs
# Includes ca-certificates and tzdata, runs as nonroot (UID 65534)
FROM gcr.io/distroless/static:nonroot@sha256:e3f945647ffb95b5839c07038d64f9811adf17308b9121d8a2b87b6a22a80a39

COPY --from=builder /build/stackweaver-orchestrator /stackweaver-orchestrator

LABEL org.opencontainers.image.source="https://github.com/vhco-pro/stackweaver-orchestrator"
LABEL org.opencontainers.image.licenses="BUSL-1.1"
LABEL org.opencontainers.image.description="Stackweaver Orchestrator — Job scheduler for the Stackweaver DevOps platform"

ENTRYPOINT ["/stackweaver-orchestrator"]
