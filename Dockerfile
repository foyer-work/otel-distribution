FROM golang:1.24.3 AS build-stage
WORKDIR /build

COPY ./builder-config.yaml builder-config.yaml

RUN --mount=type=cache,target=/root/.cache/go-build GO111MODULE=on go install go.opentelemetry.io/collector/cmd/builder@v0.126.0
RUN --mount=type=cache,target=/root/.cache/go-build builder --config builder-config.yaml

FROM gcr.io/distroless/base:latest

ARG USER_UID=10001
USER ${USER_UID}

COPY ./collector-config.yaml /otelcol/collector-config.yaml
COPY --chmod=755 --from=build-stage /build/_build/foyer-otel /otelcol

ENTRYPOINT ["/otelcol/foyer-otel"]
CMD ["--config", "/otelcol/collector-config.yaml"]

EXPOSE 4318
