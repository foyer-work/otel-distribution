// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package internal // import "github.com/foyer-work/otel-distribution/exporter/clickhouse/internal"

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"
)

const (
	// language=ClickHouse SQL
	createGaugeTableSQL = `
CREATE TABLE IF NOT EXISTS %s %s (
	ResourceAttributes JSON,
	ResourceSchemaUrl String CODEC(ZSTD(1)),
	ScopeName String CODEC(ZSTD(1)),
	ScopeVersion String CODEC(ZSTD(1)),
	ScopeAttributes JSON,
	ScopeDroppedAttrCount UInt32 CODEC(ZSTD(1)),
	ScopeSchemaUrl String CODEC(ZSTD(1)),
	ServiceName LowCardinality(String) CODEC(ZSTD(1)),
	MetricName String CODEC(ZSTD(1)),
	MetricDescription String CODEC(ZSTD(1)),
	MetricUnit String CODEC(ZSTD(1)),
	Attributes JSON,
	StartTimeUnix DateTime64(9) CODEC(Delta, ZSTD(1)),
	TimeUnix DateTime64(9) CODEC(Delta, ZSTD(1)),
	Value Float64 CODEC(ZSTD(1)),
	Flags UInt32 CODEC(ZSTD(1)),
	Exemplars Nested (
		FilteredAttributes JSON,
		TimeUnix DateTime64(9),
		Value Float64,
		SpanId String,
		TraceId String
	) CODEC(ZSTD(1)),
) ENGINE = %s
%s
PARTITION BY toDate(TimeUnix)
ORDER BY (ServiceName, MetricName, Attributes, toUnixTimestamp64Nano(TimeUnix))
SETTINGS index_granularity=8192, ttl_only_drop_parts = 1;
`
	// language=ClickHouse SQL
	insertGaugeTableSQL = `INSERT INTO %s (
    ResourceAttributes,
    ResourceSchemaUrl,
    ScopeName,
    ScopeVersion,
    ScopeAttributes,
    ScopeDroppedAttrCount,
    ScopeSchemaUrl,
    ServiceName,
    MetricName,
    MetricDescription,
    MetricUnit,
    Attributes,
    StartTimeUnix,
    TimeUnix,
    Value,
    Flags,
    Exemplars.FilteredAttributes,
		Exemplars.TimeUnix,
    Exemplars.Value,
    Exemplars.SpanId,
    Exemplars.TraceId) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`
)

type gaugeModel struct {
	metricName        string
	metricDescription string
	metricUnit        string
	metadata          *MetricsMetaData
	gauge             pmetric.Gauge
}

type gaugeMetrics struct {
	gaugeModels []*gaugeModel
	insertSQL   string
	count       int
}

func (g *gaugeMetrics) insert(ctx context.Context, db *sql.DB) error {
	if g.count == 0 {
		return nil
	}
	start := time.Now()
	err := doWithTx(ctx, db, func(tx *sql.Tx) error {
		statement, err := tx.PrepareContext(ctx, g.insertSQL)
		if err != nil {
			return err
		}

		defer func() {
			_ = statement.Close()
		}()

		for _, model := range g.gaugeModels {
			resAttr := AttributesToJSON(model.metadata.ResAttr)
			scopeAttr := AttributesToJSON(model.metadata.ScopeInstr.Attributes())
			serviceName := GetServiceName(model.metadata.ResAttr)

			for i := range model.gauge.DataPoints().Len() {
				dp := model.gauge.DataPoints().At(i)
				attrs, times, values, traceIDs, spanIDs := convertExemplars(dp.Exemplars())
				_, err = statement.ExecContext(ctx,
					resAttr,
					model.metadata.ResURL,
					model.metadata.ScopeInstr.Name(),
					model.metadata.ScopeInstr.Version(),
					scopeAttr,
					model.metadata.ScopeInstr.DroppedAttributesCount(),
					model.metadata.ScopeURL,
					serviceName,
					model.metricName,
					model.metricDescription,
					model.metricUnit,
					AttributesToJSON(dp.Attributes()),
					dp.StartTimestamp().AsTime(),
					dp.Timestamp().AsTime(),
					getValue(dp.IntValue(), dp.DoubleValue(), dp.ValueType()),
					uint32(dp.Flags()),
					attrs,
					times,
					values,
					spanIDs,
					traceIDs,
				)
				if err != nil {
					return fmt.Errorf("ExecContext:%w", err)
				}
			}
		}
		return err
	})
	duration := time.Since(start)
	if err != nil {
		logger.Debug("insert gauge metrics fail", zap.Duration("cost", duration))
		return fmt.Errorf("insert gauge metrics fail:%w", err)
	}
	return nil
}

func (g *gaugeMetrics) Add(resAttr pcommon.Map, resURL string, scopeInstr pcommon.InstrumentationScope, scopeURL string, metrics any, name string, description string, unit string) error {
	gauge, ok := metrics.(pmetric.Gauge)
	if !ok {
		return errors.New("metrics param is not type of Gauge")
	}
	g.count += gauge.DataPoints().Len()
	g.gaugeModels = append(g.gaugeModels, &gaugeModel{
		metricName:        name,
		metricDescription: description,
		metricUnit:        unit,
		metadata: &MetricsMetaData{
			ResAttr:    resAttr,
			ResURL:     resURL,
			ScopeURL:   scopeURL,
			ScopeInstr: scopeInstr,
		},
		gauge: gauge,
	})
	return nil
}
