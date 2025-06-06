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
	createHistogramTableSQL = `
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
		Count UInt64 CODEC(Delta, ZSTD(1)),
		Sum Float64 CODEC(ZSTD(1)),
		BucketCounts Array(UInt64) CODEC(ZSTD(1)),
		ExplicitBounds Array(Float64) CODEC(ZSTD(1)),
		Exemplars Nested (
			FilteredAttributes JSON,
			TimeUnix DateTime64(9),
			Value Float64,
			SpanId String,
			TraceId String
		) CODEC(ZSTD(1)),
		Flags UInt32 CODEC(ZSTD(1)),
		Min Float64 CODEC(ZSTD(1)),
		Max Float64 CODEC(ZSTD(1)),
		AggregationTemporality Int32 CODEC(ZSTD(1)),
) ENGINE = %s
%s
PARTITION BY toDate(TimeUnix)
ORDER BY (ServiceName, MetricName, Attributes, toUnixTimestamp64Nano(TimeUnix))
SETTINGS index_granularity=8192, ttl_only_drop_parts = 1;
`
	// language=ClickHouse SQL
	insertHistogramTableSQL = `INSERT INTO %s (
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
	Count,
	Sum,
	BucketCounts,
	ExplicitBounds,
  	Exemplars.FilteredAttributes,
	Exemplars.TimeUnix,
    Exemplars.Value,
    Exemplars.SpanId,
    Exemplars.TraceId,
	Flags,
	Min,
	Max,
	AggregationTemporality) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`
)

type histogramModel struct {
	metricName        string
	metricDescription string
	metricUnit        string
	metadata          *MetricsMetaData
	histogram         pmetric.Histogram
}

type histogramMetrics struct {
	histogramModel []*histogramModel
	insertSQL      string
	count          int
}

func (h *histogramMetrics) insert(ctx context.Context, db *sql.DB) error {
	if h.count == 0 {
		return nil
	}
	start := time.Now()
	err := doWithTx(ctx, db, func(tx *sql.Tx) error {
		statement, err := tx.PrepareContext(ctx, h.insertSQL)
		if err != nil {
			return err
		}

		defer func() {
			_ = statement.Close()
		}()

		for _, model := range h.histogramModel {
			resAttr := AttributesToJSON(model.metadata.ResAttr)
			scopeAttr := AttributesToJSON(model.metadata.ScopeInstr.Attributes())
			serviceName := GetServiceName(model.metadata.ResAttr)

			for i := range model.histogram.DataPoints().Len() {
				dp := model.histogram.DataPoints().At(i)
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
					dp.Count(),
					dp.Sum(),
					convertSliceToArraySet(dp.BucketCounts().AsRaw()),
					convertSliceToArraySet(dp.ExplicitBounds().AsRaw()),
					attrs,
					times,
					values,
					spanIDs,
					traceIDs,
					uint32(dp.Flags()),
					dp.Min(),
					dp.Max(),
					int32(model.histogram.AggregationTemporality()),
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
		logger.Debug("insert histogram metrics fail", zap.Duration("cost", duration))
		return fmt.Errorf("insert histogram metrics fail:%w", err)
	}

	// TODO latency metrics
	logger.Debug("insert histogram metrics", zap.Int("records", h.count),
		zap.Duration("cost", duration))
	return nil
}

func (h *histogramMetrics) Add(resAttr pcommon.Map, resURL string, scopeInstr pcommon.InstrumentationScope, scopeURL string, metrics any, name string, description string, unit string) error {
	histogram, ok := metrics.(pmetric.Histogram)
	if !ok {
		return errors.New("metrics param is not type of Histogram")
	}
	h.count += histogram.DataPoints().Len()
	h.histogramModel = append(h.histogramModel, &histogramModel{
		metricName:        name,
		metricDescription: description,
		metricUnit:        unit,
		metadata: &MetricsMetaData{
			ResAttr:    resAttr,
			ResURL:     resURL,
			ScopeURL:   scopeURL,
			ScopeInstr: scopeInstr,
		},
		histogram: histogram,
	})
	return nil
}
