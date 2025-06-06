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
	createSumTableSQL = `
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
		Flags UInt32  CODEC(ZSTD(1)),
		Exemplars Nested (
			FilteredAttributes JSON,
			TimeUnix DateTime64(9),
			Value Float64,
			SpanId String,
			TraceId String
		) CODEC(ZSTD(1)),
		AggregationTemporality Int32 CODEC(ZSTD(1)),
		IsMonotonic Boolean CODEC(Delta, ZSTD(1)),
) ENGINE = %s
%s
PARTITION BY toDate(TimeUnix)
ORDER BY (ServiceName, MetricName, Attributes, toUnixTimestamp64Nano(TimeUnix))
SETTINGS index_granularity=8192, ttl_only_drop_parts = 1;
`
	// language=ClickHouse SQL
	insertSumTableSQL = `INSERT INTO %s (
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
    Exemplars.TraceId,
	AggregationTemporality,
	IsMonotonic) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`
)

type sumModel struct {
	metricName        string
	metricDescription string
	metricUnit        string
	metadata          *MetricsMetaData
	sum               pmetric.Sum
}

type sumMetrics struct {
	sumModel  []*sumModel
	insertSQL string
	count     int
}

func (s *sumMetrics) insert(ctx context.Context, db *sql.DB) error {
	if s.count == 0 {
		return nil
	}
	start := time.Now()
	err := doWithTx(ctx, db, func(tx *sql.Tx) error {
		statement, err := tx.PrepareContext(ctx, s.insertSQL)
		if err != nil {
			return err
		}

		defer func() {
			_ = statement.Close()
		}()

		for _, model := range s.sumModel {
			resAttr := AttributesToJSON(model.metadata.ResAttr)
			scopeAttr := AttributesToJSON(model.metadata.ScopeInstr.Attributes())
			serviceName := GetServiceName(model.metadata.ResAttr)

			for i := range model.sum.DataPoints().Len() {
				dp := model.sum.DataPoints().At(i)
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
					int32(model.sum.AggregationTemporality()),
					model.sum.IsMonotonic(),
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
		logger.Debug("insert sum metrics fail", zap.Duration("cost", duration))
		return fmt.Errorf("insert sum metrics fail:%w", err)
	}

	// TODO latency metrics
	logger.Debug("insert sum metrics", zap.Int("records", s.count),
		zap.Duration("cost", duration))
	return nil
}

func (s *sumMetrics) Add(resAttr pcommon.Map, resURL string, scopeInstr pcommon.InstrumentationScope, scopeURL string, metrics any, name string, description string, unit string) error {
	sum, ok := metrics.(pmetric.Sum)
	if !ok {
		return errors.New("metrics param is not type of Sum")
	}
	s.count += sum.DataPoints().Len()
	s.sumModel = append(s.sumModel, &sumModel{
		metricName:        name,
		metricDescription: description,
		metricUnit:        unit,
		metadata: &MetricsMetaData{
			ResAttr:    resAttr,
			ResURL:     resURL,
			ScopeURL:   scopeURL,
			ScopeInstr: scopeInstr,
		},
		sum: sum,
	})
	return nil
}
