// Copyright The OpenTelemetry Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package spanmetricsconnector // import "github.com/open-telemetry/opentelemetry-collector-contrib/connector/spanmetricsconnector"

import (
	"errors"
	"fmt"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

const (
	delta      = "AGGREGATION_TEMPORALITY_DELTA"
	cumulative = "AGGREGATION_TEMPORALITY_CUMULATIVE"
)

var defaultHistogramBucketsMs = []float64{
	2, 4, 6, 8, 10, 50, 100, 200, 400, 800, 1000, 1400, 2000, 5000, 10_000, 15_000,
}

// Dimension defines the dimension name and optional default value if the Dimension is missing from a span attribute.
type Dimension struct {
	Name    string  `mapstructure:"name"`
	Default *string `mapstructure:"default"`
}

// Config defines the configuration options for spanmetricsconnector.
type Config struct {
	// LatencyHistogramBuckets is the list of durations representing latency histogram buckets.
	// See defaultLatencyHistogramBucketsMs in connector.go for the default value.
	// Deprecated: use HistogramConfig to configure explicit histogram buckets
	LatencyHistogramBuckets []time.Duration `mapstructure:"latency_histogram_buckets"`

	// Dimensions defines the list of additional dimensions on top of the provided:
	// - service.name
	// - span.kind
	// - span.kind
	// - status.code
	// The dimensions will be fetched from the span's attributes. Examples of some conventionally used attributes:
	// https://github.com/open-telemetry/opentelemetry-collector/blob/main/model/semconv/opentelemetry.go.
	Dimensions []Dimension `mapstructure:"dimensions"`

	// DimensionsCacheSize defines the size of cache for storing Dimensions, which helps to avoid cache memory growing
	// indefinitely over the lifetime of the collector.
	// Optional. See defaultDimensionsCacheSize in connector.go for the default value.
	DimensionsCacheSize int `mapstructure:"dimensions_cache_size"`

	AggregationTemporality string `mapstructure:"aggregation_temporality"`

	Histogram HistogramConfig `mapstructure:"histogram"`

	// MetricsEmitInterval is the time period between when metrics are flushed or emitted to the configured MetricsExporter.
	MetricsFlushInterval time.Duration `mapstructure:"metrics_flush_interval"`

	// Namespace is the namespace of the metrics emitted by the connector.
	Namespace string `mapstructure:"namespace"`
}

type HistogramConfig struct {
	Unit        string                      `mapstructure:"unit"`
	Exponential *ExponentialHistogramConfig `mapstructure:"exponential"`
	Explicit    *ExplicitHistogramConfig    `mapstructure:"explicit"`
}

type ExponentialHistogramConfig struct {
	MaxSize int32 `mapstructure:"max_size"`
}

type ExplicitHistogramConfig struct {
	// Buckets is the list of durations representing explicit histogram buckets.
	Buckets []time.Duration `mapstructure:"buckets"`
}

var _ component.ConfigValidator = (*Config)(nil)

// Validate checks if the processor configuration is valid
func (c Config) Validate() error {
	err := validateDimensions(c.Dimensions)
	if err != nil {
		return err
	}

	if c.DimensionsCacheSize <= 0 {
		return fmt.Errorf(
			"invalid cache size: %v, the maximum number of the items in the cache should be positive",
			c.DimensionsCacheSize,
		)
	}

	if c.Histogram.Explicit != nil && c.Histogram.Exponential != nil {
		return errors.New("use either `explicit` or `exponential` buckets histogram")
	}

	unit := c.Histogram.Unit
	if unit != "s" && unit != "ms" {
		return fmt.Errorf("allowed units are 'ms' and 's', got: '%s'", c.Histogram.Unit)
	}
	return nil
}

// GetAggregationTemporality converts the string value given in the config into a AggregationTemporality.
// Returns cumulative, unless delta is correctly specified.
func (c Config) GetAggregationTemporality() pmetric.AggregationTemporality {
	if c.AggregationTemporality == delta {
		return pmetric.AggregationTemporalityDelta
	}
	return pmetric.AggregationTemporalityCumulative
}

// validateDimensions checks duplicates for reserved dimensions and additional dimensions.
func validateDimensions(dimensions []Dimension) error {
	labelNames := make(map[string]struct{})
	for _, key := range []string{serviceNameKey, spanKindKey, statusCodeKey, spanNameKey} {
		labelNames[key] = struct{}{}
	}

	for _, key := range dimensions {
		if _, ok := labelNames[key.Name]; ok {
			return fmt.Errorf("duplicate dimension name %s", key.Name)
		}
		labelNames[key.Name] = struct{}{}
	}

	return nil
}
