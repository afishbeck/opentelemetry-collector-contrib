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

package filereceiver

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"
)

func TestFileReader_Readline(t *testing.T) {
	tc := testConsumer{}
	f, err := os.Open(filepath.Join("testdata", "metrics.json"))
	require.NoError(t, err)
	fr := newFileReader(zap.NewNop(), &tc, f)
	err = fr.readLine(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 1, len(tc.consumed))
	metrics := tc.consumed[0]
	assert.Equal(t, 26, metrics.MetricCount())
	byName := metricsByName(metrics)
	rcpMetric := byName["redis.commands.processed"]
	v := rcpMetric.Sum().DataPoints().At(0).IntValue()
	const testdataValue = 2076
	assert.EqualValues(t, testdataValue, v)
}

func TestFileReader_Cancellation(t *testing.T) {
	fr := fileReader{
		consumer:     consumertest.NewNop(),
		logger:       zap.NewNop(),
		stringReader: blockingStringReader{},
	}
	ctx, cancel := context.WithCancel(context.Background())
	go fr.readAll(ctx)
	cancel()
}

type blockingStringReader struct {
}

func (sr blockingStringReader) ReadString(byte) (string, error) {
	select {}
}

func metricsByName(pm pmetric.Metrics) map[string]pmetric.Metric {
	out := map[string]pmetric.Metric{}
	for i := 0; i < pm.ResourceMetrics().Len(); i++ {
		sms := pm.ResourceMetrics().At(i).ScopeMetrics()
		for j := 0; j < sms.Len(); j++ {
			ms := sms.At(j).Metrics()
			for k := 0; k < ms.Len(); k++ {
				metric := ms.At(k)
				out[metric.Name()] = metric
			}
		}
	}
	return out
}
