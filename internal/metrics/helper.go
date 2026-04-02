package metrics

import (
	"reflect"

	"go.opentelemetry.io/otel/metric"
)

func NewMetric[OM any](meter metric.Meter, path, unit, description string) OM {
	u := metric.WithUnit(unit)
	d := metric.WithDescription(description)
	switch reflect.TypeFor[OM]().Name() {
	case "Int64Gauge":
		om, err := meter.Int64Gauge(path, u, d)
		if err != nil {
			panic(err)
		}
		return om.(OM)

	case "Int64Counter":
		om, err := meter.Int64Counter(path, u, d)
		if err != nil {
			panic(err)
		}
		return om.(OM)

	case "Float64Histogram":
		om, err := meter.Float64Histogram(path, u, d)
		if err != nil {
			panic(err)
		}
		return om.(OM)

	case "Float64Counter":
		om, err := meter.Float64Counter(path, u, d)
		if err != nil {
			panic(err)
		}
		return om.(OM)
	}
	panic("unsupported metric type")
}
