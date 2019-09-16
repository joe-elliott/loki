package v1

import (
	"encoding/json"
	"fmt"
	"io"

	"github.com/grafana/loki/pkg/logql"

	"github.com/grafana/loki/pkg/logproto"
	"github.com/prometheus/prometheus/promql"
)

func WriteQueryResponseJSON(v promql.Value, w io.Writer) error {

	var err error
	var value ResultValue
	var resType ResultType

	switch v.Type() {
	case ResultTypeStream:
		resType = ResultTypeStream
		s, ok := v.(logql.Streams)

		if !ok {
			return fmt.Errorf("Unexpected type %T for streams", s)
		}

		value, err = NewStreams(s)

		if err != nil {
			return err
		}
	case ResultTypeVector:
		resType = ResultTypeVector
		vector, ok := v.(promql.Vector)

		if !ok {
			return fmt.Errorf("Unexpected type %T for vector", vector)
		}

		value = NewVector(vector)
	case ResultTypeMatrix:
		resType = ResultTypeMatrix
		m, ok := v.(promql.Matrix)

		if !ok {
			return fmt.Errorf("Unexpected type %T for vector", m)
		}

		value = NewMatrix(m)
	default:
		return fmt.Errorf("v1 endpoints do not support type %s", v.Type())
	}

	j := QueryResponse{
		Status: "success",
		Data: QueryResponseData{
			ResultType: resType,
			Result:     value,
		},
	}

	return json.NewEncoder(w).Encode(j)
}

//WriteLabelResponseJSON marshals a logproto.LabelResponse to JSON and then writes it to the provided io.Writer
//  Note that it simply directly marshals the value passed in.  This is because the label currently marshals
//  cleanly to the v1 http protocol.  If this ever changes, it will be caught by testing.
func WriteLabelResponseJSON(l logproto.LabelResponse, w io.Writer) error {
	return json.NewEncoder(w).Encode(l)
}
