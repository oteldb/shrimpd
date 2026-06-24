package shrimpfilter

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestExtractLabelsEmpty(t *testing.T) {
	require.Equal(t, map[string]string{}, ExtractLabels(""))
	require.Equal(t, map[string]string{}, ExtractLabels("not json"))
}

func TestExtractLabelsResourceWins(t *testing.T) {
	data := `{
		"resource": {"service.name": "res-svc"},
		"attributes": {"service.name": "attr-svc", "level": "ATTR"}
	}`
	labels := ExtractLabels(data)
	require.Equal(t, "res-svc", labels["service_name"])
	// attributes should not override
	require.Equal(t, "res-svc", labels["service_name"])
}

func TestExtractLabelsDotToUnderscore(t *testing.T) {
	data := `{"resource":{"service.name":"svc","k8s.pod.name":"p1"}}`
	labels := ExtractLabels(data)
	require.Equal(t, "svc", labels["service_name"])
	require.Equal(t, "p1", labels["k8s_pod_name"])
}

func TestExtractLabelsTopLevel(t *testing.T) {
	data := `{
		"severity_text":"ERROR",
		"body":"hello",
		"trace_id":"t1",
		"span_id":"s1"
	}`
	labels := ExtractLabels(data)
	require.Equal(t, "ERROR", labels["level"])
	require.Equal(t, "hello", labels["body"])
	require.Equal(t, "t1", labels["trace_id"])
	require.Equal(t, "s1", labels["span_id"])
}

func TestExtractLabelsBodyNonString(t *testing.T) {
	data := `{"body":{"msg":"structured"}}`
	labels := ExtractLabels(data)
	require.Equal(t, `{"msg":"structured"}`, labels["body"])
}

func TestExtractLabelsNestedValues(t *testing.T) {
	data := `{
		"resource": {"arr":[1,2]},
		"attributes": {"obj":{"k":"v"}}
	}`
	labels := ExtractLabels(data)
	require.Equal(t, "[1,2]", labels["arr"])
	require.Equal(t, `{"k":"v"}`, labels["obj"])
}

func TestExtractLabelsAttributesDoNotOverrideResource(t *testing.T) {
	data := `{
		"resource":{"service.name":"r"},
		"attributes":{"service.name":"a", "foo":"bar"}
	}`
	labels := ExtractLabels(data)
	require.Equal(t, "r", labels["service_name"])
	require.Equal(t, "bar", labels["foo"])
}

func TestExtractLabelsMissingFields(t *testing.T) {
	data := `{"resource":{"service.name":"x"}}`
	labels := ExtractLabels(data)
	_, hasLevel := labels["level"]
	_, hasBody := labels["body"]
	require.False(t, hasLevel)
	require.False(t, hasBody)
	require.Equal(t, "x", labels["service_name"])
}

func TestExtractLabelsCh2ShrimpdFormat(t *testing.T) {
	data := `{
		"service_name": "kernel",
		"resource": "{\"service.name\":\"kernel\"}",
		"severity_text": "INFO",
		"body": "boot complete",
		"trace_id": "abc",
		"span_id": "def"
	}`
	labels := ExtractLabels(data)
	require.Equal(t, "kernel", labels["service_name"])
	require.Equal(t, "INFO", labels["level"])
	require.Equal(t, "boot complete", labels["body"])
}
