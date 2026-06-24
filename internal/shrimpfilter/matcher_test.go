package shrimpfilter

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCompileMatcher(t *testing.T) {
	m, err := CompileMatcher([]LineFilter{
		{Op: OpLineRe, Value: "panic.*"},
	}, []LabelFilter{
		{Label: "service_name", Op: OpLabelEq, Value: "svc-a"},
		{Label: "level", Op: OpLabelRe, Value: "ER.*"},
	})
	require.NoError(t, err)
	require.Len(t, m.Line, 1)
	require.Len(t, m.Labels, 2)
	require.NotNil(t, m.Line[0].Re)
	require.NotNil(t, m.Labels[1].Re)
	require.Equal(t, "^(?:ER.*)$", m.Labels[1].Re.String())
}

func TestCompileMatcherBadRegex(t *testing.T) {
	_, err := CompileMatcher(nil, []LabelFilter{{Label: "x", Op: OpLabelRe, Value: "[unclosed"}})
	require.Error(t, err)
	_, err = CompileMatcher([]LineFilter{{Op: OpLineRe, Value: "[bad"}}, nil)
	require.Error(t, err)
}

func TestMatcherEmpty(t *testing.T) {
	var m Matcher
	require.True(t, m.Empty())
	m, _ = CompileMatcher(nil, nil)
	require.True(t, m.Empty())
	m, _ = CompileMatcher([]LineFilter{{Op: OpLineEq, Value: "x"}}, nil)
	require.False(t, m.Empty())
}

func TestMatchLineOps(t *testing.T) {
	data := `error panic in main`
	cases := []struct {
		f  LineFilter
		ok bool
	}{
		{LineFilter{Op: OpLineEq, Value: "panic"}, true},
		{LineFilter{Op: OpLineEq, Value: "debug"}, false},
		{LineFilter{Op: OpLineNotEq, Value: "panic"}, false},
		{LineFilter{Op: OpLineNotEq, Value: "debug"}, true},
	}
	for _, c := range cases {
		m := Matcher{Line: []LineFilter{c.f}}
		require.Equal(t, c.ok, m.MatchLine(data), "op=%d v=%q", c.f.Op, c.f.Value)
		require.Equal(t, c.ok, m.MatchLineBytes([]byte(data)), "op=%d v=%q", c.f.Op, c.f.Value)
	}
	// Regex cases must go through CompileMatcher to populate Re.
	m, _ := CompileMatcher([]LineFilter{{Op: OpLineRe, Value: "panic.*"}}, nil)
	require.True(t, m.MatchLine(data))
	require.True(t, m.MatchLineBytes([]byte(data)))
	m, _ = CompileMatcher([]LineFilter{{Op: OpLineRe, Value: "debug"}}, nil)
	require.False(t, m.MatchLine(data))
	require.False(t, m.MatchLineBytes([]byte(data)))
	m, _ = CompileMatcher([]LineFilter{{Op: OpLineNotRe, Value: "panic.*"}}, nil)
	require.False(t, m.MatchLine(data))
	require.False(t, m.MatchLineBytes([]byte(data)))
	m, _ = CompileMatcher([]LineFilter{{Op: OpLineNotRe, Value: "debug"}}, nil)
	require.True(t, m.MatchLine(data))
	require.True(t, m.MatchLineBytes([]byte(data)))
}

func TestMatchLineMultiAND(t *testing.T) {
	m, _ := CompileMatcher([]LineFilter{
		{Op: OpLineEq, Value: "error"},
		{Op: OpLineNotEq, Value: "debug"},
		{Op: OpLineRe, Value: "main.*"},
	}, nil)

	require.True(t, m.MatchLine("error panic in main"))
	require.False(t, m.MatchLine("error panic in other"))
	require.False(t, m.MatchLine("debug in main"))

	require.True(t, m.MatchLineBytes([]byte("error panic in main")))
	require.False(t, m.MatchLineBytes([]byte("error panic in other")))
	require.False(t, m.MatchLineBytes([]byte("debug in main")))
}

func TestMatchLabelsOps(t *testing.T) {
	labels := map[string]string{"service_name": "svc-a", "level": "ERROR"}
	cases := []struct {
		f  LabelFilter
		ok bool
	}{
		{LabelFilter{Label: "service_name", Op: OpLabelEq, Value: "svc-a"}, true},
		{LabelFilter{Label: "service_name", Op: OpLabelEq, Value: "svc-b"}, false},
		{LabelFilter{Label: "service_name", Op: OpLabelNotEq, Value: "svc-b"}, true},
		{LabelFilter{Label: "service_name", Op: OpLabelNotEq, Value: "svc-a"}, false},
		{LabelFilter{Label: "missing", Op: OpLabelEq, Value: ""}, true},
		{LabelFilter{Label: "missing", Op: OpLabelNotEq, Value: ""}, false},
	}
	for _, c := range cases {
		m := Matcher{Labels: []LabelFilter{c.f}}
		require.Equal(t, c.ok, m.MatchLabels(labels), "label=%s op=%d v=%q", c.f.Label, c.f.Op, c.f.Value)
	}
	// Regex cases via CompileMatcher to ensure anchoring.
	m, _ := CompileMatcher(nil, []LabelFilter{{Label: "level", Op: OpLabelRe, Value: "ER.*"}})
	require.True(t, m.MatchLabels(labels))
	m, _ = CompileMatcher(nil, []LabelFilter{{Label: "level", Op: OpLabelRe, Value: "IN.*"}})
	require.False(t, m.MatchLabels(labels))
	m, _ = CompileMatcher(nil, []LabelFilter{{Label: "level", Op: OpLabelNotRe, Value: "IN.*"}})
	require.True(t, m.MatchLabels(labels))
	m, _ = CompileMatcher(nil, []LabelFilter{{Label: "level", Op: OpLabelNotRe, Value: "ER.*"}})
	require.False(t, m.MatchLabels(labels))
}

func TestMatchLabelsRegexAnchoring(t *testing.T) {
	labels := map[string]string{"svc": "foo-bar"}
	// re is anchored ^(?:...)$
	m, _ := CompileMatcher(nil, []LabelFilter{{Label: "svc", Op: OpLabelRe, Value: "foo.*"}})
	require.True(t, m.MatchLabels(labels))
	m, _ = CompileMatcher(nil, []LabelFilter{{Label: "svc", Op: OpLabelRe, Value: ".*bar"}})
	require.True(t, m.MatchLabels(labels))
	m, _ = CompileMatcher(nil, []LabelFilter{{Label: "svc", Op: OpLabelRe, Value: "foo"}})
	require.False(t, m.MatchLabels(labels)) // not full match
}

func TestMatchLabelsNonJSON(t *testing.T) {
	// Non-JSON data -> empty map for labels
	m, _ := CompileMatcher(nil, []LabelFilter{{Label: "level", Op: OpLabelEq, Value: ""}})
	require.True(t, m.MatchLabels(map[string]string{}))
	require.True(t, m.MatchLabels(nil))
}

func TestMatchCombined(t *testing.T) {
	m, _ := CompileMatcher(
		[]LineFilter{{Op: OpLineRe, Value: "panic"}},
		[]LabelFilter{{Label: "level", Op: OpLabelEq, Value: "ERROR"}},
	)
	data := `{"body":"panic recovered"}`
	labels := map[string]string{"level": "ERROR"}
	require.True(t, m.MatchLine(data))
	require.True(t, m.MatchLineBytes([]byte(data)))
	require.True(t, m.MatchLabels(labels))
	require.False(t, m.MatchLabels(map[string]string{"level": "INFO"}))
}
