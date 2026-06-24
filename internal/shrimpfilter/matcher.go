package shrimpfilter

import (
	"bytes"
	"fmt"
	"regexp"
	"strings"
)

type LineOp uint8

const (
	OpLineEq    LineOp = iota // |=  (bytes.Contains)
	OpLineNotEq               // !=  (!bytes.Contains)
	OpLineRe                  // |~  (re.Find - partial match)
	OpLineNotRe               // !~  (re.Find == nil)
)

type LabelOp uint8

const (
	OpLabelEq    LabelOp = iota // eq  (string compare)
	OpLabelNotEq                // ne  (string compare)
	OpLabelRe                   // re  (PromQL-anchored: ^(?:re)$)
	OpLabelNotRe                // nre (!PromQL-anchored)
)

type LineFilter struct {
	Op    LineOp
	Value string
	Re    *regexp.Regexp
}

type LabelFilter struct {
	Label string
	Op    LabelOp
	Value string
	Re    *regexp.Regexp
}

type Matcher struct {
	Line   []LineFilter
	Labels []LabelFilter
}

func CompileMatcher(line []LineFilter, labels []LabelFilter) (Matcher, error) {
	out := Matcher{
		Line:   make([]LineFilter, len(line)),
		Labels: make([]LabelFilter, len(labels)),
	}
	for i, lf := range line {
		f := LineFilter{Op: lf.Op, Value: lf.Value}
		if lf.Op == OpLineRe || lf.Op == OpLineNotRe {
			re, err := regexp.Compile(lf.Value)
			if err != nil {
				return Matcher{}, fmt.Errorf("compile line regex %q: %w", lf.Value, err)
			}
			f.Re = re
		}
		out.Line[i] = f
	}
	for i, lf := range labels {
		f := LabelFilter{Label: lf.Label, Op: lf.Op, Value: lf.Value}
		if lf.Op == OpLabelRe || lf.Op == OpLabelNotRe {
			anchored := "^(?:" + lf.Value + ")$"
			re, err := regexp.Compile(anchored)
			if err != nil {
				return Matcher{}, fmt.Errorf("compile label regex %q: %w", lf.Value, err)
			}
			f.Re = re
		}
		out.Labels[i] = f
	}
	return out, nil
}

func (m Matcher) MatchLine(data string) bool {
	for _, f := range m.Line {
		switch f.Op {
		case OpLineEq:
			if !strings.Contains(data, f.Value) {
				return false
			}
		case OpLineNotEq:
			if strings.Contains(data, f.Value) {
				return false
			}
		case OpLineRe:
			if f.Re == nil || f.Re.FindString(data) == "" {
				return false
			}
		case OpLineNotRe:
			if f.Re != nil && f.Re.FindString(data) != "" {
				return false
			}
		}
	}
	return true
}

func (m Matcher) MatchLineBytes(data []byte) bool {
	for _, f := range m.Line {
		switch f.Op {
		case OpLineEq:
			if !bytes.Contains(data, []byte(f.Value)) {
				return false
			}
		case OpLineNotEq:
			if bytes.Contains(data, []byte(f.Value)) {
				return false
			}
		case OpLineRe:
			if f.Re == nil || f.Re.Find(data) == nil {
				return false
			}
		case OpLineNotRe:
			if f.Re != nil && f.Re.Find(data) != nil {
				return false
			}
		}
	}
	return true
}

func (m Matcher) MatchLabels(labels map[string]string) bool {
	for _, f := range m.Labels {
		val := labels[f.Label]
		switch f.Op {
		case OpLabelEq:
			if val != f.Value {
				return false
			}
		case OpLabelNotEq:
			if val == f.Value {
				return false
			}
		case OpLabelRe:
			if f.Re == nil || !f.Re.MatchString(val) {
				return false
			}
		case OpLabelNotRe:
			if f.Re != nil && f.Re.MatchString(val) {
				return false
			}
		}
	}
	return true
}

func (m Matcher) Empty() bool {
	return len(m.Line) == 0 && len(m.Labels) == 0
}
