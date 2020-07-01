package journalfield

import (
	"fmt"
	"strings"

	"github.com/coreos/go-systemd/v22/sdjournal"
)

type Matcher struct {
	str string
}

type MatcherBuilder struct {
	Conversions map[string]Conversion
}

var defaultBuilder = MatcherBuilder{Conversions: journaldEventFields}

func (b MatcherBuilder) Build(in string) (Matcher, error) {
	elems := strings.Split(in, "=")
	if len(elems) != 2 {
		return Matcher{}, fmt.Errorf("invalid match format: %s", in)
	}

	conversions := b.Conversions
	if conversions == nil {
		conversions = journaldEventFields
	}

	for journalKey, eventField := range conversions {
		if elems[0] == eventField.Name {
			return Matcher{journalKey + "=" + elems[1]}, nil
		}
	}

	// pass custom fields as is
	return Matcher{in}, nil
}

func BuildMatcher(in string) (Matcher, error) {
	return defaultBuilder.Build(in)
}

func (m Matcher) IsValid() bool { return m.str != "" }

func (m Matcher) String() string { return m.str }

func (m Matcher) Apply(j *sdjournal.Journal) error {
	if !m.IsValid() {
		return fmt.Errorf("can not apply invalid matcher to a journal")
	}

	err := j.AddMatch(m.str)
	if err != nil {
		return fmt.Errorf("error adding match '%s' to journal: %v", m.str, err)
	}
	return nil
}

func (m *Matcher) Unpack(value string) error {
	tmp, err := BuildMatcher(value)
	if err != nil {
		return err
	}
	*m = tmp
	return nil
}

func ApplyMatchersOr(j *sdjournal.Journal, matchers []Matcher) error {
	for _, m := range matchers {
		if err := m.Apply(j); err != nil {
			return err
		}

		if err := j.AddDisjunction(); err != nil {
			return fmt.Errorf("error adding disjunction to journal: %v", err)
		}
	}

	return nil
}
