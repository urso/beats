package journalfield

import (
	"fmt"
	"strings"
)

// Matcher is a single field condition for filtering journal entries.
//
// The Matcher type can be used as is with Beats configuration unpacking. The
// internal default conversion table will be used, similar to BuildMatcher.
type Matcher struct {
	str string
}

// MatcherBuilder can be used to create a custom builder for creating matchers
// based on a conversion table.
type MatcherBuilder struct {
	Conversions map[string]Conversion
}

type journal interface {
	AddMatch(string) error
	AddDisjunction() error
}

var defaultBuilder = MatcherBuilder{Conversions: journaldEventFields}

// Build creates a new Matcher using the configured conversion table.
// If no table has been configured the internal default table will be used.
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

// BuildMatcher creates a Matcher from a field filter string.
func BuildMatcher(in string) (Matcher, error) {
	return defaultBuilder.Build(in)
}

// IsValue returns true if the matcher was initialized correctly.
func (m Matcher) IsValid() bool { return m.str != "" }

// String returns the string representation of the field match.
func (m Matcher) String() string { return m.str }

// Apply adds the field match to an open journal for filtering.
func (m Matcher) Apply(j journal) error {
	if !m.IsValid() {
		return fmt.Errorf("can not apply invalid matcher to a journal")
	}

	err := j.AddMatch(m.str)
	if err != nil {
		return fmt.Errorf("error adding match '%s' to journal: %v", m.str, err)
	}
	return nil
}

// Unpack initializes the Matcher from a given string representation. Unpack
// fails if the input string is invalid.
// Unpack can be used with Beats configuration loading.
func (m *Matcher) Unpack(value string) error {
	tmp, err := BuildMatcher(value)
	if err != nil {
		return err
	}
	*m = tmp
	return nil
}

// ApplyMatchersOr adds a list of matchers to a journal, calling AddDisjunction after each matcher being added.
func ApplyMatchersOr(j journal, matchers []Matcher) error {
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
