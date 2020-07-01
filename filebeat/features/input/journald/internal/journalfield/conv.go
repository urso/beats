package journalfield

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/elastic/beats/v7/libbeat/common"
	"github.com/elastic/beats/v7/libbeat/logp"
)

type FieldConversion map[string]Conversion

type Conversion struct {
	Name      string
	IsInteger bool
	Dropped   bool
}

type Converter struct {
	log         *logp.Logger
	conversions FieldConversion
}

func NewConverter(log *logp.Logger, conversions FieldConversion) *Converter {
	if conversions == nil {
		conversions = journaldEventFields
	}

	return &Converter{log: log, conversions: conversions}
}

func (c *Converter) Convert(entryFields map[string]string) common.MapStr {
	fields := common.MapStr{}
	var custom common.MapStr

	for entryKey, v := range entryFields {
		if fieldConversionInfo, ok := c.conversions[entryKey]; !ok {
			if custom == nil {
				custom = common.MapStr{}
			}
			normalized := strings.ToLower(strings.TrimLeft(entryKey, "_"))
			custom.Put(normalized, v)
		} else if !fieldConversionInfo.Dropped {
			value, err := convertValue(fieldConversionInfo, v)
			if err != nil {
				value = v
				c.log.Debugf("Journald mapping error: %v", err)
			}
			fields.Put(fieldConversionInfo.Name, value)
		}
	}

	if len(custom) != 0 {
		fields.Put("journald.custom", custom)
	}

	return fields
}

func convertValue(fc Conversion, value string) (interface{}, error) {
	if fc.IsInteger {
		v, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			// On some versions of systemd the 'syslog.pid' can contain the username
			// appended to the end of the pid. In most cases this does not occur
			// but in the cases that it does, this tries to strip ',\w*' from the
			// value and then perform the conversion.
			s := strings.Split(value, ",")
			v, err = strconv.ParseInt(s[0], 10, 64)
			if err != nil {
				return value, fmt.Errorf("failed to convert field %s \"%v\" to int: %v", fc.Name, value, err)
			}
		}
		return v, nil
	}
	return value, nil
}
