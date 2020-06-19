package main

import (
	v2 "github.com/elastic/beats/v7/filebeat/input/v2"
	"github.com/elastic/beats/v7/libbeat/beat"
	"github.com/elastic/beats/v7/libbeat/common"
	"github.com/elastic/beats/v7/metricbeat/mb"
	"github.com/elastic/beats/v7/metricbeat/mb/module"

	_ "github.com/elastic/beats/v7/metricbeat/include"
	_ "github.com/elastic/beats/v7/metricbeat/include/fields"

	_ "github.com/elastic/beats/v7/auditbeat/include"
)

type metricbeatRegistry struct {
	factory *module.Factory
}

type metricbeatPluginManager struct {
	factory *module.Factory
	name    string
}

type metricbeatInput struct {
	factory *module.Factory
	config  *common.Config
}

func makeMetricbeatRegistry(info beat.Info, opts []module.Option) v2.Registry {
	factory := module.NewFactory(info, opts...)
	return &runnerFactoryRegistry{
		typeField: "module",
		factory:   factory,
		has: func(name string) bool {
			if isAuditModule(name) {
				return false
			}

			for _, other := range mb.Registry.Modules() {
				if other == name {
					return true
				}
			}
			return false
		},
	}
}

func makeAuditbeatRegistry(info beat.Info, opts []module.Option) v2.Registry {
	factory := module.NewFactory(info, opts...)
	return &runnerFactoryRegistry{
		typeField: "module",
		factory:   factory,
		has: func(name string) bool {
			if !isAuditModule(name) {
				return false
			}

			for _, other := range mb.Registry.Modules() {
				if other == name {
					return true
				}
			}
			return false
		},
	}
}

func isAuditModule(name string) bool {
	return name == "auditd" || name == "file_integrity"
}