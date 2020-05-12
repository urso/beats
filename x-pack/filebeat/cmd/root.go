// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License;
// you may not use this file except in compliance with the Elastic License.

package cmd

import (
	fbcmd "github.com/elastic/beats/v7/filebeat/cmd"
	cmd "github.com/elastic/beats/v7/libbeat/cmd"
	xpackcmd "github.com/elastic/beats/v7/x-pack/libbeat/cmd"

	// Register the includes.
	inputs "github.com/elastic/beats/v7/x-pack/filebeat/features/input/default-inputs"
	_ "github.com/elastic/beats/v7/x-pack/filebeat/include"
)

const Name = fbcmd.Name

// Filebeat build the beat root command for executing filebeat and it's subcommands.
func Filebeat() *cmd.BeatsRootCmd {
	command := fbcmd.Filebeat(inputs.Init)
	xpackcmd.AddXPack(command, Name)
	return command
}
