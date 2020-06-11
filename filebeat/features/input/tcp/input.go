// Licensed to Elasticsearch B.V. under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Elasticsearch B.V. licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package tcp

import (
	"bufio"
	"crypto/tls"
	"fmt"
	"net"
	"sync"
	"time"

	input "github.com/elastic/beats/v7/filebeat/input/v2"
	stateless "github.com/elastic/beats/v7/filebeat/input/v2/input-stateless"
	"github.com/elastic/beats/v7/filebeat/inputsource"
	netcommon "github.com/elastic/beats/v7/filebeat/inputsource/common"
	"github.com/elastic/beats/v7/filebeat/inputsource/tcp"
	"github.com/elastic/beats/v7/libbeat/beat"
	"github.com/elastic/beats/v7/libbeat/common"
	"github.com/elastic/beats/v7/libbeat/common/transport/tlscommon"
	"github.com/elastic/beats/v7/libbeat/feature"
	"github.com/elastic/go-concert/ctxtool"
)

type server struct {
	config
	splitFunc bufio.SplitFunc
}

func Plugin() input.Plugin {
	return input.Plugin{
		Name:       "tcp",
		Stability:  feature.Stable,
		Deprecated: false,
		Info:       "TCP server",
		Doc:        "The tcp input creates a TCP server and reads line delimited events",
		Manager:    stateless.NewInputManager(configure),
	}
}

func configure(cfg *common.Config) (stateless.Input, error) {
	config := defaultConfig()
	if err := cfg.Unpack(&config); err != nil {
		return nil, err
	}
	return newServer(config)
}

func newServer(config config) (*server, error) {
	splitFunc := netcommon.SplitFunc([]byte(config.LineDelimiter))
	if splitFunc == nil {
		return nil, fmt.Errorf("unable to create splitFunc for delimiter %s", config.LineDelimiter)
	}

	return &server{config: config, splitFunc: splitFunc}, nil
}

func (s *server) Name() string { return "tcp" }

// Test checks if the server can be executed as is by trying to bind the port.
func (s *server) Test(ctx input.TestContext) error {
	tlsConfig, err := tlscommon.LoadTLSServerConfig(s.TLS)
	if err != nil {
		return err
	}

	var l net.Listener
	if tlsConfig != nil {
		l, err = tls.Listen("tcp", s.Host, tlsConfig.BuildModuleConfig(s.Host))
	} else {
		l, err = net.Listen("tcp", s.Host)
	}
	if err != nil {
		return err
	}
	return l.Close()
}

// Run starts the TCP server waiting for clients to connect.  Run returns on
// failure to create the listener, or after receiving a shutdown signal from
// the execution context.
func (s *server) Run(ctx input.Context, publisher stateless.Publisher) error {
	// initialize server and event publishing
	cb := func(data []byte, metadata inputsource.NetworkMetadata) {
		event := createEvent(data, metadata)
		publisher.Publish(event)
	}
	factory := netcommon.SplitHandlerFactory(netcommon.FamilyTCP, ctx.Logger, tcp.MetadataCallback, cb, s.splitFunc)
	server, err := tcp.New(&s.config.Config, factory)
	if err != nil {
		return err
	}

	ctx.Logger.Debugf("TCP Input '%v' initialized", ctx.ID)

	// run server in background and add support to wait for shutdown
	// Note: We start the server before setting up shutdown signaling. Doing it the
	// other way around can lead to panics or the input not shutting down in
	// case the beat has already received the shutdown signal while starting up.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err = server.Run()
	}()
	ctx.Logger.Debugf("TCP Input '%v' active", ctx.ID)

	// make sure we call server.Stop() on shutdown or if Run has finished by
	// itself.
	_, cancel := ctxtool.WithFunc(ctxtool.FromCanceller(ctx.Cancelation), func() {
		ctx.Logger.Debugf("TCP Input '%v' received stop signal", ctx.ID)
		server.Stop()
	})
	defer cancel()

	// wait for 'Run' to return
	wg.Wait()

	// ignore error from 'Run' in case shutdown was signaled.
	if ctxerr := ctx.Cancelation.Err(); ctxerr != nil {
		err = ctxerr
	}
	return err
}

func createEvent(raw []byte, metadata inputsource.NetworkMetadata) beat.Event {
	return beat.Event{
		Timestamp: time.Now(),
		Fields: common.MapStr{
			"message": string(raw),
			"log": common.MapStr{
				"source": common.MapStr{
					"address": metadata.RemoteAddr.String(),
				},
			},
		},
	}
}
