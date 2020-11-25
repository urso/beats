package grpcbatch

import "github.com/elastic/beats/v7/libbeat/outputs"

func init() {
	outputs.RegisterType("grpcbatch", configureBatch)
	outputs.RegisterType("grpcstream", configureStream)
}
