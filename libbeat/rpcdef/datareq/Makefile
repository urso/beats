.PHONY: all
all: datareq.pb.go datareq_grpc.pb.go

.PHONY: clean
clean:
	rm -f *.go

datareq.pb.go: datareq.proto
	protoc --plugin=$(which protoc-gen-go) \
		--go_out=. --go_opt=paths=source_relative \
		datareq.proto

datareq_grpc.pb.go: datareq.proto
	protoc --plugin=$(which protoc-gen-go) \
		--go-grpc_out=. --go-grpc_opt=paths=source_relative \
		datareq.proto
