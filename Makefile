.PHONY: protoc build test clean run

protoc:
	PATH=$$PATH:$(HOME)/go/bin protoc \
		--proto_path=proto \
		--proto_path=/tmp/googleapis \
		--go_out=. \
		--go_opt=module=github.com/devil-mice-labs/metricbucketferry \
		proto/metrics/*.proto

build: protoc
	go build -o metricbucketferry cmd/metricbucketferry/main.go

test: protoc
	go test ./...

clean:
	rm -rf pkg/metricspb
	rm -f output.pb
	rm -f metricbucketferry

run:
	go run cmd/metricbucketferry/main.go
