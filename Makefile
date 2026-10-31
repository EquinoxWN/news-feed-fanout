.PHONY: setup lint test test-integration bench fuzz audit demo ci

setup:
	go mod download

lint:
	@files="$$(gofmt -l cmd internal)"; test -z "$$files" || (echo "$$files"; echo "run gofmt -w"; exit 1)
	go vet ./...
	go vet -tags integration ./...

test:
	go test -race -count=1 ./...

# Needs Kafka and Valkey (CI starts them as service containers).
test-integration:
	go test -tags integration -count=1 -timeout 6m -run KafkaAndValkey -v ./internal/fanout

# The numbers in the README: 3M-user graph, fan-out of 2,003 posts in-process (about 0.5 GB of RAM).
bench:
	go run ./cmd/news-feed-fanout demo -users 3000000 -posts 2000 -celebrities 3

fuzz:
	go test -run '^$$' -fuzz FuzzDecode -fuzztime 30s ./internal/event
	go test -run '^$$' -fuzz FuzzReadReply -fuzztime 30s ./internal/timeline

# Known vulnerabilities in called code (Go 1.26.10 as in CI); any finding fails.
audit:
	bash scripts/vulncheck.sh

demo:
	go run ./cmd/news-feed-fanout demo

ci: setup lint test
