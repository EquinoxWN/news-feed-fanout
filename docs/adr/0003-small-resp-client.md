# ADR 0003: A small RESP2 client for Valkey instead of valkey-go

- **Status:** Accepted

## Context

The timeline store needs four commands: `ZADD`, `ZREMRANGEBYRANK`, `ZRANGE ... BYSCORE REV LIMIT`
and `ZSCORE`, sent in pipelines. The first version used `github.com/valkey-io/valkey-go` v1.0.79.
With Go 1.26.9 (the version the portfolio pins), `govulncheck@v1.8.0` reported GO-2026-6809
(crypto/x509, fixed in Go 1.26.10) as reachable from `Valkey.CountMissing` through the client's
TLS support, which this project never turns on. A first rewrite on `bufio` was still flagged,
because the call graph lets any `io.Reader` stored in a `bufio.Reader` be a TLS connection.

## Decision

`internal/timeline/resp.go` implements the RESP2 subset directly: commands are encoded into one
byte slice and written to a `*net.TCPConn`; replies are read by a 64 KiB buffered reader that only
wraps that TCP connection. The reader caps line length (512 bytes), bulk strings (1 MiB), arrays
(1M elements) and nesting (depth 4). A pool keeps up to 8 idle connections; a connection is closed
after any I/O or protocol error, because the stream position is then unknown.

## Consequences

- One dependency less, and the Valkey path no longer reaches crypto/x509.
- The client is tested locally against a loopback fake server that implements the four commands,
  plus a fuzz test of the reply decoder; the real Valkey run is in CI.
- No TLS, AUTH, RESP3 or cluster support. Adding them later means either extending this client or
  returning to a full client; since the move to Go 1.26.10 (2026-10-30) GO-2026-6809 is fixed in the
  standard library, so that advisory no longer argues against a full client.
- With Go 1.26.10, govulncheck reports no vulnerabilities for the whole module (the Kafka client's
  TLS dialer included), and `scripts/vulncheck.sh` fails on any finding.
