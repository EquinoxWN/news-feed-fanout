# RFC 0001: news-feed-fanout design

- **Status:** Accepted for M1 (M2 and M3 sections are plans)
- **Author:** EquinoxWN
- **Created:** 2026-10

## Problem

A home timeline ("posts from the accounts I follow, newest first") has to load in a few
milliseconds. Building it at read time means querying every followed account on every read.
Building it at write time (fan-out on write) makes reads one sorted-set lookup, but one post
costs one timeline write per follower, and follower counts follow a power law: most accounts
have a handful of followers, a few have millions. This project builds the write path, measures
what it costs on a realistic graph, and (in M2/M3) adds the hybrid that real feeds use.

**Who has the problem:** backend engineers designing or debugging feed systems, and people
preparing for the "design the Twitter feed" system-design question who want numbers rather
than hand-waving.

## Critical-to-quality measures (Define)

| CTQ | Target (M1) | How it is checked |
|---|---|---|
| Every follower's timeline holds every post (no loss) | 0 missing entries | `verify:` line of the demo; oracle and redelivery tests |
| No duplicate timeline entries under redelivery | entries = sum of follower counts | `TestRedeliveryAfterFailuresIsIdempotent` |
| Each post body stored once | rows = posts published | `stored once:` line; `TestCelebrityPostReachesEveryFollower` |
| Graph has celebrity accounts | at least 2 accounts with >= 1M followers at 3M users | `graph` / `demo` output |
| Graph has a heavy tail | Hill tail exponent in [0.6, 2.0], top 1% of accounts hold > 40% of follows | `TestPowerLawShape` |
| Untrusted bytes cannot crash a worker | decoders reject malformed input, fuzzed | `FuzzDecode`, `FuzzReadReply`, malformed-input tests |

## Goals

- M1: store posts once; generate a power-law follow graph with a few accounts that have millions of
  followers; fan out on write through Kafka consumer groups into Valkey sorted sets.
- M2: above a follower threshold, skip fan-out and merge celebrity posts at read time; paginate with a cursor.
- M3: k6 measures write amplification, read p99 and cache memory for push, pull and hybrid; pick the threshold from those numbers.

## Non-goals

- Running as a hosted production service; authentication; ranking or recommendations.
- Durable post storage (posts live in an in-process store in M1; PostgreSQL is planned).
- Exactly-once delivery: the design is at-least-once plus idempotent writes.

## Design (M1)

```
Publish(author, body)
  -> post store (once, by 53-bit time-ordered ID)
  -> topic "posts"         key = author             value = PostCreated{postID, author}
       consumer group "fanout-split": one FanoutTask per chunk of the author's followers
  -> topic "fanout-tasks"  key = postID + offset   value = FanoutTask{postID, author, offset, count}
       consumer group "fanout-write": followers[offset:offset+count] -> ZADD tl:<user> id id
  -> timelines: sorted set per user, trimmed to the newest 800 entries
```

- **Graph:** each user follows k accounts, k from a truncated Pareto law (exponent 2.5, 8..2000);
  targets are drawn by a continuous Zipf law over popularity rank (exponent 1.0), so account 0 is
  the most followed. Two deterministic passes (count, then fill) build a CSR layout: one `uint32`
  offset per user and one `uint32` per follow edge (268.7 MiB for 67.4M edges).
- **IDs:** 41 bits of milliseconds since 2026-01-01 plus a 12-bit sequence, so every ID fits in
  53 bits and is stored exactly as a float64 sorted-set score. IDs never go backwards.
- **Two stages:** splitting a post into chunk tasks spreads one celebrity post across every
  partition of the tasks topic, instead of one worker writing 1.5M timelines alone (ADR 0002).
- **Delivery:** offsets are committed after the handler succeeds. A crash or error means
  redelivery; `ZADD` of the same member and score is a no-op, so redelivery adds nothing twice.
- **Interfaces:** `bus.Bus` (Kafka via franz-go, or an in-process partitioned log with consumer
  groups), `timeline.Store` (Valkey via a small RESP2 client, ADR 0003, or an in-process store).
  Local runs use the in-process versions; CI runs the Kafka and Valkey versions.
- **Bounds:** post body <= 1024 bytes and valid UTF-8; event decoders check length, version,
  ranges; the RESP reader caps line, bulk and array sizes and nesting depth.

## Alternatives considered

| Option | Why not (for M1) |
|---|---|
| Fan-out on read (pull): read every followed account's recent posts at request time | Reads cost one query per followed account (mean 22.5, up to 2000); the read path is what users wait on. It is the right tool for celebrities, so it comes back in M2 as half of the hybrid. |
| One fan-out message per post, one worker writes all followers | Simple, but a 1.5M-follower post pins one partition and one worker while the others idle; chunk tasks spread it (ADR 0002). |
| Store the full post in every timeline | Copies the body once per follower (1.5M copies for one post); IDs keep timelines small and edits/deletes cheap. |
| Real-world graph dataset (for example a public Twitter crawl) | Large downloads and licence terms; a seeded generator is reproducible, can be scaled, and its shape is checked by tests. |
| valkey-go client | Pulled a TLS path that govulncheck reported with Go 1.26.9 (GO-2026-6809, fixed in Go 1.26.10) and many features this project does not use (ADR 0003). |

## Measurement plan

- M1 (done): graph shape (percentiles, accounts over 100k and 1M, top-1% share, Hill exponent),
  write amplification (timeline writes per post), fan-out wall time and entries per second for one
  in-process run, Go heap in use, and a full verification of every follower timeline.
- M3: k6 drives publish and read traffic against push-only, pull-only and hybrid; measure write
  amplification, read p99 and Valkey memory (`INFO memory`) per strategy and sweep the threshold.

## Milestones

- **M1:** posts stored once; power-law graph; fan-out on write through Kafka-style consumer groups into sorted-set timelines.
- **M2:** follower threshold for pull; read-time merge and cursor pagination.
- **M3:** k6 comparison of push, pull and hybrid; threshold chosen from measured costs.

## Risks and open questions

- **Dual write:** the post is stored, then announced. If the announce fails the post exists but is
  not fanned out (the error says so). An outbox table, read by a relay, closes this gap once posts
  move to PostgreSQL.
- **Graph realism:** a 3M-user graph with million-follower accounts means half of all users follow
  the top account, which is denser than a real network of hundreds of millions. The measured tail
  exponent (about 1.0) is heavier than published follower-count fits for Twitter; the generator's
  exponents are flags so M3 can test sensitivity.
- **Timing noise:** local timings were taken while other builds shared the machine; M3 needs
  repeated runs before any number is used to pick a threshold.
