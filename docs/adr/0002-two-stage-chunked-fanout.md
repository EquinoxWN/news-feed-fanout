# ADR 0002: Split each post into chunked fan-out tasks

- **Status:** Accepted

## Context

A Kafka topic is processed in parallel only across partitions, and records with the same key go
to the same partition. If the fan-out message for a post is the unit of work, all timeline writes
for a post happen in one worker. In the generated 3M-user graph the most-followed account has
1,537,803 followers while the median account has 4, so one celebrity post would keep one worker
busy while the rest of the group idles, and every post queued behind it on that partition waits.

## Decision

Fan-out runs in two consumer groups. The splitter reads `PostCreated` and publishes one
`FanoutTask{postID, author, offset, count}` per chunk of followers (1000 by default) to a second
topic, keyed by post ID and offset so chunks land on different partitions. Writers apply a chunk
with one pipelined batch of timeline writes.

## Consequences

- One post's work spreads over every partition and writer; in the demo the three celebrity posts
  became 3,490 of the 5,406 tasks.
- One extra hop and one extra topic: more messages (one per 1000 followers) and a second consumer
  group to monitor.
- Task records carry only an offset into the follower list, so a writer must read the same graph
  version as the splitter. In M1 the graph is immutable; a changing graph would need a snapshot ID
  in the task or the follower IDs in the message.
- Chunk size trades message count against batch size; it is a flag (`-chunk`), not yet tuned.
