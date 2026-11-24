# Spec: `lab-consumer` (`:8092`) — Kafka in, decisions and side effects out

## ADDED Requirements

### Requirement: Continuous pop
The app SHALL subscribe through Boot Kafka beans (`KafkaBroker` → `KafkaListenerContainerFactory` →
`@KafkaListener` on application methods) and SHALL keep polling for the life of the JVM. The app SHALL NOT
construct an `org.apache.kafka` consumer directly and SHALL NOT hardcode group names, topics, or block/timeout
values in Java code.

#### Scenario: alive after drain
- **WHEN** all current messages have been processed
- **THEN** the JVM MUST still be running and MUST keep polling
- **AND WHEN** a new message is pushed while the app is alive
- **THEN** it MUST be processed without restarting the app

#### Scenario: cold-start backlog
- **WHEN** the app starts on a topic that already holds messages
- **THEN** those messages MUST be processed
- **AND IF** the `skip.records` strategy is configured instead
- **THEN** the log MUST show the backlog skipped (the two options' difference made observable)

### Requirement: Three groups, one purpose each
The app SHALL run listener beans on groups `lab-consumer` (main lanes), `lab-competitor` (the two-listener
race on `lab-rebalance`), and `lab-txn` (transactional lane).

#### Scenario: one owner per message
- **WHEN** `lab-rebalance` messages are consumed by both `lab-consumer` and `lab-competitor` in the same group
- **THEN** the total number of processings across both listeners MUST equal the number of messages
- **AND THEN** the two listeners' offsets MUST be shared (same group)
- **AND WHEN** the same topic is read under a different group id
- **THEN** that group MUST keep its own independent offsets

### Requirement: Acknowledgment dial
`lab.ack.strategy` SHALL select one of `immediate`, `after-processed` (default), `batch` (checkpoint every
`lab.ack.batch-size`) and the app SHALL log the active mode at startup.

#### Scenario: modes differ observably
- **WHEN** the app runs in each mode with the same message sequence
- **THEN** the log MUST show the offset commit happening before processing (immediate), after processing
  (after-processed), or at checkpoint boundaries (batch)
- **AND THEN** `docs/experiments.md` MUST state the loss window for each mode

### Requirement: Per-message lifecycle log
For each popped message the app SHALL log one line carrying partition, offset, key, headers, payload, and the
decision (`accept`, `skip`, `reject-quarantine`, `retry`).

#### Scenario: decisions attributable
- **WHEN** a message is filtered out
- **THEN** the log MUST show decision `skip` with the header that caused it
- **AND THEN** a header/payload routing decision MUST be visible in the log

### Requirement: Filter by header, not by payload
A `RecordFilterStrategy` bean SHALL decide accept/skip from message headers/key before deserialization, and a
filtered-out message SHALL be acknowledged (skipped on purpose).

#### Scenario: skipped is not pending
- **WHEN** messages carrying `x-lab-skip: true` are consumed
- **THEN** they MUST NOT be processed and MUST NOT be re-read on the next poll
- **AND IF** they are re-read
- **THEN** the filter MUST be treated as broken

### Requirement: Bounded retry with exponential backoff
A `CommonErrorHandler`-equivalent bean SHALL retry a processing failure at most `lab.retry.max-attempts`
(default 3) times with backoff `lab.retry.backoff.base.ms` × 2^attempt, then quarantine.

#### Scenario: injected failure rate
- **WHEN** `POST /lab/inject-failures {"percentage":30,"lane":"lab-orders"}` is issued and 50 orders are pushed
- **THEN** roughly 30 % of processing attempts MUST throw
- **AND THEN** each affected message MUST be retried within the cap and MUST end handled or quarantined
- **AND THEN** the log MUST show the backoff growing
- **AND IF** an exception escapes the error handler
- **THEN** the listener MUST NOT stop and an offset MUST NOT silently advance past an unacked message

### Requirement: Quarantine lane (`lab-rejected`)
After the retry budget the message SHALL be written to `lab-rejected` per the quarantine contract, carrying
raw payload, reason, source partition/offset, and timestamps, and the source message SHALL then be ACKed.

#### Scenario: re-drivable record
- **WHEN** a poison message is quarantined
- **THEN** `GET /lab/rejected` MUST return it with `sourcePartition`/`sourceOffset`
- **AND THEN** the record MUST be re-publishable to `lab-orders` by the documented command
- **AND IF** the reason is a deliberate skip
- **THEN** no quarantine record MUST be written

### Requirement: Rollback processor with two modes
`lab.rollback.mode` SHALL select `retry-next-poll` (default) or `discard` via an
`AfterRollbackProcessor`-equivalent bean.

#### Scenario: rollback then retry
- **WHEN** a transaction is forced to fail
- **THEN** in `retry-next-poll` mode the side-effect topic offsets MUST NOT advance and the source message MUST
  be reprocessed on a later poll
- **AND IN** `discard` mode the message MUST be skipped with a log line naming the offset discarded

### Requirement: Transactional pop
The transactional lane SHALL ack the source message and push side effects to `lab-ack-a` and `lab-ack-b`
inside one Kafka transaction (`KafkaTransactionManager` bean; `ChainedKafkaTransactionManager` marked `@Primary`
where chaining is used).

#### Scenario: atomic pair, no half-state
- **WHEN** one transactional message is handled successfully
- **THEN** `lab-ack-a` and `lab-ack-b` MUST each gain exactly one record keyed by `orderId`
- **AND WHEN** a side effect is forced to fail
- **THEN** the source offset MUST be unchanged and the message MUST be reprocessed later

### Requirement: Graceful shutdown preserves the backlog
On SIGTERM the app SHALL unsubscribe beans, destroy factories, commit what it has legitimately ACKed, and exit
0 within its shutdown timeout.

#### Scenario: restart after SIGTERM
- **WHEN** the app is stopped with SIGTERM while messages remain un-ACKed and then restarted
- **THEN** the un-ACKed messages MUST be reprocessed exactly once
- **AND** messages already ACKed MUST NOT be reprocessed
- **AND THEN** the log MUST state the number of pending messages at shutdown

### Requirement: Rebalance and seek
A `ConsumerAwareRebalanceListener`-equivalent bean SHALL handle partition close/open; with
`GROUP_INITIAL_REBALANCE_DELAY_MS=0` on this broker, closing one listener MUST cause the survivor to re-read
from the group offsets. `POST /lab/listeners/competitor/off` SHALL stop the competitor listener.

#### Scenario: survivor re-reads from offsets
- **WHEN** the competitor listener is turned off while messages are in flight
- **THEN** the survivor MUST process the messages the competitor had not ACKed
- **AND THEN** `GET /lab/diagnostics` MUST show the offsets that made that possible
- **AND THEN** the log MUST state what would differ with a multi-broker cluster (concept-only)

### Requirement: Idempotent processing
The app SHALL keep an in-memory ledger keyed by `orderId` so crash-retry redelivery does not double-count an
order.

#### Scenario: redelivery after forced failure
- **WHEN** one order is processed, fails, is retried, and succeeds
- **THEN** the ledger MUST count it once
- **AND THEN** the README MUST state that ACK means "don't lose", not "fully processed"

### Requirement: Broker-owned state, read-only
`GET /lab/diagnostics` SHALL report per partition: this group's partition-offset, the partition's next offset,
the in-memory queue size, and — through `spring.kafka.admin.*` — the topic list, `__consumer_offsets`, and
`_schemas` (AVRO versions). No write path SHALL touch those topics.

#### Scenario: two different "where is my data" questions
- **WHEN** `GET /lab/backlog` is called
- **THEN** it MUST report the in-process queue size, distinct from the offset gap
- **AND WHEN** `GET /lab/metrics` is called
- **THEN** per-listener throughput/timing labels MUST appear via Boot metrics + Micrometer
- **AND THEN** no topic offset MAY change because of a diagnostics call
