# Speed Lane vs. Batch Lane: Write-up

**Author:** Ashish Rijal

## How the two lanes relate

Phase 1 (the batch lane) syncs the OLTP database and flat files every night at 01:00 AM Eastern and builds a complete, tested star schema for Power BI. Phase 2 (the speed lane) reads the same kind of business events, orders placed, paid, shipped and cancelled, but processes them continuously through Kafka and Databricks Structured Streaming. They use the same medallion pattern and the same business vocabulary, so the two lanes describe one business at two different speeds.

## When a business needs each

The **batch lane** is the right tool for questions that need complete, reconciled and historical answers: monthly revenue, product margin after supplier cost, customer growth, and finance reporting. It is cheaper, easier to test, and gives numbers that are final once the night's run finishes. Waiting until morning costs nothing for these questions.

The **speed lane** is the right tool when waiting is the problem: how many orders are arriving right now, and did revenue suddenly drop in the last ten minutes? Operations, support and fraud or outage monitoring need to react within minutes, not tomorrow. The trade-off is that streaming numbers are provisional, while the batch numbers remain the source of truth.

## Why Gold windows appear late

The Gold table groups payments into 5-minute windows, and a window is only written once Spark knows no more events can arrive for it. Spark learns this from the watermark, which is the newest event time seen minus 10 minutes. A window is emitted only after the watermark passes its end, so the newest rows always run roughly 10 to 15 minutes behind real time. This is deliberate: the delay lets late or out-of-order events land in the correct window, so each emitted window is correct and does not need to be rewritten.

## Why events are keyed by `order_id`

Kafka guarantees ordering only within a partition, and events with the same key always go to the same partition. Keying by `order_id` keeps one order's lifecycle (placed, then paid, then shipped or cancelled) in order. Without it, a "cancelled" event could be processed before its "placed" event, which would corrupt any per-order logic. De-duplication is a separate concern: it uses the unique `event_id`, not `order_id`.
