package com.zendesk.maxwell.replication;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Timer;
import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.shyiko.mysql.binlog.event.GtidEventData;
import com.github.shyiko.mysql.binlog.event.MariadbGtidEventData;
import com.zendesk.maxwell.monitoring.Metrics;
import com.zendesk.maxwell.producer.MaxwellOutputConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

class BinlogConnectorEventListener implements BinaryLogClient.EventListener {
	private static final Logger LOGGER = LoggerFactory.getLogger(BinlogConnectorEventListener.class);
	private static final long LAG_WARN_THRESHOLD_MS = 30_000;
	private static final long QUEUE_FULL_LOG_INTERVAL_MS = 5_000;

	private final BlockingQueue<BinlogConnectorEvent> queue;
	private final Timer queueTimer;
	protected final AtomicBoolean mustStop = new AtomicBoolean(false);
	private final BinaryLogClient client;
	private final MaxwellOutputConfig outputConfig;
	private long replicationLag;
	private String gtid;
	private long lastQueueFullLogAt = 0;
	private long queueFullOfferCount = 0;

	public BinlogConnectorEventListener(
		BinaryLogClient client,
		BlockingQueue<BinlogConnectorEvent> q,
		Metrics metrics,
		MaxwellOutputConfig outputConfig
	) {
		this.client = client;
		this.queue = q;
		this.queueTimer =  metrics.getRegistry().timer(metrics.metricName("replication", "queue", "time"));
		this.outputConfig = outputConfig;

		final BinlogConnectorEventListener self = this;
		metrics.register(metrics.metricName("replication", "lag"), (Gauge<Long>) () -> self.replicationLag);
	}

	public void stop() {
		mustStop.set(true);
	}

	@Override
	public void onEvent(Event event) {
		long eventSeenAt = 0;
		boolean trackMetrics = false;

		EventType eventType = event.getHeader().getEventType();

		if ( eventType == EventType.GTID) {
			gtid = ((GtidEventData)event.getData()).getGtid();
		} else if ( eventType == EventType.MARIADB_GTID) {
			gtid = ((MariadbGtidEventData)event.getData()).toString();
		}

		BinlogConnectorEvent ep = new BinlogConnectorEvent(event, client.getBinlogFilename(), client.getGtidSet(), gtid, outputConfig);

		if (ep.isCommitEvent()) {
			trackMetrics = true;
			eventSeenAt = System.currentTimeMillis();
			replicationLag = eventSeenAt - event.getHeader().getTimestamp();
			if ( replicationLag > LAG_WARN_THRESHOLD_MS ) {
				LOGGER.warn("[event-listener] high replication lag detected: {}ms ({}s)", replicationLag, replicationLag / 1000);
			}
		}

		while (mustStop.get() != true) {
			try {
				if ( queue.offer(ep, 100, TimeUnit.MILLISECONDS ) ) {
					if ( queueFullOfferCount > 0 ) {
						LOGGER.info("[event-listener] queue unblocked after {} failed offers, current queue size={}",
							queueFullOfferCount, queue.size());
						queueFullOfferCount = 0;
					}
					break;
				} else {
					queueFullOfferCount++;
					long now = System.currentTimeMillis();
					if ( now - lastQueueFullLogAt >= QUEUE_FULL_LOG_INTERVAL_MS ) {
						LOGGER.warn("[event-listener] replication event queue is full (capacity={}, size={}), producer may be stuck. Blocked for ~{}ms on event type={}",
							queue.size() + queue.remainingCapacity(), queue.size(),
							queueFullOfferCount * 100L, eventType);
						lastQueueFullLogAt = now;
					}
				}
			} catch (InterruptedException e) {
				return;
			}
		}

		if (trackMetrics) {
			queueTimer.update(System.currentTimeMillis() - eventSeenAt, TimeUnit.MILLISECONDS);
		}
	}
}

