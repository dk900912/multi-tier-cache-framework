package io.github.dk900912.multitiercache.spi.support;

import io.github.dk900912.multitiercache.api.CacheMessageDeliveryEvent;
import io.github.dk900912.multitiercache.api.CacheMessageDeliveryEventType;
import io.github.dk900912.multitiercache.api.CacheMessageListener;
import io.github.dk900912.multitiercache.api.model.CacheConfig;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PubSubMessageDispatcherTest {

    @Test
    void aggregatesTenThousandRejectionsIntoOneOverloadEpisode() throws Exception {
        CacheConfig.Subscriber subscriber = new CacheConfig.Subscriber();
        subscriber.setCorePoolSize(1);
        subscriber.setMaximumPoolSize(1);
        subscriber.setCapacity(1);
        subscriber.setRecoveryLowWatermarkRatio(0.5d);
        subscriber.setRecoveryQuietPeriod(Duration.ofMillis(150));

        CountDownLatch workerStarted = new CountDownLatch(1);
        CountDownLatch releaseWorker = new CountDownLatch(1);
        CountDownLatch overloaded = new CountDownLatch(1);
        CountDownLatch recovered = new CountDownLatch(1);
        AtomicInteger overloadEvents = new AtomicInteger();
        AtomicInteger recoveryEvents = new AtomicInteger();
        AtomicLong recoveredDrops = new AtomicLong();

        CacheMessageListener listener = new CacheMessageListener() {
            @Override
            public void onMessage(String channel, String message) {
                if ("block".equals(message)) {
                    workerStarted.countDown();
                    try {
                        releaseWorker.await(5, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
            }

            @Override
            public void onDeliveryEvent(CacheMessageDeliveryEvent event) {
                if (event.type() == CacheMessageDeliveryEventType.PROCESSING_OVERLOADED) {
                    overloadEvents.incrementAndGet();
                    overloaded.countDown();
                } else if (event.type() == CacheMessageDeliveryEventType.PROCESSING_RECOVERED) {
                    recoveryEvents.incrementAndGet();
                    recoveredDrops.set(event.droppedMessages());
                    recovered.countDown();
                }
            }
        };

        try (PubSubMessageDispatcher dispatcher =
                     new PubSubMessageDispatcher(subscriber, "dispatcher-test")) {
            dispatcher.dispatch("cache", "block", listener);
            assertTrue(workerStarted.await(2, TimeUnit.SECONDS));
            dispatcher.dispatch("cache", "queued", listener);

            for (int i = 0; i < 10_000; i++) {
                dispatcher.dispatch("cache", "dropped-" + i, listener);
            }

            assertTrue(overloaded.await(2, TimeUnit.SECONDS));
            assertEquals(1, overloadEvents.get());

            releaseWorker.countDown();
            assertFalse(recovered.await(50, TimeUnit.MILLISECONDS),
                    "Recovery must wait for the configured quiet period");
            assertTrue(recovered.await(3, TimeUnit.SECONDS));
            assertEquals(1, recoveryEvents.get());
            assertEquals(10_000L, recoveredDrops.get());
        } finally {
            releaseWorker.countDown();
        }
    }
}
