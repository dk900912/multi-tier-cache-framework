package io.github.dk900912.multitiercache.core;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SingleFlightTest {

    private final SingleFlight singleFlight = new SingleFlight();

    @Test
    void testExecute_SingleCall() {
        String result = singleFlight.execute("key", Duration.ofSeconds(1), () -> "success");
        assertEquals("success", result);
    }

    @Test
    void testExecute_SlowFirstCall_ShouldNotBeTreatedAsRecursive() {
        String result = singleFlight.execute("slow-first-key", Duration.ofSeconds(1), () -> {
            Thread.sleep(100);
            return "success";
        });

        assertEquals("success", result);
    }

    @Test
    void directExecutorShouldPublishFlightBeforeStartingLoader() {
        SingleFlight directSingleFlight = new SingleFlight(Runnable::run);
        AtomicInteger loaderCalls = new AtomicInteger();

        String first = directSingleFlight.execute("direct-key", Duration.ofSeconds(1), () -> {
            loaderCalls.incrementAndGet();
            return "first";
        });
        String second = directSingleFlight.execute("direct-key", Duration.ofSeconds(1), () -> {
            loaderCalls.incrementAndGet();
            return "second";
        });

        assertEquals("first", first);
        assertEquals("second", second);
        assertEquals(2, loaderCalls.get());
    }

    @Test
    void rejectedExecutionShouldRemovePublishedFlight() {
        AtomicInteger submissions = new AtomicInteger();
        SingleFlight rejectingSingleFlight = new SingleFlight(command -> {
            if (submissions.getAndIncrement() == 0) {
                throw new RejectedExecutionException("rejected");
            }
            command.run();
        });

        assertThrows(RejectedExecutionException.class,
                () -> rejectingSingleFlight.execute("rejected-key", Duration.ofSeconds(1), () -> "unexpected"));

        assertEquals("recovered", rejectingSingleFlight.execute(
                "rejected-key", Duration.ofSeconds(1), () -> "recovered"));
    }

    @Test
    void completedResultShouldBePublishedOnlyAfterFlightRemoval() throws Exception {
        try (ExecutorService loaderExecutor = Executors.newSingleThreadExecutor();
             ExecutorService callerExecutor = Executors.newSingleThreadExecutor()) {
            SingleFlight observedSingleFlight = new SingleFlight(loaderExecutor);
            CountDownLatch loaderEntered = new CountDownLatch(1);
            CountDownLatch releaseLoader = new CountDownLatch(1);
            Future<String> response = callerExecutor.submit(() -> observedSingleFlight.execute(
                    "completion-order-key",
                    Duration.ofSeconds(5),
                    () -> {
                        loaderEntered.countDown();
                        assertTrue(releaseLoader.await(5, TimeUnit.SECONDS));
                        return "value";
                    }));

            assertTrue(loaderEntered.await(5, TimeUnit.SECONDS));

            Field flightsField = SingleFlight.class.getDeclaredField("flights");
            flightsField.setAccessible(true);
            @SuppressWarnings("unchecked")
            ConcurrentHashMap<String, Object> flights =
                    (ConcurrentHashMap<String, Object>) flightsField.get(observedSingleFlight);
            Object activeFlight = awaitPublishedFlight(flights, "completion-order-key");
            var resultAccessor = activeFlight.getClass().getDeclaredMethod("result");
            resultAccessor.setAccessible(true);
            @SuppressWarnings("unchecked")
            CompletableFuture<Object> result = (CompletableFuture<Object>) resultAccessor.invoke(activeFlight);
            AtomicBoolean mappedAtCompletion = new AtomicBoolean(true);
            result.whenComplete((ignored, failure) ->
                    mappedAtCompletion.set(flights.get("completion-order-key") == activeFlight));

            releaseLoader.countDown();

            assertEquals("value", response.get(5, TimeUnit.SECONDS));
            assertFalse(mappedAtCompletion.get(), "Flight must be removed before its result becomes observable");
        }
    }

    @Test
    void testExecute_ConcurrentCalls_OnlyOneExecution() throws InterruptedException {
        int threads = 50;
        ExecutorService executor = Executors.newFixedThreadPool(threads);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(threads);
        AtomicInteger executeCount = new AtomicInteger(0);
        AtomicInteger successCount = new AtomicInteger(0);

        for (int i = 0; i < threads; i++) {
            executor.submit(() -> {
                try {
                    startLatch.await();
                    String result = singleFlight.execute("concurrent-key", Duration.ofSeconds(5), () -> {
                        executeCount.incrementAndGet();
                        Thread.sleep(100); // Simulate slow db
                        return "shared-result";
                    });
                    if ("shared-result".equals(result)) {
                        successCount.incrementAndGet();
                    }
                } catch (Exception ignored) {
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        doneLatch.await(10, TimeUnit.SECONDS);
        executor.shutdown();

        assertEquals(1, executeCount.get(), "Loader should be executed exactly once");
        assertEquals(threads, successCount.get(), "All threads should get the same shared result");
    }

    @Test
    void testExecute_ConcurrentCalls_ExceptionPropagation() throws InterruptedException {
        int threads = 10;
        ExecutorService executor = Executors.newFixedThreadPool(threads);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(threads);
        AtomicInteger exceptionCount = new AtomicInteger(0);

        for (int i = 0; i < threads; i++) {
            executor.submit(() -> {
                try {
                    startLatch.await();
                    singleFlight.execute("fail-key", Duration.ofSeconds(5), () -> {
                        try {
                            Thread.sleep(50);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                        throw new RuntimeException("DB Error");
                    });
                } catch (RuntimeException e) {
                    if ("DB Error".equals(e.getMessage())) {
                        exceptionCount.incrementAndGet();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        doneLatch.await(10, TimeUnit.SECONDS);
        executor.shutdown();

        assertEquals(threads, exceptionCount.get(), "All threads should receive the propagated exception");
    }

    @Test
    void testExecute_Timeout() {
        assertThrows(IllegalStateException.class, () -> {
            // First thread holds the lock and sleeps
            new Thread(() -> {
                try {
                    singleFlight.execute("timeout-key", Duration.ofSeconds(10), () -> {
                        Thread.sleep(2000);
                        return "ok";
                    });
                } catch (Exception ignored) {}
            }).start();

            try {
                Thread.sleep(100); // give it a head start
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }

            // Second thread waits but times out quickly
            singleFlight.execute("timeout-key", Duration.ofMillis(10), () -> "fail");
        });
    }

    @Test
    void testExecute_DeadlockPrevention() {
        assertThrows(IllegalStateException.class, () -> {
            singleFlight.execute("recursive-key", Duration.ofSeconds(1), () -> {
                // Same thread trying to acquire the same key
                return singleFlight.execute("recursive-key", Duration.ofSeconds(1), () -> "deadlock");
            });
        });
    }

    private static Object awaitPublishedFlight(ConcurrentHashMap<String, Object> flights, String key)
            throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        Object flight;
        while ((flight = flights.get(key)) == null && System.nanoTime() < deadline) {
            Thread.sleep(1);
        }
        assertNotNull(flight, "Flight was not published");
        return flight;
    }
}
