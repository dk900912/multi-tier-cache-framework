package io.github.dk900912.multitiercache.core;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Implementation of the SingleFlight pattern.
 * <p>
 * Ensures that only one concurrent request is executed for a given key, while other
 * concurrent requests wait for the result. This effectively prevents cache breakdown
 * under high concurrency.
 * </p>
 *
 * @author dukui
 */
public final class SingleFlight {

    private final ConcurrentHashMap<String, Flight> flights = new ConcurrentHashMap<>();

    <T> T execute(String key, Duration timeout, FlightLoader<T> loader) {
        Objects.requireNonNull(key, "SingleFlight key cannot be null");
        Objects.requireNonNull(timeout, "SingleFlight timeout cannot be null");
        Objects.requireNonNull(loader, "SingleFlight loader cannot be null");

        Flight newFlight = new Flight(new CompletableFuture<>(), Thread.currentThread().threadId());
        Flight existingFlight = flights.putIfAbsent(key, newFlight);
        if (existingFlight != null) {
            return await(key, existingFlight, timeout);
        }

        try {
            T value = loader.load();
            newFlight.result().complete(value);
            return value;
        } catch (RuntimeException | Error e) {
            newFlight.result().completeExceptionally(e);
            throw e;
        } catch (Exception e) {
            IllegalStateException wrapped = new IllegalStateException("SingleFlight loader failed for key " + key, e);
            newFlight.result().completeExceptionally(wrapped);
            throw wrapped;
        } finally {
            flights.remove(key, newFlight);
        }
    }

    @SuppressWarnings("unchecked")
    private <T> T await(String key, Flight flight, Duration timeout) {
        if (flight.ownerThreadId() == Thread.currentThread().threadId()) {
            throw new IllegalStateException("Recursive cache load for key " + key + " would deadlock");
        }

        try {
            return (T) flight.result().get(toWaitNanos(timeout), TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted waiting for concurrent cache load for key " + key, e);
        } catch (TimeoutException e) {
            throw new IllegalStateException("Timed out waiting for concurrent cache load for key " + key, e);
        } catch (ExecutionException e) {
            throw propagateFailure(key, e.getCause());
        }
    }

    private RuntimeException propagateFailure(String key, Throwable cause) {
        if (cause instanceof RuntimeException runtimeException) {
            return runtimeException;
        }
        if (cause instanceof Error error) {
            throw error;
        }
        return new IllegalStateException("Concurrent cache load failed for key " + key, cause);
    }

    private long toWaitNanos(Duration timeout) {
        try {
            return timeout.toNanos();
        } catch (ArithmeticException e) {
            return Long.MAX_VALUE;
        }
    }

    @FunctionalInterface
    interface FlightLoader<T> {
        T load() throws Exception;
    }

    private record Flight(CompletableFuture<Object> result, long ownerThreadId) { }
}
