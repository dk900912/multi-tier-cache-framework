package io.github.dk900912.multitiercache.core;

import java.time.Duration;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Industrial-grade SingleFlight implementation.
 * <p>
 * Eliminates asymmetric timeout risks and prevents permanent line freezing.
 * Fully compatible with Java 21+ virtual threads.
 * </p>
 *
 * @author dukui
 */
public final class SingleFlight {

    private static final ThreadLocal<Set<String>> LOADING_KEYS = ThreadLocal.withInitial(HashSet::new);

    private final ConcurrentHashMap<String, Flight> flights = new ConcurrentHashMap<>();
    private final Executor executor;

    public SingleFlight() {
        this.executor = ForkJoinPool.commonPool();
    }

    public SingleFlight(Executor executor) {
        this.executor = Objects.requireNonNull(executor, "Executor cannot be null");
    }

    public <T> T execute(String key, Duration timeout, FlightLoader<T> loader) {
        Objects.requireNonNull(key, "SingleFlight key cannot be null");
        Objects.requireNonNull(timeout, "SingleFlight timeout cannot be null");
        Objects.requireNonNull(loader, "SingleFlight loader cannot be null");

        if (LOADING_KEYS.get().contains(key)) {
            throw recursiveLoadFailure(key);
        }

        // Publish the candidate before starting its task. Starting work from inside a
        // computeIfAbsent mapping function can let an eager executor recursively mutate
        // the same map entry before ConcurrentHashMap has finished publishing it.
        Flight candidate = new Flight(new CompletableFuture<>());
        Flight existing = flights.putIfAbsent(key, candidate);
        Flight flight;
        if (existing == null) {
            flight = candidate;
            try {
                // Execute separately so only the successfully published owner starts the loader.
                executor.execute(() -> runLoader(key, loader, candidate));
            } catch (RuntimeException | Error submissionFailure) {
                flights.remove(key, candidate);
                candidate.result().completeExceptionally(submissionFailure);
                throw submissionFailure;
            }
        } else {
            flight = existing;
        }

        // 2. Symmetrically await and retrieve the result for both owner and waiters
        try {
            @SuppressWarnings("unchecked")
            T result = (T) flight.result().get(toWaitNanos(timeout), TimeUnit.NANOSECONDS);
            return result;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while waiting for key: " + key, e);
        } catch (TimeoutException e) {
            flights.remove(key, flight);
            throw new IllegalStateException("Execution timed out after " + timeout.toMillis() + "ms for key: " + key, e);
        } catch (ExecutionException e) {
            throw propagateFailure(key, e.getCause());
        }
    }

    private <T> void runLoader(String key, FlightLoader<T> loader, Flight flight) {
        Set<String> loadingKeys = LOADING_KEYS.get();
        if (!loadingKeys.add(key)) {
            completeFlight(key, flight, null, recursiveLoadFailure(key));
            return;
        }

        Object value = null;
        Throwable failure = null;
        try {
            value = loader.load();
        } catch (Throwable e) {
            failure = e;
        } finally {
            loadingKeys.remove(key);
            if (loadingKeys.isEmpty()) {
                LOADING_KEYS.remove();
            }
        }
        completeFlight(key, flight, value, failure);
    }

    private void completeFlight(String key, Flight flight, Object value, Throwable failure) {
        // Remove first so a request arriving after completion never observes a stale result.
        // Conditional removal prevents a timed-out older owner from deleting a newer flight.
        flights.remove(key, flight);
        if (failure == null) {
            flight.result().complete(value);
        } else {
            flight.result().completeExceptionally(failure);
        }
    }

    private IllegalStateException recursiveLoadFailure(String key) {
        return new IllegalStateException("Recursive cache load detected for key '" + key + "' which would cause a deadlock.");
    }

    private RuntimeException propagateFailure(String key, Throwable cause) {
        if (cause instanceof RuntimeException runtimeException) {
            return runtimeException;
        }
        if (cause instanceof Error error) {
            throw error;
        }
        return new IllegalStateException("Load failed for key: " + key, cause);
    }

    private long toWaitNanos(Duration timeout) {
        try {
            return timeout.toNanos();
        } catch (ArithmeticException e) {
            return Long.MAX_VALUE;
        }
    }

    @FunctionalInterface
    public interface FlightLoader<T> {
        T load() throws Exception;
    }

    private record Flight(CompletableFuture<Object> result) { }
}
