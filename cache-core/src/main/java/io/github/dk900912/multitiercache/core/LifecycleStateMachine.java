package io.github.dk900912.multitiercache.core;

import java.util.concurrent.atomic.AtomicReference;

/**
 * Shared lifecycle state machine for internal cache modules.
 *
 * @author dukui
 */
final class LifecycleStateMachine {

    private final String ownerName;
    private final AtomicReference<State> state = new AtomicReference<>(State.NEW);

    LifecycleStateMachine(String ownerName) {
        this.ownerName = ownerName;
    }

    boolean beginBootstrap() {
        while (true) {
            State current = state.get();
            if (current == State.STARTED || current == State.STARTING) {
                return false;
            }
            if (current == State.SHUTDOWN) {
                throw new IllegalStateException(ownerName + " has already been shut down");
            }
            if (state.compareAndSet(State.NEW, State.STARTING)) {
                return true;
            }
        }
    }

    void markStarted() {
        state.set(State.STARTED);
    }

    void markBootstrapFailed() {
        state.set(State.NEW);
    }

    boolean beginShutdown() {
        return state.getAndSet(State.SHUTDOWN) != State.SHUTDOWN;
    }

    enum State {
        NEW,
        STARTING,
        STARTED,
        SHUTDOWN
    }
}
