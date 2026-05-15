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
        transition(State.STARTING, State.STARTED, "markStarted");
    }

    void markBootstrapFailed() {
        transition(State.STARTING, State.NEW, "markBootstrapFailed");
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

    private void transition(State expected, State target, String action) {
        State current = state.get();
        if (!state.compareAndSet(expected, target)) {
            throw new IllegalStateException(ownerName + " cannot " + action
                    + " from state " + current + "; expected " + expected);
        }
    }
}
