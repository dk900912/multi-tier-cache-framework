package io.github.dk900912.multitiercache.core;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LifecycleStateMachineTest {

    @Test
    void shouldTransitionFromNewToStarted() {
        LifecycleStateMachine stateMachine = new LifecycleStateMachine("TestLifecycle");

        assertTrue(stateMachine.beginBootstrap());
        assertDoesNotThrow(stateMachine::markStarted);
    }

    @Test
    void shouldReturnToNewWhenBootstrapFailsFromStarting() {
        LifecycleStateMachine stateMachine = new LifecycleStateMachine("TestLifecycle");

        assertTrue(stateMachine.beginBootstrap());
        assertDoesNotThrow(stateMachine::markBootstrapFailed);
        assertTrue(stateMachine.beginBootstrap());
    }

    @Test
    void shouldRejectInvalidMarkStartedTransition() {
        LifecycleStateMachine stateMachine = new LifecycleStateMachine("TestLifecycle");

        IllegalStateException exception = assertThrows(IllegalStateException.class, stateMachine::markStarted);
        assertEquals(
                "TestLifecycle cannot markStarted from state NEW; expected STARTING",
                exception.getMessage()
        );
    }

    @Test
    void shouldRejectInvalidMarkBootstrapFailedTransition() {
        LifecycleStateMachine stateMachine = new LifecycleStateMachine("TestLifecycle");
        stateMachine.beginShutdown();

        IllegalStateException exception = assertThrows(IllegalStateException.class, stateMachine::markBootstrapFailed);
        assertEquals(
                "TestLifecycle cannot markBootstrapFailed from state SHUTDOWN; expected STARTING",
                exception.getMessage()
        );
    }

    @Test
    void shouldRejectBootstrapAfterShutdown() {
        LifecycleStateMachine stateMachine = new LifecycleStateMachine("TestLifecycle");
        stateMachine.beginShutdown();

        IllegalStateException exception = assertThrows(IllegalStateException.class, stateMachine::beginBootstrap);
        assertEquals("TestLifecycle has already been shut down", exception.getMessage());
    }

    @Test
    void shouldShutdownOnlyOnce() {
        LifecycleStateMachine stateMachine = new LifecycleStateMachine("TestLifecycle");

        assertTrue(stateMachine.beginShutdown());
        assertFalse(stateMachine.beginShutdown());
    }
}
