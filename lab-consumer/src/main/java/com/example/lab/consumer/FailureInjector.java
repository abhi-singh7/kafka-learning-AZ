package com.example.lab.consumer;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Test hooks, both driven from REST so failures are observable on demand:
 * <ul>
 *   <li>a configurable percentage of processing attempts throw (POST /lab/inject-failures)
 *       — this is what exercises the retry → backoff → quarantine path;</li>
 *   <li>a forced side-effect failure switch for the transactional lane — makes the ack-a/
 *       ack-b transaction roll back so you can watch the rollback mode behave.</li>
 * </ul>
 */
public class FailureInjector {

    private final AtomicInteger percentage = new AtomicInteger(0);
    private final AtomicBoolean failTxnSideEffect = new AtomicBoolean(false);

    public void inject(int pct, String lane) {
        percentage.set(Math.max(0, Math.min(100, pct)));
    }

    public int percentage() {
        return percentage.get();
    }

    /** Throws when the current roll lands inside the injected failure window. */
    public void maybeThrow(String lane) {
        int pct = percentage.get();
        if (pct > 0 && ThreadLocalRandom.current().nextInt(100) < pct) {
            throw new IllegalStateException("injected processing failure on lane " + lane);
        }
    }

    /** Arm/disarm the forced txn side-effect failure (POST /lab/inject-failures). */
    public void setFailTxnSideEffect(boolean fail) {
        failTxnSideEffect.set(fail);
    }

    public boolean failTxnSideEffect() {
        return failTxnSideEffect.get();
    }
}
