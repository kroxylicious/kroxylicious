/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.admin.reload;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Manages reload state and maintains a history of recent reload operations.
 * Thread-safe implementation using atomic references and synchronized access
 * to the history buffer.
 */
public class ReloadStateManager {

    /**
     * Maximum number of reload results to keep in history.
     */
    private static final int MAX_HISTORY_SIZE = 10;

    private final AtomicReference<ReloadState> currentState;
    private final Deque<ReloadResult> reloadHistory;

    public ReloadStateManager() {
        this.currentState = new AtomicReference<>(ReloadState.IDLE);
        this.reloadHistory = new ArrayDeque<>(MAX_HISTORY_SIZE);
    }

    /**
     * Mark that a reload operation has started.
     */
    public void startReload() {
        currentState.set(ReloadState.IN_PROGRESS);
    }

    /**
     * Record a successful reload operation.
     */
    public void recordSuccess(ReloadResult result) {
        currentState.set(ReloadState.IDLE);
        addToHistory(result);
    }

    /**
     * Record a failed reload operation.
     */
    public void recordFailure(Throwable error) {
        currentState.set(ReloadState.IDLE);
        ReloadResult failureResult = ReloadResult.failure(error.getMessage());
        addToHistory(failureResult);
    }

    /**
     * Get the current reload state.
     */
    public ReloadState getCurrentState() {
        return currentState.get();
    }

    /**
     * Get the most recent reload result.
     */
    public Optional<ReloadResult> getLastResult() {
        synchronized (reloadHistory) {
            return reloadHistory.isEmpty() ?
                    Optional.empty() :
                    Optional.of(reloadHistory.peekLast());
        }
    }

    /**
     * Get all reload results in history (most recent last).
     */
    public Deque<ReloadResult> getHistory() {
        synchronized (reloadHistory) {
            return new ArrayDeque<>(reloadHistory);
        }
    }

    /**
     * Add a result to history, removing oldest if at capacity.
     */
    private void addToHistory(ReloadResult result) {
        synchronized (reloadHistory) {
            if (reloadHistory.size() >= MAX_HISTORY_SIZE) {
                reloadHistory.removeFirst();
            }
            reloadHistory.addLast(result);
        }
    }

    /**
     * Reload operation state.
     */
    public enum ReloadState {
        /**
         * No reload operation in progress.
         */
        IDLE,

        /**
         * A reload operation is currently in progress.
         */
        IN_PROGRESS
    }
}
