/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.admin.reload;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Result of a configuration reload operation containing detailed information
 * about the reload outcome, changes applied, and timing metrics.
 * Uses the Builder pattern for flexible construction.
 */
public class ReloadResult {

    private final boolean success;
    private final String message;
    private final int clustersModified;
    private final int clustersAdded;
    private final int clustersRemoved;
    private final Instant timestamp;
    private final Duration duration;

    private ReloadResult(Builder builder) {
        this.success = builder.success;
        this.message = Objects.requireNonNull(builder.message, "message cannot be null");
        this.clustersModified = builder.clustersModified;
        this.clustersAdded = builder.clustersAdded;
        this.clustersRemoved = builder.clustersRemoved;
        this.timestamp = builder.timestamp != null ? builder.timestamp : Instant.now();
        this.duration = builder.duration;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static ReloadResult success() {
        return builder().success(true).message("Reload successful").build();
    }

    public static ReloadResult failure(String message) {
        return builder().success(false).message(message).build();
    }

    public boolean isSuccess() {
        return success;
    }

    public String getMessage() {
        return message;
    }

    public int getClustersModified() {
        return clustersModified;
    }

    public int getClustersAdded() {
        return clustersAdded;
    }

    public int getClustersRemoved() {
        return clustersRemoved;
    }

    public Instant getTimestamp() {
        return timestamp;
    }

    @Nullable
    public Duration getDuration() {
        return duration;
    }

    public int getTotalClustersChanged() {
        return clustersModified + clustersAdded + clustersRemoved;
    }

    public static class Builder {
        private boolean success;
        private String message;
        private int clustersModified;
        private int clustersAdded;
        private int clustersRemoved;
        private Instant timestamp;
        private Duration duration;

        public Builder success(boolean success) {
            this.success = success;
            return this;
        }

        public Builder message(String message) {
            this.message = message;
            return this;
        }

        public Builder clustersModified(int count) {
            this.clustersModified = count;
            return this;
        }

        public Builder clustersAdded(int count) {
            this.clustersAdded = count;
            return this;
        }

        public Builder clustersRemoved(int count) {
            this.clustersRemoved = count;
            return this;
        }

        public Builder timestamp(Instant timestamp) {
            this.timestamp = timestamp;
            return this;
        }

        public Builder duration(Duration duration) {
            this.duration = duration;
            return this;
        }

        public ReloadResult build() {
            return new ReloadResult(this);
        }
    }
}
