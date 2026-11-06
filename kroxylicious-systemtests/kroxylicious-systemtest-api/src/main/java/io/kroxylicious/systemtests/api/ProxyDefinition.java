/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.api;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * A deployment-agnostic description of the desired proxy configuration.
 * No knowledge of namespaces, CRD templates, or deployment mechanism.
 * <p>
 * Use {@link #builder()} to construct instances.
 */
public final class ProxyDefinition {

    private final KafkaClusterHandle upstream;
    private final List<FilterSpec> filters;
    private final TlsSpec tls;
    private final TlsSpec downstreamTls;
    private final int replicas;

    private ProxyDefinition(Builder builder) {
        this.upstream = Objects.requireNonNull(builder.upstream, "builder.upstream must not be null");
        this.filters = List.copyOf(builder.filters);
        this.tls = builder.tls;
        this.downstreamTls = builder.downstreamTls;
        this.replicas = builder.replicas;
    }

    /**
     * Creates a new builder for a {@code ProxyDefinition}.
     *
     * @return a new builder
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * The upstream Kafka cluster this proxy fronts.
     *
     * @return upstream cluster handle
     */
    @NonNull
    public KafkaClusterHandle upstream() {
        return upstream;
    }

    /**
     * The filters to apply, in order.
     *
     * @return unmodifiable list of filter specs
     */
    @NonNull
    public List<FilterSpec> filters() {
        return filters;
    }

    /**
     * TLS configuration for the downstream (client-facing) side of the proxy,
     * or {@code null} if downstream TLS is not configured.
     *
     * @return downstream TLS, or {@code null}
     */
    @Nullable
    public TlsSpec downstreamTls() {
        return downstreamTls;
    }

    /**
     * TLS configuration for the upstream side of the proxy,
     * or {@code null} if TLS is not configured.
     *
     * @return TLS, or {@code null}
     */
    @Nullable
    public TlsSpec tls() {
        return tls;
    }

    /**
     * The number of proxy replicas to deploy.
     *
     * @return replica count (default 1)
     */
    public int replicas() {
        return replicas;
    }

    /**
     * Builder for {@link ProxyDefinition}.
     */
    public static final class Builder {

        private KafkaClusterHandle upstream;
        private final List<FilterSpec> filters = new ArrayList<>();
        private TlsSpec tls;
        private TlsSpec downstreamTls;
        private int replicas = 1;

        private Builder() {
        }

        /**
         * Sets the upstream Kafka cluster. Required.
         *
         * @param upstream handle to a provisioned Kafka cluster
         * @return this builder
         */
        public Builder withUpstream(@NonNull KafkaClusterHandle upstream) {
            this.upstream = Objects.requireNonNull(upstream, "upstream must not be null");
            return this;
        }

        /**
         * Adds a filter to the proxy's filter chain.
         * Filters are applied in the order they are added.
         *
         * @param filter filter specification
         * @return this builder
         */
        public Builder withFilter(@NonNull FilterSpec filter) {
            this.filters.add(Objects.requireNonNull(filter, "filter must not be null"));
            return this;
        }

        /**
         * Adds multiple filters to the proxy's filter chain.
         *
         * @param filters filter specifications
         * @return this builder
         */
        public Builder withFilters(@NonNull List<FilterSpec> filters) {
            Objects.requireNonNull(filters, "filters must not be null");
            this.filters.addAll(filters);
            return this;
        }

        /**
         * Configures TLS on the downstream (client-facing) side of the proxy.
         *
         * @param downstreamTls TLS specification
         * @return this builder
         */
        public Builder withDownstreamTls(@NonNull TlsSpec downstreamTls) {
            this.downstreamTls = Objects.requireNonNull(downstreamTls, "downstreamTls must not be null");
            return this;
        }

        /**
         * Configures TLS on the upstream side of the proxy.
         *
         * @param tls TLS specification
         * @return this builder
         */
        public Builder withTls(@NonNull TlsSpec tls) {
            this.tls = Objects.requireNonNull(tls, "tls must not be null");
            return this;
        }

        /**
         * Sets the number of proxy replicas. Defaults to 1.
         *
         * @param replicas replica count (must be positive)
         * @return this builder
         * @throws IllegalArgumentException if replicas is not positive
         */
        public Builder withReplicas(int replicas) {
            if (replicas < 1) {
                throw new IllegalArgumentException("replicas must be positive, got " + replicas);
            }
            this.replicas = replicas;
            return this;
        }

        /**
         * Builds the {@link ProxyDefinition}.
         *
         * @return a new proxy definition
         * @throws NullPointerException if upstream was not set
         */
        public ProxyDefinition build() {
            return new ProxyDefinition(this);
        }
    }
}
