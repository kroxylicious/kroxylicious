/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.installation.kroxylicious;

import java.util.ArrayList;
import java.util.List;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProtocolFilter;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyIngress;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaService;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaCluster;
import io.kroxylicious.kubernetes.api.v1alpha1.kafkaservicespec.Tls;

public class KroxyliciousBuilder {
    private KafkaProxy kafkaProxy;
    private KafkaProxyIngress kafkaProxyIngress;
    private final List<KafkaProtocolFilter> kafkaProtocolFilters = new ArrayList<>();
    private KafkaService kafkaService;
    private VirtualKafkaCluster virtualKafkaCluster;
    private Tls tls;
    private Tls downstreamTls;
    private String namespace;

    /**
     * Select the namespace to be used for Kroxylicious instance
     *
     * @param namespace the namespace
     * @return the builder
     */
    public KroxyliciousBuilder withNamespace(String namespace) {
        this.namespace = namespace;
        return this;
    }

    /**
     * Select the TLS configuration to be used for Kroxylicious instance
     *
     * @param tls the tls object
     * @return the builder
     */
    public KroxyliciousBuilder withTls(Tls tls) {
        this.tls = tls;
        return this;
    }

    /**
     * Select the Downstream TLS configuration to be used for Kroxylicious instance
     *
     * @param tls the tls object
     * @return the builder
     */
    public KroxyliciousBuilder withDownstreamTls(Tls tls) {
        this.downstreamTls = tls;
        return this;
    }

    /**
     * Select the Kafka Proxy to be used for Kroxylicious instance
     *
     * @param kafkaProxy the Kafka proxy
     * @return the builder
     */
    public KroxyliciousBuilder withKafkaProxy(KafkaProxy kafkaProxy) {
        this.kafkaProxy = kafkaProxy;
        return this;
    }

    /**
     * Select the Kafka Proxy Ingress to be used for Kroxylicious instance
     *
     * @param kafkaProxyIngress the Kafka proxy ingress
     * @return the builder
     */
    public KroxyliciousBuilder withKafkaProxyIngress(KafkaProxyIngress kafkaProxyIngress) {
        this.kafkaProxyIngress = kafkaProxyIngress;
        return this;
    }

    /**
     * Select the Kafka Service to be used for Kroxylicious instance
     *
     * @param kafkaService the Kafka Service
     * @return the builder
     */
    public KroxyliciousBuilder withKafkaService(KafkaService kafkaService) {
        this.kafkaService = kafkaService;
        return this;
    }

    /**
     * Select the Virtual Kafka Cluster to be used for Kroxylicious instance
     *
     * @param virtualKafkaCluster the virtual Kafka cluster
     * @return the builder
     */
    public KroxyliciousBuilder withVirtualKafkaCluster(VirtualKafkaCluster virtualKafkaCluster) {
        this.virtualKafkaCluster = virtualKafkaCluster;
        return this;
    }

    /**
     * Adds a Kafka Protocol Filter to be used for Kroxylicious instance
     *
     * @param kafkaProtocolFilter the Kafka protocol filter
     * @return the builder
     */
    public KroxyliciousBuilder addKafkaProtocolFilter(KafkaProtocolFilter kafkaProtocolFilter) {
        this.kafkaProtocolFilters.add(kafkaProtocolFilter);
        return this;
    }

    /**
     * Select the Kafka Protocol Filters to be used for Kroxylicious instance
     *
     * @param kafkaProtocolFilters the Kafka protocol filters
     * @return the builder
     */
    public KroxyliciousBuilder withKafkaProtocolFilters(List<KafkaProtocolFilter> kafkaProtocolFilters) {
        this.kafkaProtocolFilters.addAll(kafkaProtocolFilters);
        return this;
    }

    /**
     * Build the kroxylicious instance
     *
     * @return the builder
     */
    public Kroxylicious build() {
        Kroxylicious buildable = new Kroxylicious();
        buildable.setNamespace(this.namespace);
        buildable.setTls(this.tls);
        buildable.setDownstreamTls(this.downstreamTls);
        buildable.setKafkaProtocolFilters(this.kafkaProtocolFilters);
        buildable.setKafkaProxy(this.kafkaProxy);
        buildable.setKafkaProxyIngress(this.kafkaProxyIngress);
        buildable.setKafkaService(this.kafkaService);
        buildable.setVirtualKafkaCluster(this.virtualKafkaCluster);
        return buildable;
    }
}
