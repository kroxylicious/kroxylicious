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

    public KroxyliciousBuilder withNamespace(String namespace) {
        this.namespace = namespace;
        return this;
    }

    public KroxyliciousBuilder withTls(Tls tls) {
        this.tls = tls;
        return this;
    }

    public KroxyliciousBuilder withDownstreamTls(Tls tls) {
        this.downstreamTls = tls;
        return this;
    }

    public KroxyliciousBuilder withKafkaProxy(KafkaProxy kafkaProxy) {
        this.kafkaProxy = kafkaProxy;
        return this;
    }

    public KroxyliciousBuilder withKafkaProxyIngress(KafkaProxyIngress kafkaProxyIngress) {
        this.kafkaProxyIngress = kafkaProxyIngress;
        return this;
    }

    public KroxyliciousBuilder withKafkaService(KafkaService kafkaService) {
        this.kafkaService = kafkaService;
        return this;
    }

    public KroxyliciousBuilder withVirtualKafkaCluster(VirtualKafkaCluster virtualKafkaCluster) {
        this.virtualKafkaCluster = virtualKafkaCluster;
        return this;
    }

    public KroxyliciousBuilder addKafkaProtocolFilter(KafkaProtocolFilter kafkaProtocolFilter) {
        this.kafkaProtocolFilters.add(kafkaProtocolFilter);
        return this;
    }

    public KroxyliciousBuilder withKafkaProtocolFilters(List<KafkaProtocolFilter> kafkaProtocolFilters) {
        this.kafkaProtocolFilters.addAll(kafkaProtocolFilters);
        return this;
    }

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
