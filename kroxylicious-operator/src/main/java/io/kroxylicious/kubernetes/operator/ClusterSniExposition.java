/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.kubernetes.api.v1alpha1.kafkaproxyspec.Clusters;
import io.kroxylicious.kubernetes.api.v1alpha1.kafkaproxyspec.Listeners;

import edu.umd.cs.findbugs.annotations.NonNull;

import static java.util.function.Function.identity;

/**
 * Calculates how to expose a cluster via SNI.
 * For a listener + hostname combination we need to calculate the
 * services to expose and the corresponding SNI hostname
 */
public final class ClusterSniExposition {
    private final List<HostnameExposition> expositions;

    /**
     */
    private ClusterSniExposition(List<HostnameExposition> expositions) {
        this.expositions = expositions;
    }

    public static ClusterSniExposition planExposition(KafkaProxy proxy, Clusters cluster) {
        Map<String, Listeners> namedListeners = proxy.getSpec().getListeners().stream().collect(Collectors.toMap(Listeners::getName, identity()));
        List<HostnameExposition> expositions = cluster.getHostnames().stream().map(HostnamePattern::new).map(hostnamePattern -> {
            // todo add parentRefs to cluster and lookup listener by name
            Listeners listenerForHostname = namedListeners.values().stream().findFirst().get();
            Stream<String> brokerServiceNames = cluster.getAdvertisedBrokers().stream().map(hostnamePattern::resolveBrokerServiceName);
            Stream<String> bootstrapServiceNames = Stream.of(hostnamePattern.resolveBootstrapServiceName());
            Stream<String> allServices = Stream.concat(bootstrapServiceNames, brokerServiceNames);
            List<Service> serviceNames = allServices.map(name -> new Service(name, "ClusterIP")).toList();
            return new HostnameExposition(listenerForHostname, hostnamePattern.bootstrapHostname(), hostnamePattern.brokerPattern(), serviceNames);
        }).toList();
        return new ClusterSniExposition(expositions);
    }

    public List<HostnameExposition> getExpositions() {
        return expositions;
    }

    public record HostnameExposition(Listeners listener, String sniBootstrapHostname, String sniBrokerPattern, List<Service> services) {

    }

    public record Service(String name, String type) {}

    private record HostnamePattern(String hostname) {

        public static final String NODE_ID_PATTERN = "$(nodeId)";

        HostnamePattern {
            if (!hostname.contains("%") || hostname.indexOf("%") != hostname.lastIndexOf("%")) {
                throw new RuntimeException("host must contain a single '%'");
            }
            // todo, check that it contains the right namespace and no extra parts
            if (!hostname.endsWith("svc.cluster.local")) {
                throw new RuntimeException("host must end with 'svc.cluster.local'");
            }
            String servicePrefix = hostname.substring(0, hostname.indexOf("%"));
            if (servicePrefix.contains(".")) {
                throw new RuntimeException("% must be in the first host segment");
            }
        }

        public String bootstrapHostname() {
            return hostname.replace("%", "bootstrap");
        }

        public String brokerPattern() {
            return hostname.replace("%", "broker-" + NODE_ID_PATTERN);
        }

        public String resolveBrokerServiceName(Integer brokerId) {
            String fullyQualified = brokerPattern().replace(NODE_ID_PATTERN, brokerId.toString());
            // if hostname is xyz-broker-1.my-ns.svc.local.cluster service name should be xyz-broker-1
            return firstHostnameSection(fullyQualified);
        }

        public String resolveBootstrapServiceName() {
            String fullyQualified = bootstrapHostname();
            // if hostname is xyz-bootstrap.my-ns.svc.local.cluster service name should be xyz-bootstrap
            return firstHostnameSection(fullyQualified);
        }
    }

    private static @NonNull String firstHostnameSection(String fullyQualified) {
        return fullyQualified.substring(0, fullyQualified.indexOf("."));
    }
}
