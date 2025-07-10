/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.config;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import com.fasterxml.jackson.annotation.JsonProperty;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * A virtual cluster.
 *
 * @param name virtual cluster name
 * @param targetCluster the cluster being proxied
 * @param gateways virtual cluster gateways
 * @param logNetwork if true, network will be logged
 * @param logFrames if true, kafka rpcs will be logged
 * @param filters filers.
 */
@SuppressWarnings("java:S1123") // suppressing the spurious warning about missing @deprecated in javadoc. It is the field that is deprecated, not the class.
public record VirtualCluster(@NonNull @JsonProperty(required = true) String name,
                             @NonNull @JsonProperty(required = true) TargetCluster targetCluster,
                             @JsonProperty(required = true) List<VirtualClusterGateway> gateways,
                             boolean logNetwork,
                             boolean logFrames,
                             @Nullable List<String> filters) {

    private static final Pattern DNS_LABEL_PATTERN = Pattern.compile("^[a-z0-9]([-a-z0-9]*[a-z0-9])?$", Pattern.CASE_INSENSITIVE);

    /**
     * Name given to the gateway defined using the deprecated fields.
     */
    @Deprecated(forRemoval = true, since = "0.11.0")
    static final String DEFAULT_GATEWAY_NAME = "default";

    @SuppressWarnings({ "removal", "java:S2789" }) // S2789 - checking for null tls is the intent
    public VirtualCluster {
        Objects.requireNonNull(name);
        Objects.requireNonNull(targetCluster);
        if (!isDnsLabel(name)) {
            throw new IllegalConfigurationException(
                    "Virtual cluster name '" + name + "' is invalid. It must be less than 64 characters long and match pattern " + DNS_LABEL_PATTERN.pattern()
                            + " (case insensitive)");
        }
        if (gateways == null || gateways.isEmpty()) {
            throw new IllegalConfigurationException("no gateways configured for virtual cluster '" + name + "'");
        }
        if (gateways.stream().anyMatch(Objects::isNull)) {
            throw new IllegalConfigurationException("one or more gateways were null for virtual cluster '" + name + "'");
        }
        validateNoDuplicatedGatewayNames(gateways);
    }

    boolean isDnsLabel(String name) {
        if (name.length() > 63) {
            return false;
        }
        else {
            return DNS_LABEL_PATTERN.matcher(name).matches();
        }
    }

    private void validateNoDuplicatedGatewayNames(List<VirtualClusterGateway> gateways) {
        var names = gateways.stream()
                .map(VirtualClusterGateway::name)
                .toList();
        var duplicates = names.stream()
                .filter(i -> Collections.frequency(names, i) > 1)
                .collect(Collectors.toSet());
        if (!duplicates.isEmpty()) {
            throw new IllegalConfigurationException(
                    "Gateway names for a virtual cluster must be unique. The following gateway names are duplicated: [%s]".formatted(
                            String.join(", ", duplicates)));
        }
    }

}
