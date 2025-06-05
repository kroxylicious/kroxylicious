/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator;

import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

public class BootstrapServersAnnotation {

    public static final String BOOTSTRAP_SERVERS_ANNOTATION_KEY = "kroxylicious.io/bootstrap-servers";

    private BootstrapServersAnnotation() {
    }

    public record BootstrapServer(String clusterName, String ingressName, String bootstrapServers) {
        public BootstrapServer {
            Objects.requireNonNull(clusterName);
            Objects.requireNonNull(ingressName);
            Objects.requireNonNull(bootstrapServers);
        }
    }

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static String toAnnotation(Set<BootstrapServer> bootstrapServers) {
        List<BootstrapServer> list = bootstrapServers.stream().sorted(Comparator.comparing(BootstrapServer::clusterName).thenComparing(BootstrapServer::ingressName))
                .toList();
        try {
            return OBJECT_MAPPER.writeValueAsString(list);
        }
        catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    public static Set<BootstrapServer> fromAnnotation(String bootstrapServers) {
        try {
            return OBJECT_MAPPER.readValue(bootstrapServers, new TypeReference<>() {
            });
        }
        catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
