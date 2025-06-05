/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator;

import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.ObjectMetaFluent;

import io.kroxylicious.proxy.tag.VisibleForTesting;

/**
 * Used to annotate kubernetes Services with the client facing bootstrap servers that we expect clients will
 * connect with. We include (clusterName, ingressName, bootstrapServers) for each bootstrap address that the
 * client should eventually host.
 * <p>
 * We also include a version in the value of the annotation, in case we ever need to break compatibility but
 * continue reading older Service's metadata.
 * </p>
 */
public class BootstrapServersAnnotation {

    public static final String BOOTSTRAP_SERVERS_ANNOTATION_KEY = "kroxylicious.io/bootstrap-servers";

    private BootstrapServersAnnotation() {
    }

    /**
     * Adds a `kroxylicious.io/bootstrap-servers`annotation to the supplied metadata fluent
     * @param meta the Metadata fluent builder to add the annotation to
     * @param bootstrapServers the bootstrap servers to serialize into the annotation value
     */
    public static void annotate(ObjectMetaFluent<?> meta, Set<BootstrapServer> bootstrapServers) {
        if (bootstrapServers.isEmpty()) {
            return;
        }
        meta.addToAnnotations(BOOTSTRAP_SERVERS_ANNOTATION_KEY, toAnnotation(bootstrapServers));
    }

    /**
     * Read bootstrap servers from ObjectMeta, extracting them from an `kroxylicious.io/bootstrap-servers`
     * annotation if present.
     * @param meta the metadata to extract the bootstrap servers from
     * @return the bootstrap servers from the metadata if the annotation is present, else an empty Set
     */
    public static Set<BootstrapServer> bootstrapServersFrom(ObjectMeta meta) {
        if (!meta.getAnnotations().containsKey(BOOTSTRAP_SERVERS_ANNOTATION_KEY)) {
            return Set.of();
        }
        else {
            return fromAnnotation(meta.getAnnotations().get(BOOTSTRAP_SERVERS_ANNOTATION_KEY));
        }
    }

    @JsonPropertyOrder({ "clusterName", "ingressName", "bootstrapServers" })
    public record BootstrapServer(String clusterName, String ingressName, String bootstrapServers) {
        public BootstrapServer {
            Objects.requireNonNull(clusterName);
            Objects.requireNonNull(ingressName);
            Objects.requireNonNull(bootstrapServers);
        }
    }

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @VisibleForTesting
    @JsonPropertyOrder({ "version", "bootstrapServers" })
    record Wrapper(String version, List<BootstrapServer> bootstrapServers) {
        Wrapper {
            Objects.requireNonNull(version);
            Objects.requireNonNull(bootstrapServers);
        }
    }

    private static String toAnnotation(Set<BootstrapServer> bootstrapServers) {
        List<BootstrapServer> list = bootstrapServers.stream()
                .sorted(Comparator.comparing(BootstrapServer::clusterName).thenComparing(BootstrapServer::ingressName).thenComparing(BootstrapServer::bootstrapServers))
                .toList();
        Wrapper wrapper = new Wrapper("0.13.0", list);
        try {
            return OBJECT_MAPPER.writeValueAsString(wrapper);
        }
        catch (JsonProcessingException e) {
            throw new AnnotationSerializationException(e);
        }
    }

    private static Set<BootstrapServer> fromAnnotation(String bootstrapServers) {
        try {
            Wrapper wrapper = OBJECT_MAPPER.readValue(bootstrapServers, Wrapper.class);
            return new HashSet<>(wrapper.bootstrapServers());
        }
        catch (JsonProcessingException e) {
            throw new AnnotationSerializationException(e);
        }
    }
}
