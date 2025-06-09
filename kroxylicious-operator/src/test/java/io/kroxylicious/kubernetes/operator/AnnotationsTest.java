/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.ObjectMetaFluent;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceBuilder;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class AnnotationsTest {

    public static final String BOOTSTRAP_SERVERS = "a.kafka.com:123";

    @Test
    void annotateWithBootstrapServers() {
        ObjectMetaFluent<?> meta = new ObjectMetaFluent<>();
        Annotations.annotateWithBootstrapServers(meta, Set.of(new Annotations.BootstrapServer("a", "b", BOOTSTRAP_SERVERS)));
        String expectedValue = "{\"version\":\"0.13.0\",\"bootstrapServers\":[{\"clusterName\":\"a\",\"ingressName\":\"b\",\"bootstrapServers\":\"" + BOOTSTRAP_SERVERS
                + "\"}]}";
        assertThat(meta.getAnnotations()).containsEntry(Annotations.BOOTSTRAP_SERVERS_ANNOTATION_KEY, expectedValue);
    }

    @Test
    void annotateWithNullReferentChecksum() {
        ObjectMetaFluent<?> meta = new ObjectMetaFluent<>();
        assertThatThrownBy(() -> Annotations.annotateWithReferentChecksum(meta, null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void annotateHasMetadataWithNullReferentChecksum() {
        Service build = new ServiceBuilder().build();
        assertThatThrownBy(() -> Annotations.annotateWithReferentChecksum(build, null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void annotateNullMetaFluentWithReferentChecksum() {
        assertThatThrownBy(() -> Annotations.annotateWithReferentChecksum((ObjectMetaFluent<?>) null, "checksum"))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void annotateNullHasMetadataWithReferentChecksum() {
        assertThatThrownBy(() -> Annotations.annotateWithReferentChecksum((ObjectMetaFluent<?>) null, "checksum"))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void annotateWithReferentChecksum() {
        ObjectMetaFluent<?> meta = new ObjectMetaFluent<>();
        String checksum = "checksum";
        Annotations.annotateWithReferentChecksum(meta, checksum);
        assertThat(meta.getAnnotations()).containsEntry(Annotations.REFERENT_CHECKSUM_ANNOTATION_KEY, checksum);
    }

    @Test
    void annotateHasMetadataWithReferentChecksum() {
        String checksum = "checksum";
        HasMetadata resource = new ServiceBuilder().build();
        Annotations.annotateWithReferentChecksum(resource, checksum);
        assertThat(resource.getMetadata().getAnnotations()).containsEntry(Annotations.REFERENT_CHECKSUM_ANNOTATION_KEY, checksum);
    }

    @Test
    void annotateHasMetadataExistingAnnotationsWithReferentChecksum() {
        String checksum = "checksum";
        HasMetadata resource = new ServiceBuilder().withNewMetadata().withAnnotations(Map.of("a", "b")).endMetadata().build();
        Annotations.annotateWithReferentChecksum(resource, checksum);
        assertThat(resource.getMetadata().getAnnotations())
                .containsEntry(Annotations.REFERENT_CHECKSUM_ANNOTATION_KEY, checksum)
                .containsEntry("a", "b");
    }

    @Test
    void readReferentChecksumFromEmptyAnnotations() {
        Map<String, String> annotations = Map.of();
        HasMetadata resource = new ServiceBuilder().withNewMetadata().withAnnotations(annotations).endMetadata().build();
        Optional<String> referentChecksum = Annotations.readReferentChecksumFrom(resource);
        assertThat(referentChecksum).isEmpty();
    }

    @Test
    void readReferentChecksumFromNullAnnotations() {
        HasMetadata resource = new ServiceBuilder().withNewMetadata().withAnnotations(null).endMetadata().build();
        Optional<String> referentChecksum = Annotations.readReferentChecksumFrom(resource);
        assertThat(referentChecksum).isEmpty();
    }

    @Test
    void readReferentChecksumFromNullMetadata() {
        HasMetadata resource = new ServiceBuilder().withMetadata(null).build();
        Optional<String> referentChecksum = Annotations.readReferentChecksumFrom(resource);
        assertThat(referentChecksum).isEmpty();
    }

    @Test
    void readReferentChecksumFromAnnotatedResource() {
        String checksum = "abc";
        Map<String, String> annotations = Map.of(Annotations.REFERENT_CHECKSUM_ANNOTATION_KEY, checksum);
        HasMetadata resource = new ServiceBuilder().withNewMetadata().withAnnotations(annotations).endMetadata().build();
        Optional<String> referentChecksum = Annotations.readReferentChecksumFrom(resource);
        assertThat(referentChecksum).isPresent().contains(checksum);
    }

    @Test
    void annotateWithBootstrapServersEmptySet() {
        ObjectMetaFluent<?> meta = new ObjectMetaFluent<>();
        Annotations.annotateWithBootstrapServers(meta, Set.of());
        assertThat(meta.hasAnnotations()).isFalse();
    }

    @Test
    void readBootstrapServersFromEmptyMetadata() {
        HasMetadata resource = new ServiceBuilder().withNewMetadata().endMetadata().build();
        Set<Annotations.BootstrapServer> bootstrapServers = Annotations.readBootstrapServersFrom(resource);
        assertThat(bootstrapServers).isEmpty();
    }

    @Test
    void readBootstrapServersFromNullMetadata() {
        HasMetadata resource = new ServiceBuilder().build();
        Set<Annotations.BootstrapServer> bootstrapServers = Annotations.readBootstrapServersFrom(resource);
        assertThat(bootstrapServers).isEmpty();
    }

    @Test
    void readBootstrapServersFromAnnotationContainingBootstrapServers() {
        String value = "{\"version\":\"0.13.0\",\"bootstrapServers\":[{\"clusterName\":\"a\",\"ingressName\":\"b\",\"bootstrapServers\":\"" + BOOTSTRAP_SERVERS + "\"}]}";
        HasMetadata resource = new ServiceBuilder().withNewMetadata().withAnnotations(Map.of(Annotations.BOOTSTRAP_SERVERS_ANNOTATION_KEY, value)).endMetadata().build();
        Set<Annotations.BootstrapServer> bootstrapServers = Annotations.readBootstrapServersFrom(resource);
        assertThat(bootstrapServers).contains(new Annotations.BootstrapServer("a", "b", BOOTSTRAP_SERVERS));
    }

    @Test
    void annotateWithBootstrapServersOrderIsStable() throws JsonProcessingException {
        ObjectMetaFluent<?> meta = new ObjectMetaFluent<>();
        List<Annotations.BootstrapServer> bootstrapServersInExpectedOrder = new ArrayList<>();
        bootstrapServersInExpectedOrder.add(new Annotations.BootstrapServer("a", "a", "a"));
        bootstrapServersInExpectedOrder.add(new Annotations.BootstrapServer("a", "a", "b"));
        bootstrapServersInExpectedOrder.add(new Annotations.BootstrapServer("a", "b", "a"));
        bootstrapServersInExpectedOrder.add(new Annotations.BootstrapServer("a", "b", "b"));
        bootstrapServersInExpectedOrder.add(new Annotations.BootstrapServer("b", "a", "a"));
        bootstrapServersInExpectedOrder.add(new Annotations.BootstrapServer("b", "a", "b"));
        bootstrapServersInExpectedOrder.add(new Annotations.BootstrapServer("b", "b", "a"));
        bootstrapServersInExpectedOrder.add(new Annotations.BootstrapServer("b", "b", "b"));
        HashSet<Annotations.BootstrapServer> unsorted = new HashSet<>(bootstrapServersInExpectedOrder);
        Annotations.annotateWithBootstrapServers(meta, unsorted);
        assertThat(meta.getAnnotations()).containsKey(Annotations.BOOTSTRAP_SERVERS_ANNOTATION_KEY);
        String annotationValue = meta.getAnnotations().remove(Annotations.BOOTSTRAP_SERVERS_ANNOTATION_KEY);
        Annotations.Wrapper decoded = new ObjectMapper().readValue(annotationValue, Annotations.Wrapper.class);
        assertThat(decoded.bootstrapServers()).containsExactlyElementsOf(bootstrapServersInExpectedOrder);
    }
}
