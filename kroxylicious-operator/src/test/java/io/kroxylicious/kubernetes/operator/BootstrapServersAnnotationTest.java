/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.ObjectMetaFluent;

import static org.assertj.core.api.Assertions.assertThat;

class BootstrapServersAnnotationTest {

    public static final String BOOTSTRAP_SERVERS = "a.kafka.com:123";

    @Test
    void testAnnotate() {
        ObjectMetaFluent<?> meta = new ObjectMetaFluent<>();
        BootstrapServersAnnotation.annotate(meta, Set.of(new BootstrapServersAnnotation.BootstrapServer("a", "b", BOOTSTRAP_SERVERS)));
        String expectedValue = "{\"version\":\"0.13.0\",\"bootstrapServers\":[{\"clusterName\":\"a\",\"ingressName\":\"b\",\"bootstrapServers\":\"" + BOOTSTRAP_SERVERS
                + "\"}]}";
        assertThat(meta.getAnnotations()).containsEntry(BootstrapServersAnnotation.BOOTSTRAP_SERVERS_ANNOTATION_KEY, expectedValue);
    }

    @Test
    void testAnnotateEmptySet() {
        ObjectMetaFluent<?> meta = new ObjectMetaFluent<>();
        BootstrapServersAnnotation.annotate(meta, Set.of());
        assertThat(meta.hasAnnotations()).isFalse();
    }

    @Test
    void testReadAnnotationFromEmptyMetadata() {
        ObjectMeta meta = new ObjectMeta();
        Set<BootstrapServersAnnotation.BootstrapServer> bootstrapServers = BootstrapServersAnnotation.bootstrapServersFrom(meta);
        assertThat(bootstrapServers).isEmpty();
    }

    @Test
    void testReadAnnotationContainingBootstrapServers() {
        ObjectMeta meta = new ObjectMeta();
        String value = "{\"version\":\"0.13.0\",\"bootstrapServers\":[{\"clusterName\":\"a\",\"ingressName\":\"b\",\"bootstrapServers\":\"" + BOOTSTRAP_SERVERS + "\"}]}";
        meta.getAnnotations().put(BootstrapServersAnnotation.BOOTSTRAP_SERVERS_ANNOTATION_KEY, value);
        Set<BootstrapServersAnnotation.BootstrapServer> bootstrapServers = BootstrapServersAnnotation.bootstrapServersFrom(meta);
        assertThat(bootstrapServers).contains(new BootstrapServersAnnotation.BootstrapServer("a", "b", BOOTSTRAP_SERVERS));
    }

    @Test
    void testAnnotateOrderIsStable() throws JsonProcessingException {
        ObjectMetaFluent<?> meta = new ObjectMetaFluent<>();
        List<BootstrapServersAnnotation.BootstrapServer> bootstrapServersInExpectedOrder = new ArrayList<>();
        bootstrapServersInExpectedOrder.add(new BootstrapServersAnnotation.BootstrapServer("a", "a", "a"));
        bootstrapServersInExpectedOrder.add(new BootstrapServersAnnotation.BootstrapServer("a", "a", "b"));
        bootstrapServersInExpectedOrder.add(new BootstrapServersAnnotation.BootstrapServer("a", "b", "a"));
        bootstrapServersInExpectedOrder.add(new BootstrapServersAnnotation.BootstrapServer("a", "b", "b"));
        bootstrapServersInExpectedOrder.add(new BootstrapServersAnnotation.BootstrapServer("b", "a", "a"));
        bootstrapServersInExpectedOrder.add(new BootstrapServersAnnotation.BootstrapServer("b", "a", "b"));
        bootstrapServersInExpectedOrder.add(new BootstrapServersAnnotation.BootstrapServer("b", "b", "a"));
        bootstrapServersInExpectedOrder.add(new BootstrapServersAnnotation.BootstrapServer("b", "b", "b"));
        HashSet<BootstrapServersAnnotation.BootstrapServer> unsorted = new HashSet<>(bootstrapServersInExpectedOrder);
        BootstrapServersAnnotation.annotate(meta, unsorted);
        assertThat(meta.getAnnotations()).containsKey(BootstrapServersAnnotation.BOOTSTRAP_SERVERS_ANNOTATION_KEY);
        String annotationValue = meta.getAnnotations().remove(BootstrapServersAnnotation.BOOTSTRAP_SERVERS_ANNOTATION_KEY);
        BootstrapServersAnnotation.Wrapper decoded = new ObjectMapper().readValue(annotationValue, BootstrapServersAnnotation.Wrapper.class);
        assertThat(decoded.bootstrapServers()).containsExactlyElementsOf(bootstrapServersInExpectedOrder);
    }
}
