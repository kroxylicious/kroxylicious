/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.api.common;

import com.fasterxml.jackson.annotation.JsonIgnore;

import io.fabric8.kubernetes.api.model.KubernetesResource;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * A reference, used in a Kubernetes resource, to a {@link KafkaProxy} resource in the same namespace.
 *
 * <p>This is a statically-typed ref: {@code group} ({@code "kroxylicious.io"}) and
 * {@code kind} ({@code "KafkaProxy"}) are hardcoded constants and are not serialized
 * to JSON. Only {@code name} appears in the CRD schema.</p>
 *
 * <p>Despite being a distinct Java class, a {@code ProxyRef} is considered equal to any
 * other {@link LocalRef} subclass instance (e.g. {@link AnyLocalRef}) that carries the
 * same {@code group}, {@code kind} and {@code name} values. See {@link LocalRef} for details.</p>
 */
@com.fasterxml.jackson.annotation.JsonInclude(com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL)
@com.fasterxml.jackson.annotation.JsonPropertyOrder({ "name" })
@com.fasterxml.jackson.databind.annotation.JsonDeserialize(using = com.fasterxml.jackson.databind.JsonDeserializer.None.class)
@javax.annotation.processing.Generated("io.fabric8.java.generator.CRGeneratorRunner")
@lombok.ToString()
@io.sundr.builder.annotations.Buildable(editableEnabled = false, validationEnabled = false, generateBuilderPackage = false, builderPackage = "io.fabric8.kubernetes.api.builder", refs = {
        @io.sundr.builder.annotations.BuildableReference(io.fabric8.kubernetes.api.model.ObjectMeta.class),
        @io.sundr.builder.annotations.BuildableReference(io.fabric8.kubernetes.api.model.ObjectReference.class),
        @io.sundr.builder.annotations.BuildableReference(io.fabric8.kubernetes.api.model.LabelSelector.class),
        @io.sundr.builder.annotations.BuildableReference(io.fabric8.kubernetes.api.model.Container.class),
        @io.sundr.builder.annotations.BuildableReference(io.fabric8.kubernetes.api.model.EnvVar.class),
        @io.sundr.builder.annotations.BuildableReference(io.fabric8.kubernetes.api.model.ContainerPort.class),
        @io.sundr.builder.annotations.BuildableReference(io.fabric8.kubernetes.api.model.Volume.class),
        @io.sundr.builder.annotations.BuildableReference(io.fabric8.kubernetes.api.model.VolumeMount.class)
})
public class ProxyRef
        extends LocalRef<KafkaProxy>
        implements io.fabric8.kubernetes.api.builder.Editable<ProxyRefBuilder>,
        KubernetesResource {

    @Override
    public ProxyRefBuilder edit() {
        return new ProxyRefBuilder(this);
    }

    @JsonIgnore
    @NonNull
    @Override
    public String getGroup() {
        return "kroxylicious.io";
    }

    @JsonIgnore
    @NonNull
    @Override
    public String getKind() {
        return "KafkaProxy";
    }

    @com.fasterxml.jackson.annotation.JsonProperty("name")
    @io.fabric8.generator.annotation.Required()
    @com.fasterxml.jackson.annotation.JsonSetter(nulls = com.fasterxml.jackson.annotation.Nulls.SKIP)
    private String name;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}
