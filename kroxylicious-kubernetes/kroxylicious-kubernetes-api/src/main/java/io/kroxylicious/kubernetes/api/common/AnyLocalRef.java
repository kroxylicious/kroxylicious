/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.api.common;

/**
 * A generic reference, used in a Kubernetes resource, to some other Kubernetes resource
 * in the same namespace, where the {@code group} and {@code kind} of the target are
 * <em>not</em> known statically.
 *
 * <p>All three fields ({@code group}, {@code kind}, {@code name}) are serialized to JSON
 * and must be supplied by the user. When the target resource type <em>is</em> known at
 * compile time, prefer a statically-typed subclass such as {@link ProxyRef} or
 * {@link FilterRef} instead.</p>
 *
 * <p>Despite being a different Java class, an {@code AnyLocalRef} is considered equal to
 * any other {@link LocalRef} subclass instance that carries the same {@code group},
 * {@code kind} and {@code name} values. See {@link LocalRef} for details.</p>
 */
@com.fasterxml.jackson.annotation.JsonInclude(com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL)
@com.fasterxml.jackson.annotation.JsonPropertyOrder({ "group", "kind", "name" })
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
public class AnyLocalRef
        extends AbstractLocalRef
        implements io.fabric8.kubernetes.api.builder.Editable<AnyLocalRefBuilder> {

    @java.lang.Override
    public AnyLocalRefBuilder edit() {
        return new AnyLocalRefBuilder(this);
    }

}
