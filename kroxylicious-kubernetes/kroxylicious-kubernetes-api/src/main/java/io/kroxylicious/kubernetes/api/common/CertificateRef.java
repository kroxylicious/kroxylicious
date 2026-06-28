/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.api.common;

import io.fabric8.kubernetes.api.model.KubernetesResource;

/**
 * A reference, used in a Kubernetes resource, to a resource that provides a private key
 * and certificate (e.g. a Kubernetes {@code Secret} or a cert-manager {@code Certificate}).
 *
 * <p>This is a dynamically-typed ref: the target resource type is not fixed, so
 * {@code group}, {@code kind} and {@code name} are all serialized to JSON and must be
 * supplied by the user. This allows references to TLS credentials stored in different
 * resource types depending on the operator environment.</p>
 *
 * <p>Despite being a distinct Java class, a {@code CertificateRef} is considered equal to
 * any other {@link LocalRef} subclass instance (e.g. {@link AnyLocalRef}) that carries the
 * same {@code group}, {@code kind} and {@code name} values. See {@link LocalRef} for details.</p>
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
public class CertificateRef
        extends AbstractLocalRef
        implements io.fabric8.kubernetes.api.builder.Editable<CertificateRefBuilder>,
        KubernetesResource {

    @Override
    public CertificateRefBuilder edit() {
        return new CertificateRefBuilder(this);
    }
}
