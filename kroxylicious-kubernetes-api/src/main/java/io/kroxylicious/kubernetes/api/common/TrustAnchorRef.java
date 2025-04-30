/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.api.common;

import java.util.Comparator;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonUnwrapped;

import io.fabric8.kubernetes.api.model.KubernetesResource;

/**
 * A reference, used in a kubernetes resource, to a resource containing trust anchor(s).
 */
@com.fasterxml.jackson.annotation.JsonInclude(com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL)
@com.fasterxml.jackson.annotation.JsonPropertyOrder({ "ref", "key" })
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
public class TrustAnchorRef
        implements io.fabric8.kubernetes.api.builder.Editable<TrustAnchorRefBuilder>,
        KubernetesResource, Comparable<TrustAnchorRef> {

    private static final Comparator<TrustAnchorRef> COMPARATOR = Comparator
            .<TrustAnchorRef, AnyLocalRef> comparing(TrustAnchorRef::getRef, Comparator.nullsLast(AnyLocalRef::compareTo))
            .thenComparing(TrustAnchorRef::getKey, Comparator.nullsLast(String::compareTo));

    @Override
    public TrustAnchorRefBuilder edit() {
        return new TrustAnchorRefBuilder(this);
    }

    @JsonUnwrapped
    @io.fabric8.generator.annotation.Required()
    private AnyLocalRef ref;

    public AnyLocalRef getRef() {
        return ref;
    }

    public void setRef(AnyLocalRef ref) {
        this.ref = ref;
    }

    @com.fasterxml.jackson.annotation.JsonProperty("key")
    @io.fabric8.generator.annotation.Required()
    @com.fasterxml.jackson.annotation.JsonSetter(nulls = com.fasterxml.jackson.annotation.Nulls.SKIP)
    private String key;

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String toString() {
        return this.getClass() + "(ref=" + this.getRef() + ", key=" + this.getKey() + ")";
    }

    @Override
    public final int hashCode() {
        return Objects.hash(getRef(), getKey());
    }

    @Override
    @SuppressWarnings("unchecked")
    public final boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof TrustAnchorRef)) {
            return false;
        }
        TrustAnchorRef other = (TrustAnchorRef) obj;
        return Objects.equals(getRef(), other.getRef())
                && Objects.equals(getKey(), other.getKey());

    }

    @Override
    public int compareTo(TrustAnchorRef o) {
        return COMPARATOR.compare(this, o);
    }
}
