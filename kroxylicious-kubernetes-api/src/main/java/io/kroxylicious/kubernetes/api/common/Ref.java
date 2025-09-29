package io.kroxylicious.kubernetes.api.common;

import java.util.Comparator;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonUnwrapped;

import io.fabric8.kubernetes.api.model.KubernetesResource;

@com.fasterxml.jackson.annotation.JsonInclude(com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL)
@com.fasterxml.jackson.annotation.JsonPropertyOrder({ "ref", "listener" })
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
public class Ref
        implements io.fabric8.kubernetes.api.builder.Editable<RefBuilder>,
        KubernetesResource, Comparable<Ref> {

    private static final Comparator<Ref> COMPARATOR = Comparator
            .<Ref, AnyLocalRef> comparing(Ref::getStrimziKafkaRef, Comparator.nullsLast(AnyLocalRef::compareTo))
            .thenComparing(Ref::getListenerName, Comparator.nullsLast(String::compareTo));

    @Override
    public RefBuilder edit() {
        return new RefBuilder(this);
    }

    @JsonUnwrapped
    @io.fabric8.generator.annotation.Required()
    private AnyLocalRef strimziKafkaRef;

    public AnyLocalRef getStrimziKafkaRef() {
        return strimziKafkaRef;
    }

    public void setStrimziKafkaRef(AnyLocalRef strimziKafkaRef) {
        this.strimziKafkaRef = strimziKafkaRef;
    }

    @com.fasterxml.jackson.annotation.JsonProperty("listenerName")
    @io.fabric8.generator.annotation.Required()
    @com.fasterxml.jackson.annotation.JsonSetter(nulls = com.fasterxml.jackson.annotation.Nulls.SKIP)
    private String listenerName;

    public String getListenerName() {
        return listenerName;
    }

    public void setListenerName(String listenerName) {
        this.listenerName = listenerName;
    }

    private String name;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    private String namespace;

    public String getNamespace() {
        return namespace;
    }

    public void setNamespace(String namespace) {
        this.namespace = namespace;
    }

    public String toString() {
        return this.getClass() + "(ref=" + this.getStrimziKafkaRef() + ", key=" + this.getListenerName() + ")";
    }

    @Override
    @SuppressWarnings("unchecked")
    public final boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof Ref)) {
            return false;
        }
        Ref other = (Ref) obj;
        return Objects.equals(getStrimziKafkaRef(), other.getStrimziKafkaRef())
                && Objects.equals(getListenerName(), other.getListenerName());

    }

    @Override
    public int compareTo(Ref o) {
        return COMPARATOR.compare(this, o);
    }
}
