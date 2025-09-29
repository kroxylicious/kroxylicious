package io.kroxylicious.kubernetes.api.common;

import java.util.Comparator;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonUnwrapped;

import io.fabric8.kubernetes.api.model.KubernetesResource;

@com.fasterxml.jackson.annotation.JsonInclude(com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL)
@com.fasterxml.jackson.annotation.JsonPropertyOrder({ "strimziKafkaRef", "listener" })
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
public class StrimziKafkaRef
        implements io.fabric8.kubernetes.api.builder.Editable<StrimziKafkaRefBuilder>,
        KubernetesResource, Comparable<StrimziKafkaRef> {

    private static final Comparator<StrimziKafkaRef> COMPARATOR = Comparator
            .<StrimziKafkaRef, AnyLocalRef> comparing(StrimziKafkaRef::getRef, Comparator.nullsLast(AnyLocalRef::compareTo))
            .thenComparing(StrimziKafkaRef::getListenerName, Comparator.nullsLast(String::compareTo));

    @Override
    public StrimziKafkaRefBuilder edit() {
        return new StrimziKafkaRefBuilder(this);
    }

    @JsonUnwrapped
    @io.fabric8.generator.annotation.Required()
    private AnyLocalRef ref;

    public AnyLocalRef getRef() {
        return ref;
    }

    public void setRef(AnyLocalRef strimziKafkaRef) {
        this.ref = ref;
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
        return this.getClass() + "(strimziKafkaRef=" + this.getRef() + ", listenerName=" + this.getListenerName() + ")";
    }

    @Override
    @SuppressWarnings("unchecked")
    public final boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof StrimziKafkaRef)) {
            return false;
        }
        StrimziKafkaRef other = (StrimziKafkaRef) obj;
        return Objects.equals(getRef(), other.getRef())
                && Objects.equals(getListenerName(), other.getListenerName());

    }

    @Override
    public int compareTo(StrimziKafkaRef o) {
        return COMPARATOR.compare(this, o);
    }
}
