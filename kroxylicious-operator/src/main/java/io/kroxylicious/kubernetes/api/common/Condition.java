/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.api.common;

import java.util.Objects;

import com.fasterxml.jackson.annotation.Nulls;

import io.fabric8.generator.annotation.Default;
import io.fabric8.generator.annotation.Required;

/**
 * A common Condition type, used in CR statuses.
 */
@com.fasterxml.jackson.annotation.JsonInclude(com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL)
@com.fasterxml.jackson.annotation.JsonPropertyOrder({ "observedGeneration", "type", "status", "lastTransitionTime", "reason", "message" })
@com.fasterxml.jackson.databind.annotation.JsonDeserialize(using = com.fasterxml.jackson.databind.JsonDeserializer.None.class)
@javax.annotation.processing.Generated("io.fabric8.java.generator.CRGeneratorRunner")
@lombok.EqualsAndHashCode()
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
public class Condition implements io.fabric8.kubernetes.api.builder.Editable<ConditionBuilder>, io.fabric8.kubernetes.api.model.KubernetesResource {

    public static final String REASON_INTERPOLATED_REFS_NOT_FOUND = "InterpolatedReferencedResourcesNotFound";
    public static final String REASON_REFS_NOT_FOUND = "ReferencedResourcesNotFound";
    public static final String REASON_TRANSITIVE_REFS_NOT_FOUND = "TransitivelyReferencedResourcesNotFound";
    public static final String REASON_INVALID = "Invalid";

    @Override
    public ConditionBuilder edit() {
        return new ConditionBuilder(this);
    }

    /**
     * lastTransitionTime is the last time the condition transitioned from one status to another.
     * This should be when the underlying condition changed.
     * If that is not known, then using the time when the API field changed is acceptable.
     */
    @com.fasterxml.jackson.annotation.JsonProperty("lastTransitionTime")
    @com.fasterxml.jackson.annotation.JsonPropertyDescription("lastTransitionTime is the last time the condition transitioned from one status to another. \nThis should be when the underlying condition changed. \nIf that is not known, then using the time when the API field changed is acceptable.\n")
    @com.fasterxml.jackson.annotation.JsonSetter(nulls = Nulls.FAIL)
    @com.fasterxml.jackson.annotation.JsonFormat(shape = com.fasterxml.jackson.annotation.JsonFormat.Shape.STRING)
    private java.time.Instant lastTransitionTime;

    public java.time.Instant getLastTransitionTime() {
        return lastTransitionTime;
    }

    public void setLastTransitionTime(java.time.Instant lastTransitionTime) {
        this.lastTransitionTime = Objects.requireNonNull(lastTransitionTime);
    }

    /**
     * message is a human readable message indicating details about the transition.
     * This may be an empty string.
     */
    @com.fasterxml.jackson.annotation.JsonProperty(value = "message", defaultValue = "")
    @com.fasterxml.jackson.annotation.JsonPropertyDescription("message is a human readable message indicating details about the transition. \nThis may be an empty string.\n")
    @com.fasterxml.jackson.annotation.JsonSetter(nulls = Nulls.FAIL, value = "")
    @Default("")
    private String message;

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = Objects.requireNonNull(message);
    }

    /**
     * observedGeneration represents the .metadata.generation that the condition was set based upon.
     * For instance, if .metadata.generation is currently 12, but the
     * .status.conditions[x].observedGeneration is 9, the condition is out of date with
     * respect to the current state of the instance.
     */
    @com.fasterxml.jackson.annotation.JsonProperty("observedGeneration")
    @com.fasterxml.jackson.annotation.JsonPropertyDescription("observedGeneration represents the .metadata.generation that the condition was set based upon. \nFor instance, if .metadata.generation is currently 12, but the \n.status.conditions[x].observedGeneration is 9, the condition is out of date with \nrespect to the current state of the instance.\n")
    @com.fasterxml.jackson.annotation.JsonSetter(nulls = Nulls.FAIL)
    private Long observedGeneration;

    public Long getObservedGeneration() {
        return observedGeneration;
    }

    public void setObservedGeneration(Long observedGeneration) {
        this.observedGeneration = Objects.requireNonNull(observedGeneration);
    }

    /**
     * reason contains a programmatic identifier indicating the reason for the condition's last transition.
     * Producers of specific condition types may define expected values and meanings for this field,
     * and whether the values are considered a guaranteed API.
     * The value should be a CamelCase string.
     * This field may not be empty.
     */
    @com.fasterxml.jackson.annotation.JsonProperty(value = "reason", defaultValue = "")
    @com.fasterxml.jackson.annotation.JsonPropertyDescription("reason contains a programmatic identifier indicating the reason for the condition's last transition. \nProducers of specific condition types may define expected values and meanings for this field, \nand whether the values are considered a guaranteed API. \nThe value should be a CamelCase string. \nThis field may not be empty.\n")
    @com.fasterxml.jackson.annotation.JsonSetter(nulls = Nulls.FAIL, value = "")
    @Required
    private String reason;

    public String getReason() {
        return reason;
    }

    public void setReason(String reason) {
        this.reason = Objects.requireNonNull(reason);
    }

    @Override
    public String toString() {
        return "Condition{" +
                "observedGeneration=" + observedGeneration +
                ", type=" + type +
                ", status=" + status +
                ", lastTransitionTime=" + lastTransitionTime +
                ", reason='" + reason + '\'' +
                ", message='" + message + '\'' +
                '}';
    }

    public enum Status {

        @com.fasterxml.jackson.annotation.JsonProperty("True")
        TRUE("True"),
        @com.fasterxml.jackson.annotation.JsonProperty("False")
        FALSE("False"),
        @com.fasterxml.jackson.annotation.JsonProperty("Unknown")
        UNKNOWN("Unknown");

        String value;

        Status(String value) {
            this.value = value;
        }

        @com.fasterxml.jackson.annotation.JsonValue()
        public String getValue() {
            return value;
        }
    }

    /**
     * status of the condition, one of True, False, Unknown.
     */
    @com.fasterxml.jackson.annotation.JsonProperty("status")
    @com.fasterxml.jackson.annotation.JsonPropertyDescription("status of the condition, one of True, False, Unknown.")
    @com.fasterxml.jackson.annotation.JsonSetter(nulls = com.fasterxml.jackson.annotation.Nulls.SKIP)
    private Status status;

    public Status getStatus() {
        return status;
    }

    public void setStatus(Status status) {
        this.status = status;
    }

    /**
     * type of condition in CamelCase or in foo.example.com/CamelCase.
     */
    @com.fasterxml.jackson.annotation.JsonProperty(value = "type")
    @com.fasterxml.jackson.annotation.JsonPropertyDescription("type of condition in CamelCase or in foo.example.com/CamelCase.")
    @com.fasterxml.jackson.annotation.JsonSetter(nulls = Nulls.FAIL)
    private Type type;

    public Type getType() {
        return type;
    }

    public void setType(Type type) {
        this.type = Objects.requireNonNull(type);
    }

    public enum Type {
        Ready("Ready"),
        ResolvedRefs("ResolvedRefs"),
        Accepted("Accepted");

        private final String value;

        Type(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }
    }

    public static boolean isResolvedRefsFalse(@NonNull Condition condition) {
        Objects.requireNonNull(condition);
        return Condition.Type.ResolvedRefs.equals(condition.getType())
                && Condition.Status.FALSE.equals(condition.getStatus());
    }
}