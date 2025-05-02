/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.api.common.tls;

import java.util.List;

/**
 * TLS Protocol controls.
 */
@com.fasterxml.jackson.annotation.JsonInclude(com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL)
@com.fasterxml.jackson.annotation.JsonPropertyOrder({ "allowed", "denied" })
@com.fasterxml.jackson.databind.annotation.JsonDeserialize(using = com.fasterxml.jackson.databind.JsonDeserializer.None.class)
@javax.annotation.processing.Generated("io.fabric8.java.generator.CRGeneratorRunner")
@lombok.ToString()
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
public class Protocols implements io.fabric8.kubernetes.api.builder.Editable<ProtocolsBuilder>, io.fabric8.kubernetes.api.model.KubernetesResource {

    @Override
    public ProtocolsBuilder edit() {
        return new ProtocolsBuilder(this);
    }

    /**
     * The allowed TLS protocols, ordered by preference.
     * If not specified this defaults to the TLS protocols of the proxy JVM's default SSL context.
     */
    @com.fasterxml.jackson.annotation.JsonProperty("allowed")
    @com.fasterxml.jackson.annotation.JsonPropertyDescription("The allowed TLS protocols, ordered by preference.\nIf not specified this defaults to the TLS protocols of the proxy JVM's default SSL context.\n")
    @com.fasterxml.jackson.annotation.JsonSetter(nulls = com.fasterxml.jackson.annotation.Nulls.SKIP)
    private java.util.List<String> allowed;

    /**
     * Ordered list of allowed protocols.
     *
     * @return list of protocols.
     */
    public List<String> getAllowed() {
        return allowed;
    }

    /**
     * Sets the ordered list of allowed protocols.
     *
     * @param allowed list of protocols.
     */
    public void setAllowed(List<String> allowed) {
        this.allowed = allowed;
    }

    /**
     * The denied TLS protocols.
     * If not specified this defaults to the empty list.
     */
    @com.fasterxml.jackson.annotation.JsonProperty("denied")
    @com.fasterxml.jackson.annotation.JsonPropertyDescription("The denied TLS protocols.\nIf not specified this defaults to the empty list.\n")
    @com.fasterxml.jackson.annotation.JsonSetter(nulls = com.fasterxml.jackson.annotation.Nulls.SKIP)
    private List<String> denied = io.fabric8.kubernetes.client.utils.Serialization.unmarshal("[]", java.util.List.class);

    /**
     *  List of denied protocols.
     *
     * @return list of protocols.
     */
    public List<String> getDenied() {
        return denied;
    }

    /**
     * Sets the list of denied protocols.
     *
     * @param denied list of protocols.
     */
    public void setDenied(List<String> denied) {
        this.denied = denied;
    }
}
