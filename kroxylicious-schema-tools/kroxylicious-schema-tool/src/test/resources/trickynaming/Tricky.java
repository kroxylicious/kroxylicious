/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package trickynaming;

/**
 * The Kubernetes CRD model
 */
@javax.annotation.processing.Generated("io.kroxylicious.tools.schema.compiler.CodeGen")
@com.fasterxml.jackson.annotation.JsonInclude(com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL)
@com.fasterxml.jackson.annotation.JsonPropertyOrder({ "apiVersion", "kind", "spec" })
@com.fasterxml.jackson.databind.annotation.JsonDeserialize(using = com.fasterxml.jackson.databind.JsonDeserializer.None.class)
public class Tricky {

    @edu.umd.cs.findbugs.annotations.NonNull()
    private java.lang.String apiVersion;

    @edu.umd.cs.findbugs.annotations.NonNull()
    private java.lang.String kind;

    @edu.umd.cs.findbugs.annotations.Nullable()
    private trickynaming.TrickySpec spec;

    /**
     * Nullary constructor (used for deserialization).
     */
    public Tricky() {
    }

    /**
     * Required properties constructor.
     * @param apiVersion The value of the {@code apiVersion} property. This is a required property.
     * @param kind The value of the {@code kind} property. This is a required property.
     */
    public Tricky(@edu.umd.cs.findbugs.annotations.NonNull java.lang.String apiVersion, @edu.umd.cs.findbugs.annotations.NonNull java.lang.String kind) {
        this.apiVersion = java.util.Objects.requireNonNull(apiVersion);
        this.kind = java.util.Objects.requireNonNull(kind);
    }

    /**
     * All properties constructor.
     * @param apiVersion The value of the {@code apiVersion} property. This is a required property.
     * @param kind The value of the {@code kind} property. This is a required property.
     * @param spec The value of the {@code spec} property. This is an optional property.
     */
    @com.fasterxml.jackson.annotation.JsonCreator()
    public Tricky(@com.fasterxml.jackson.annotation.JsonProperty(value = "apiVersion", required = true) @edu.umd.cs.findbugs.annotations.NonNull java.lang.String apiVersion, @com.fasterxml.jackson.annotation.JsonProperty(value = "kind", required = true) @edu.umd.cs.findbugs.annotations.NonNull java.lang.String kind, @com.fasterxml.jackson.annotation.JsonProperty(value = "spec") @edu.umd.cs.findbugs.annotations.Nullable trickynaming.TrickySpec spec) {
        this.apiVersion = java.util.Objects.requireNonNull(apiVersion);
        this.kind = java.util.Objects.requireNonNull(kind);
        this.spec = spec;
    }

    /**
     * Return the apiVersion.
     *
     * @return The value of this object's apiVersion.
     */
    @edu.umd.cs.findbugs.annotations.NonNull()
    @com.fasterxml.jackson.annotation.JsonProperty(value = "apiVersion", required = true)
    public java.lang.String apiVersion() {
        return this.apiVersion;
    }

    /**
     * Set the apiVersion.
     *
     *  @param apiVersion The new value for this object's apiVersion.
     */
    public void apiVersion(@edu.umd.cs.findbugs.annotations.NonNull() java.lang.String apiVersion) {
        this.apiVersion = java.util.Objects.requireNonNull(apiVersion);
    }

    /**
     * The kind of the CRD API (not the kind of the CR API being defined)
     * @return The value of this object's kind.
     */
    @edu.umd.cs.findbugs.annotations.NonNull()
    @com.fasterxml.jackson.annotation.JsonProperty(value = "kind", required = true)
    public java.lang.String kind() {
        return this.kind;
    }

    /**
     * The kind of the CRD API (not the kind of the CR API being defined)
     *  @param kind The new value for this object's kind.
     */
    public void kind(@edu.umd.cs.findbugs.annotations.NonNull() java.lang.String kind) {
        this.kind = java.util.Objects.requireNonNull(kind);
    }

    /**
     * API being defined
     * @return The value of this object's spec.
     */
    @edu.umd.cs.findbugs.annotations.Nullable()
    @com.fasterxml.jackson.annotation.JsonProperty(value = "spec")
    public trickynaming.TrickySpec spec() {
        return this.spec;
    }

    /**
     * API being defined
     *  @param spec The new value for this object's spec.
     */
    public void spec(@edu.umd.cs.findbugs.annotations.Nullable() trickynaming.TrickySpec spec) {
        this.spec = spec;
    }

    @java.lang.Override()
    public java.lang.String toString() {
        return "Tricky[" + "apiVersion: " + this.apiVersion + ", kind: " + this.kind + ", spec: " + this.spec + "]";
    }

    @java.lang.Override()
    public int hashCode() {
        return java.util.Objects.hash(this.apiVersion, this.kind, this.spec);
    }

    @java.lang.Override()
    public boolean equals(java.lang.Object other) {
        if (this == other)
            return true;
        else if (other instanceof trickynaming.Tricky otherTricky)
            return java.util.Objects.equals(this.apiVersion, otherTricky.apiVersion) && java.util.Objects.equals(this.kind, otherTricky.kind) && java.util.Objects.equals(this.spec, otherTricky.spec);
        else
            return false;
    }
}
