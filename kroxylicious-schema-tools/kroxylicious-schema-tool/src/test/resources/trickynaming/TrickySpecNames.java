/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package trickynaming;

/**
 * The names of the API being defined
 */
@javax.annotation.processing.Generated("io.kroxylicious.tools.schema.compiler.CodeGen")
@com.fasterxml.jackson.annotation.JsonInclude(com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL)
@com.fasterxml.jackson.annotation.JsonPropertyOrder({ "kind", "plural", "singular", "shortNames" })
@com.fasterxml.jackson.databind.annotation.JsonDeserialize(using = com.fasterxml.jackson.databind.JsonDeserializer.None.class)
public class TrickySpecNames {

    @edu.umd.cs.findbugs.annotations.Nullable()
    private java.lang.String kind;

    @edu.umd.cs.findbugs.annotations.Nullable()
    private java.lang.String plural;

    @edu.umd.cs.findbugs.annotations.Nullable()
    private java.lang.String singular;

    @edu.umd.cs.findbugs.annotations.Nullable()
    private java.util.List<java.lang.String> shortNames;

    /**
     * Required properties constructor.
     */
    public TrickySpecNames() {
    }

    /**
     * All properties constructor.
     * @param kind The value of the {@code kind} property. This is an optional property.
     * @param plural The value of the {@code plural} property. This is an optional property.
     * @param singular The value of the {@code singular} property. This is an optional property.
     * @param shortNames The value of the {@code shortNames} property. This is an optional property.
     */
    public TrickySpecNames(@edu.umd.cs.findbugs.annotations.Nullable() java.lang.String kind, @edu.umd.cs.findbugs.annotations.Nullable() java.lang.String plural, @edu.umd.cs.findbugs.annotations.Nullable() java.lang.String singular, @edu.umd.cs.findbugs.annotations.Nullable() java.util.List<java.lang.String> shortNames) {
        this.kind = kind;
        this.plural = plural;
        this.singular = singular;
        this.shortNames = shortNames;
    }

    /**
     * The kind of the API being defined
     * @return The value of this object's kind.
     */
    @edu.umd.cs.findbugs.annotations.Nullable()
    @com.fasterxml.jackson.annotation.JsonProperty(value = "kind")
    public java.lang.String kind() {
        return this.kind;
    }

    /**
     * The kind of the API being defined
     *  @param kind The new value for this object's kind.
     */
    @com.fasterxml.jackson.annotation.JsonProperty(value = "kind")
    @com.fasterxml.jackson.annotation.JsonSetter(nulls = com.fasterxml.jackson.annotation.Nulls.SKIP)
    public void kind(@edu.umd.cs.findbugs.annotations.Nullable() java.lang.String kind) {
        this.kind = kind;
    }

    /**
     * The plural name of the API being defined
     * @return The value of this object's plural.
     */
    @edu.umd.cs.findbugs.annotations.Nullable()
    @com.fasterxml.jackson.annotation.JsonProperty(value = "plural")
    public java.lang.String plural() {
        return this.plural;
    }

    /**
     * The plural name of the API being defined
     *  @param plural The new value for this object's plural.
     */
    @com.fasterxml.jackson.annotation.JsonProperty(value = "plural")
    @com.fasterxml.jackson.annotation.JsonSetter(nulls = com.fasterxml.jackson.annotation.Nulls.SKIP)
    public void plural(@edu.umd.cs.findbugs.annotations.Nullable() java.lang.String plural) {
        this.plural = plural;
    }

    /**
     * The singular name of the API being defined
     * @return The value of this object's singular.
     */
    @edu.umd.cs.findbugs.annotations.Nullable()
    @com.fasterxml.jackson.annotation.JsonProperty(value = "singular")
    public java.lang.String singular() {
        return this.singular;
    }

    /**
     * The singular name of the API being defined
     *  @param singular The new value for this object's singular.
     */
    @com.fasterxml.jackson.annotation.JsonProperty(value = "singular")
    @com.fasterxml.jackson.annotation.JsonSetter(nulls = com.fasterxml.jackson.annotation.Nulls.SKIP)
    public void singular(@edu.umd.cs.findbugs.annotations.Nullable() java.lang.String singular) {
        this.singular = singular;
    }

    /**
     * The short name(s) of the API being defined for use with tools. E.g. `kubectl get ...`
     * @return The value of this object's shortNames.
     */
    @edu.umd.cs.findbugs.annotations.Nullable()
    @com.fasterxml.jackson.annotation.JsonProperty(value = "shortNames")
    public java.util.List<java.lang.String> shortNames() {
        return this.shortNames;
    }

    /**
     * The short name(s) of the API being defined for use with tools. E.g. `kubectl get ...`
     *  @param shortNames The new value for this object's shortNames.
     */
    @com.fasterxml.jackson.annotation.JsonProperty(value = "shortNames")
    @com.fasterxml.jackson.annotation.JsonSetter(nulls = com.fasterxml.jackson.annotation.Nulls.SKIP)
    public void shortNames(@edu.umd.cs.findbugs.annotations.Nullable() java.util.List<java.lang.String> shortNames) {
        this.shortNames = shortNames;
    }

    @java.lang.Override()
    public java.lang.String toString() {
        return "TrickySpecNames[" + "kind: " + this.kind + ", plural: " + this.plural + ", singular: " + this.singular + ", shortNames: " + this.shortNames + "]";
    }

    @java.lang.Override()
    public int hashCode() {
        return java.util.Objects.hash(this.kind, this.plural, this.singular, this.shortNames);
    }

    @java.lang.Override()
    public boolean equals(java.lang.Object other) {
        if (this == other)
            return true;
        else if (other instanceof trickynaming.TrickySpecNames otherTrickySpecNames)
            return java.util.Objects.equals(this.kind, otherTrickySpecNames.kind) && java.util.Objects.equals(this.plural, otherTrickySpecNames.plural) && java.util.Objects.equals(this.singular, otherTrickySpecNames.singular) && java.util.Objects.equals(this.shortNames, otherTrickySpecNames.shortNames);
        else
            return false;
    }
}
