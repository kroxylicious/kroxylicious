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

    @com.fasterxml.jackson.annotation.JsonProperty(value = "kind")
    @com.fasterxml.jackson.annotation.JsonSetter(nulls = com.fasterxml.jackson.annotation.Nulls.SKIP)
    private java.lang.String kind;

    @com.fasterxml.jackson.annotation.JsonProperty(value = "plural")
    @com.fasterxml.jackson.annotation.JsonSetter(nulls = com.fasterxml.jackson.annotation.Nulls.SKIP)
    private java.lang.String plural;

    @com.fasterxml.jackson.annotation.JsonProperty(value = "singular")
    @com.fasterxml.jackson.annotation.JsonSetter(nulls = com.fasterxml.jackson.annotation.Nulls.SKIP)
    private java.lang.String singular;

    @com.fasterxml.jackson.annotation.JsonProperty(value = "shortNames")
    @com.fasterxml.jackson.annotation.JsonSetter(nulls = com.fasterxml.jackson.annotation.Nulls.SKIP)
    private java.util.List<java.lang.String> shortNames;

    /**
     * The kind of the API being defined
     * @return The value of this object's kind.
     */
    public java.lang.String getKind() {
        return this.kind;
    }

    /**
     * The kind of the API being defined
     *  @param kind The new value for this object's kind.
     */
    public void setKind(java.lang.String kind) {
        this.kind = kind;
    }

    /**
     * The plural name of the API being defined
     * @return The value of this object's plural.
     */
    public java.lang.String getPlural() {
        return this.plural;
    }

    /**
     * The plural name of the API being defined
     *  @param plural The new value for this object's plural.
     */
    public void setPlural(java.lang.String plural) {
        this.plural = plural;
    }

    /**
     * The singular name of the API being defined
     * @return The value of this object's singular.
     */
    public java.lang.String getSingular() {
        return this.singular;
    }

    /**
     * The singular name of the API being defined
     *  @param singular The new value for this object's singular.
     */
    public void setSingular(java.lang.String singular) {
        this.singular = singular;
    }

    /**
     * The short name(s) of the API being defined for use with tools. E.g. `kubectl get ...`
     * @return The value of this object's shortNames.
     */
    public java.util.List<java.lang.String> getShortNames() {
        return this.shortNames;
    }

    /**
     * The short name(s) of the API being defined for use with tools. E.g. `kubectl get ...`
     *  @param shortNames The new value for this object's shortNames.
     */
    public void setShortNames(java.util.List<java.lang.String> shortNames) {
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