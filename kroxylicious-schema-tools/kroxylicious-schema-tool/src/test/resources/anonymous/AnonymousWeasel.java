/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package anonymous;

/**
 * Auto-generated class representing the schema at /properties/weasels/items/0.
 */
@javax.annotation.processing.Generated("io.kroxylicious.tools.schema.compiler.CodeGen")
@com.fasterxml.jackson.annotation.JsonInclude(com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL)
@com.fasterxml.jackson.annotation.JsonPropertyOrder({ "baz" })
@com.fasterxml.jackson.databind.annotation.JsonDeserialize(using = com.fasterxml.jackson.databind.JsonDeserializer.None.class)
public class AnonymousWeasel {

    @edu.umd.cs.findbugs.annotations.Nullable
    private java.lang.String baz;

    /**
     * All properties constructor.
     * @param baz The value of the {@code baz} property. This is an optional property.
     */
    @com.fasterxml.jackson.annotation.JsonCreator
    public AnonymousWeasel(@com.fasterxml.jackson.annotation.JsonProperty(value = "baz") @edu.umd.cs.findbugs.annotations.Nullable java.lang.String baz) {
        this.baz = baz;
    }

    /**
     * Return the baz.
     *
     * @return The value of this object's baz.
     */
    @edu.umd.cs.findbugs.annotations.Nullable
    @com.fasterxml.jackson.annotation.JsonProperty(value = "baz")
    public java.lang.String baz() {
        return this.baz;
    }

    /**
     * Set the baz.
     *
     *  @param baz The new value for this object's baz.
     */
    public void baz(@edu.umd.cs.findbugs.annotations.Nullable java.lang.String baz) {
        this.baz = baz;
    }

    @java.lang.Override
    public java.lang.String toString() {
        return "AnonymousWeasel[" + "baz: " + this.baz + "]";
    }

    @java.lang.Override
    public int hashCode() {
        return java.util.Objects.hash(this.baz);
    }

    @java.lang.Override
    public boolean equals(java.lang.Object other) {
        if (this == other)
            return true;
        else if (other instanceof anonymous.AnonymousWeasel otherAnonymousWeasel)
            return java.util.Objects.equals(this.baz, otherAnonymousWeasel.baz);
        else
            return false;
    }
}
