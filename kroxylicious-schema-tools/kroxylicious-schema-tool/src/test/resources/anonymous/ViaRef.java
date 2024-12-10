/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package anonymous;

/**
 * Auto-generated class representing the schema at /definitions/ViaRef.
 */
@javax.annotation.processing.Generated("io.kroxylicious.tools.schema.compiler.CodeGen")
@com.fasterxml.jackson.annotation.JsonInclude(com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL)
@com.fasterxml.jackson.annotation.JsonPropertyOrder({ "bar" })
@com.fasterxml.jackson.databind.annotation.JsonDeserialize(using = com.fasterxml.jackson.databind.JsonDeserializer.None.class)
public class ViaRef {

    @edu.umd.cs.findbugs.annotations.Nullable
    private java.lang.String bar;

    /**
     * All properties constructor.
     * @param bar The value of the {@code bar} property. This is an optional property.
     */
    @com.fasterxml.jackson.annotation.JsonCreator
    public ViaRef(@edu.umd.cs.findbugs.annotations.Nullable @com.fasterxml.jackson.annotation.JsonProperty(value = "bar") java.lang.String bar) {
        this.bar = bar;
    }

    /**
     * Return the bar.
     *
     * @return The value of this object's bar.
     */
    @edu.umd.cs.findbugs.annotations.Nullable
    @com.fasterxml.jackson.annotation.JsonProperty(value = "bar")
    public java.lang.String bar() {
        return this.bar;
    }

    /**
     * Set the bar.
     *
     *  @param bar The new value for this object's bar.
     */
    public void bar(@edu.umd.cs.findbugs.annotations.Nullable java.lang.String bar) {
        this.bar = bar;
    }

    @java.lang.Override
    public java.lang.String toString() {
        return "ViaRef[" + "bar: " + this.bar + "]";
    }

    @java.lang.Override
    public int hashCode() {
        return java.util.Objects.hash(this.bar);
    }

    @java.lang.Override
    public boolean equals(java.lang.Object other) {
        if (this == other)
            return true;
        else if (other instanceof anonymous.ViaRef otherViaRef)
            return java.util.Objects.equals(this.bar, otherViaRef.bar);
        else
            return false;
    }
}
