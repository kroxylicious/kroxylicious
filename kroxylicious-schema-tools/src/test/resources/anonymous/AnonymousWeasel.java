/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package anonymous;

@javax.annotation.processing.Generated("io.kroxylicious.tools.schema.compiler.CodeGen")
@com.fasterxml.jackson.annotation.JsonInclude(com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL)
@com.fasterxml.jackson.annotation.JsonPropertyOrder({ "baz" })
@com.fasterxml.jackson.databind.annotation.JsonDeserialize(using = com.fasterxml.jackson.databind.JsonDeserializer.None.class)
public class AnonymousWeasel {

    @com.fasterxml.jackson.annotation.JsonProperty("baz")
    @com.fasterxml.jackson.annotation.JsonSetter(nulls = com.fasterxml.jackson.annotation.Nulls.SKIP)
    private java.lang.String baz;

    public java.lang.String getBaz() {
        return this.baz;
    }

    public void setBaz(java.lang.String baz) {
        this.baz = baz;
    }

    @java.lang.Override()
    public java.lang.String toString() {
        return "AnonymousWeasel[" + "baz: " + this.baz + "]";
    }

    @java.lang.Override()
    public int hashCode() {
        return java.util.Objects.hash(this.baz);
    }

    @java.lang.Override()
    public boolean equals(java.lang.Object other) {
        if (this == other)
            return true;
        else if (other instanceof anonymous.AnonymousWeasel otherAnonymousWeasel)
            return java.util.Objects.equals(this.baz, otherAnonymousWeasel.baz);
        else
            return false;
    }
}