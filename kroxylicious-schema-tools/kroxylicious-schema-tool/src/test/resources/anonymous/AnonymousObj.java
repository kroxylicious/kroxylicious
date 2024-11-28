/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package anonymous;

/**
 * Auto-generated class representing the schema at /properties/obj.
 */
@javax.annotation.processing.Generated("io.kroxylicious.tools.schema.compiler.CodeGen")
@com.fasterxml.jackson.annotation.JsonInclude(com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL)
@com.fasterxml.jackson.annotation.JsonPropertyOrder({ "foo" })
@com.fasterxml.jackson.databind.annotation.JsonDeserialize(using = com.fasterxml.jackson.databind.JsonDeserializer.None.class)
public class AnonymousObj {

    @edu.umd.cs.findbugs.annotations.Nullable()
    private java.lang.String foo;

    /**
     * Required properties constructor.
     */
    public AnonymousObj() {
    }

    /**
     * All properties constructor.
     * @param foo The value of the {@code foo} property. This is an optional property.
     */
    @com.fasterxml.jackson.annotation.JsonCreator()
    public AnonymousObj(@com.fasterxml.jackson.annotation.JsonProperty(value = "foo") @edu.umd.cs.findbugs.annotations.Nullable java.lang.String foo) {
        this.foo = foo;
    }

    /**
     * Return the foo.
     *
     * @return The value of this object's foo.
     */
    @edu.umd.cs.findbugs.annotations.Nullable()
    @com.fasterxml.jackson.annotation.JsonProperty(value = "foo")
    public java.lang.String foo() {
        return this.foo;
    }

    /**
     * Set the foo.
     *
     *  @param foo The new value for this object's foo.
     */
    public void foo(@edu.umd.cs.findbugs.annotations.Nullable() java.lang.String foo) {
        this.foo = foo;
    }

    @java.lang.Override()
    public java.lang.String toString() {
        return "AnonymousObj[" + "foo: " + this.foo + "]";
    }

    @java.lang.Override()
    public int hashCode() {
        return java.util.Objects.hash(this.foo);
    }

    @java.lang.Override()
    public boolean equals(java.lang.Object other) {
        if (this == other)
            return true;
        else if (other instanceof anonymous.AnonymousObj otherAnonymousObj)
            return java.util.Objects.equals(this.foo, otherAnonymousObj.foo);
        else
            return false;
    }
}
