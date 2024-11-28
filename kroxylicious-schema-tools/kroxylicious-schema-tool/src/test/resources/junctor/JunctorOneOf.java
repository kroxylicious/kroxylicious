/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package junctor;

/**
 * Auto-generated class representing the schema at /properties/oneOf.
 */
@javax.annotation.processing.Generated("io.kroxylicious.tools.schema.compiler.CodeGen")
@com.fasterxml.jackson.annotation.JsonInclude(com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL)
@com.fasterxml.jackson.annotation.JsonPropertyOrder({ "foo", "bar" })
@com.fasterxml.jackson.databind.annotation.JsonDeserialize(using = com.fasterxml.jackson.databind.JsonDeserializer.None.class)
public class JunctorOneOf {

    @com.fasterxml.jackson.annotation.JsonProperty(value = "foo")
    @com.fasterxml.jackson.annotation.JsonSetter(nulls = com.fasterxml.jackson.annotation.Nulls.SKIP)
    private java.lang.Boolean foo;

    @com.fasterxml.jackson.annotation.JsonProperty(value = "bar")
    @com.fasterxml.jackson.annotation.JsonSetter(nulls = com.fasterxml.jackson.annotation.Nulls.SKIP)
    private java.lang.Boolean bar;

    /**
     * Return the foo.
     *
     * @return The value of this object's foo.
     */
    public java.lang.Boolean getFoo() {
        return this.foo;
    }

    /**
     * Set the foo.
     *
     *  @param foo The new value for this object's foo.
     */
    public void setFoo(java.lang.Boolean foo) {
        this.foo = foo;
    }

    /**
     * Return the bar.
     *
     * @return The value of this object's bar.
     */
    public java.lang.Boolean getBar() {
        return this.bar;
    }

    /**
     * Set the bar.
     *
     *  @param bar The new value for this object's bar.
     */
    public void setBar(java.lang.Boolean bar) {
        this.bar = bar;
    }

    @java.lang.Override()
    public java.lang.String toString() {
        return "JunctorOneOf[" + "foo: " + this.foo + ", bar: " + this.bar + "]";
    }

    @java.lang.Override()
    public int hashCode() {
        return java.util.Objects.hash(this.foo, this.bar);
    }

    @java.lang.Override()
    public boolean equals(java.lang.Object other) {
        if (this == other)
            return true;
        else if (other instanceof junctor.JunctorOneOf otherJunctorOneOf)
            return java.util.Objects.equals(this.foo, otherJunctorOneOf.foo) && java.util.Objects.equals(this.bar, otherJunctorOneOf.bar);
        else
            return false;
    }
}