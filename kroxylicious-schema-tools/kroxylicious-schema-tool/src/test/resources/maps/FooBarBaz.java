/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package maps;

/**
 * Auto-generated class representing the schema at /definitions/FooBarBaz.
 */
@javax.annotation.processing.Generated("io.kroxylicious.tools.schema.compiler.CodeGen")
@com.fasterxml.jackson.annotation.JsonInclude(com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL)
@com.fasterxml.jackson.annotation.JsonPropertyOrder({ "foo", "bar", "baz" })
@com.fasterxml.jackson.databind.annotation.JsonDeserialize(using = com.fasterxml.jackson.databind.JsonDeserializer.None.class)
public class FooBarBaz {

    @com.fasterxml.jackson.annotation.JsonProperty(value = "foo")
    @com.fasterxml.jackson.annotation.JsonSetter(nulls = com.fasterxml.jackson.annotation.Nulls.SKIP)
    private java.lang.String foo;

    @com.fasterxml.jackson.annotation.JsonProperty(value = "bar")
    @com.fasterxml.jackson.annotation.JsonSetter(nulls = com.fasterxml.jackson.annotation.Nulls.SKIP)
    private java.lang.String bar;

    @com.fasterxml.jackson.annotation.JsonProperty(value = "baz")
    @com.fasterxml.jackson.annotation.JsonSetter(nulls = com.fasterxml.jackson.annotation.Nulls.SKIP)
    private java.lang.Long baz;

    /**
     * Return the foo.
     *
     * @return The value of this object's foo.
     */
    public java.lang.String getFoo() {
        return this.foo;
    }

    /**
     * Set the foo.
     *
     *  @param foo The new value for this object's foo.
     */
    public void setFoo(java.lang.String foo) {
        this.foo = foo;
    }

    /**
     * Return the bar.
     *
     * @return The value of this object's bar.
     */
    public java.lang.String getBar() {
        return this.bar;
    }

    /**
     * Set the bar.
     *
     *  @param bar The new value for this object's bar.
     */
    public void setBar(java.lang.String bar) {
        this.bar = bar;
    }

    /**
     * Return the baz.
     *
     * @return The value of this object's baz.
     */
    public java.lang.Long getBaz() {
        return this.baz;
    }

    /**
     * Set the baz.
     *
     *  @param baz The new value for this object's baz.
     */
    public void setBaz(java.lang.Long baz) {
        this.baz = baz;
    }

    @java.lang.Override()
    public java.lang.String toString() {
        return "FooBarBaz[" + "foo: " + this.foo + ", bar: " + this.bar + ", baz: " + this.baz + "]";
    }

    @java.lang.Override()
    public int hashCode() {
        return java.util.Objects.hash(this.foo, this.bar, this.baz);
    }

    @java.lang.Override()
    public boolean equals(java.lang.Object other) {
        if (this == other)
            return true;
        else if (other instanceof maps.FooBarBaz otherFooBarBaz)
            return java.util.Objects.equals(this.foo, otherFooBarBaz.foo) && java.util.Objects.equals(this.bar, otherFooBarBaz.bar) && java.util.Objects.equals(this.baz, otherFooBarBaz.baz);
        else
            return false;
    }
}