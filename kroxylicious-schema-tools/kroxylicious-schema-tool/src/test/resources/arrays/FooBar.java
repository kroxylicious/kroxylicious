/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package arrays;

/**
 * Auto-generated class representing the schema at /definitions/FooBar.
 */
@javax.annotation.processing.Generated("io.kroxylicious.tools.schema.compiler.CodeGen")
@com.fasterxml.jackson.annotation.JsonInclude(com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL)
@com.fasterxml.jackson.annotation.JsonPropertyOrder({ "foo", "bar" })
@com.fasterxml.jackson.databind.annotation.JsonDeserialize(using = com.fasterxml.jackson.databind.JsonDeserializer.None.class)
public class FooBar {

    @edu.umd.cs.findbugs.annotations.Nullable()
    @com.fasterxml.jackson.annotation.JsonProperty(value = "foo")
    @com.fasterxml.jackson.annotation.JsonSetter(nulls = com.fasterxml.jackson.annotation.Nulls.SKIP)
    private java.lang.String foo;

    @edu.umd.cs.findbugs.annotations.Nullable()
    @com.fasterxml.jackson.annotation.JsonProperty(value = "bar")
    @com.fasterxml.jackson.annotation.JsonSetter(nulls = com.fasterxml.jackson.annotation.Nulls.SKIP)
    private java.lang.String bar;

    /**
     * Required properties constructor.
     */
    public FooBar() {
    }

    /**
     * All properties constructor.
     * @param foo The value of the {@code foo} property. This is an optional property.
     * @param bar The value of the {@code bar} property. This is an optional property.
     */
    public FooBar(@edu.umd.cs.findbugs.annotations.Nullable() java.lang.String foo, @edu.umd.cs.findbugs.annotations.Nullable() java.lang.String bar) {
        this.foo = foo;
        this.bar = bar;
    }

    /**
     * Return the foo.
     *
     * @return The value of this object's foo.
     */
    @edu.umd.cs.findbugs.annotations.Nullable()
    public java.lang.String getFoo() {
        return this.foo;
    }

    /**
     * Set the foo.
     *
     *  @param foo The new value for this object's foo.
     */
    public void setFoo(@edu.umd.cs.findbugs.annotations.Nullable() java.lang.String foo) {
        this.foo = foo;
    }

    /**
     * Return the bar.
     *
     * @return The value of this object's bar.
     */
    @edu.umd.cs.findbugs.annotations.Nullable()
    public java.lang.String getBar() {
        return this.bar;
    }

    /**
     * Set the bar.
     *
     *  @param bar The new value for this object's bar.
     */
    public void setBar(@edu.umd.cs.findbugs.annotations.Nullable() java.lang.String bar) {
        this.bar = bar;
    }

    @java.lang.Override()
    public java.lang.String toString() {
        return "FooBar[" + "foo: " + this.foo + ", bar: " + this.bar + "]";
    }

    @java.lang.Override()
    public int hashCode() {
        return java.util.Objects.hash(this.foo, this.bar);
    }

    @java.lang.Override()
    public boolean equals(java.lang.Object other) {
        if (this == other)
            return true;
        else if (other instanceof arrays.FooBar otherFooBar)
            return java.util.Objects.equals(this.foo, otherFooBar.foo) && java.util.Objects.equals(this.bar, otherFooBar.bar);
        else
            return false;
    }
}
