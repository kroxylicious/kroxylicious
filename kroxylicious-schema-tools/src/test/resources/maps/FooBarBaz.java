/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package maps;

@javax.annotation.processing.Generated("io.kroxylicious.tools.schema.compiler.CodeGen")
@com.fasterxml.jackson.annotation.JsonInclude(com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL)
@com.fasterxml.jackson.annotation.JsonPropertyOrder({ "foo", "bar", "baz" })
@com.fasterxml.jackson.databind.annotation.JsonDeserialize(using = com.fasterxml.jackson.databind.JsonDeserializer.None.class)
public class FooBarBaz {

    @com.fasterxml.jackson.annotation.JsonProperty("foo")
    @com.fasterxml.jackson.annotation.JsonSetter(nulls = com.fasterxml.jackson.annotation.Nulls.SKIP)
    private java.lang.String foo;

    @com.fasterxml.jackson.annotation.JsonProperty("bar")
    @com.fasterxml.jackson.annotation.JsonSetter(nulls = com.fasterxml.jackson.annotation.Nulls.SKIP)
    private java.lang.String bar;

    @com.fasterxml.jackson.annotation.JsonProperty("baz")
    @com.fasterxml.jackson.annotation.JsonSetter(nulls = com.fasterxml.jackson.annotation.Nulls.SKIP)
    private java.lang.Long baz;

    public java.lang.String getFoo() {
        return this.foo;
    }

    public void setFoo(java.lang.String foo) {
        this.foo = foo;
    }

    public java.lang.String getBar() {
        return this.bar;
    }

    public void setBar(java.lang.String bar) {
        this.bar = bar;
    }

    public java.lang.Long getBaz() {
        return this.baz;
    }

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