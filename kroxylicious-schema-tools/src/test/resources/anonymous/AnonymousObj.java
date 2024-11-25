/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package anonymous;

@javax.annotation.processing.Generated("io.kroxylicious.tools.schema.compiler.CodeGen")
@com.fasterxml.jackson.annotation.JsonInclude(com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL)
@com.fasterxml.jackson.annotation.JsonPropertyOrder({ "foo" })
@com.fasterxml.jackson.databind.annotation.JsonDeserialize(using = com.fasterxml.jackson.databind.JsonDeserializer.None.class)
public class AnonymousObj {

    @com.fasterxml.jackson.annotation.JsonProperty("foo")
    @com.fasterxml.jackson.annotation.JsonSetter(nulls = com.fasterxml.jackson.annotation.Nulls.SKIP)
    private java.lang.String foo;

    public java.lang.String getFoo() {
        return this.foo;
    }

    public void setFoo(java.lang.String foo) {
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