/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.tools.schema.compiler;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import edu.umd.cs.findbugs.annotations.Nullable;

import io.kroxylicious.tools.schema.model.SchemaVisitor;

import org.junit.jupiter.api.Test;

import io.kroxylicious.tools.schema.model.SchemaObject;
import io.kroxylicious.tools.schema.model.SchemaObjectBuilder;
import io.kroxylicious.tools.schema.model.SchemaType;
import io.kroxylicious.tools.schema.model.VisitException;

import edu.umd.cs.findbugs.annotations.NonNull;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SchemaObjectTest {

    private SchemaObject schema = new SchemaObjectBuilder().withType(SchemaType.OBJECT)
            .addToProperties("foo", new SchemaObjectBuilder().withType(SchemaType.STRING).build())
            .addToProperties("bar", new SchemaObjectBuilder().withType(SchemaType.STRING).build())
            .build();

    @Test
    void testVisitor() {
        MySchemaVisitor visitor = new MySchemaVisitor(null);
        URI base = URI.create("test://schema");
        schema.visitSchemas(base, visitor);
        assertThat(visitor.contextsPreorder).hasToString(
                "[Context{base=test://schema, keyword='', fullPath=''}, "
                + "Context{keyword='properties', fullPath='/properties/foo'}, "
                + "Context{keyword='properties', fullPath='/properties/bar'}]");
        assertThat(visitor.contextsPostorder).hasToString(
                "[Context{keyword='properties', fullPath='/properties/foo'}, "
                        + "Context{keyword='properties', fullPath='/properties/bar'}, "
                        + "Context{base=test://schema, keyword='', fullPath=''}]");
    }

    @Test
    void testVisitorThrows() {
        SchemaVisitor throwingVisitor = new MySchemaVisitor("/properties/bar");
        URI base = URI.create("test://schema");
        assertThatThrownBy(() -> schema.visitSchemas(base, throwingVisitor))
                .isExactlyInstanceOf(VisitException.class)
                .hasMessage(
                        "io.kroxylicious.tools.schema.compiler.SchemaObjectTest$MySchemaVisitor#enterSchema() threw exception while visiting schema object at '/properties/bar' from test://schema");
    }



    @Test
    void hasCodeAndEquals() {
        SchemaObject fooBar = new SchemaObjectBuilder().withType(SchemaType.OBJECT)
                .addToProperties("foo", new SchemaObjectBuilder().withType(SchemaType.STRING).build())
                .addToProperties("bar", new SchemaObjectBuilder().withType(SchemaType.STRING).build())
                .build();
        SchemaObject fooBar2 = new SchemaObjectBuilder().withType(SchemaType.OBJECT)
                .addToProperties("foo", new SchemaObjectBuilder().withType(SchemaType.STRING).build())
                .addToProperties("bar", new SchemaObjectBuilder().withType(SchemaType.STRING).build())
                .build();
        SchemaObject fooBaz = new SchemaObjectBuilder().withType(SchemaType.OBJECT)
                .addToProperties("foo", new SchemaObjectBuilder().withType(SchemaType.STRING).build())
                .addToProperties("baz", new SchemaObjectBuilder().withType(SchemaType.STRING).build())
                .build();
        SchemaObject fooBazInt = new SchemaObjectBuilder().withType(SchemaType.OBJECT)
                .addToProperties("foo", new SchemaObjectBuilder().withType(SchemaType.STRING).build())
                .addToProperties("baz", new SchemaObjectBuilder().withType(SchemaType.INTEGER).build())
                .build();
        SchemaObject fooBarJava = new SchemaObjectBuilder().withType(SchemaType.OBJECT)
                .addToProperties("foo", new SchemaObjectBuilder().withType(SchemaType.STRING).build())
                .addToProperties("bar", new SchemaObjectBuilder().withType(SchemaType.STRING).build())
                .withJavaType("$$FooBar")
                .build();
        SchemaObject fooBarId = new SchemaObjectBuilder().withType(SchemaType.OBJECT)
                .addToProperties("foo", new SchemaObjectBuilder().withType(SchemaType.STRING).build())
                .addToProperties("bar", new SchemaObjectBuilder().withType(SchemaType.STRING).build())
                .withId("foobar")
                .build();

        assertThat(fooBar)
                .isEqualTo(fooBar)
                .isEqualTo(fooBar2)
                .isNotEqualTo(fooBaz)
                .isNotEqualTo(fooBazInt)
                .isNotEqualTo(fooBarJava)
                .isNotEqualTo(fooBarId);
        assertThat(fooBar2)
                .isEqualTo(fooBar)
                .isEqualTo(fooBar2)
                .isNotEqualTo(fooBaz)
                .isNotEqualTo(fooBazInt)
                .isNotEqualTo(fooBarJava)
                .isNotEqualTo(fooBarId);
        assertThat(fooBaz)
                .isNotEqualTo(fooBar)
                .isNotEqualTo(fooBar2)
                .isEqualTo(fooBaz)
                .isNotEqualTo(fooBazInt)
                .isNotEqualTo(fooBarJava)
                .isNotEqualTo(fooBarId);
        assertThat(fooBazInt)
                .isNotEqualTo(fooBar)
                .isNotEqualTo(fooBar2)
                .isNotEqualTo(fooBaz)
                .isEqualTo(fooBazInt)
                .isNotEqualTo(fooBarJava)
                .isNotEqualTo(fooBarId);

        assertThat(fooBar)
                .hasSameHashCodeAs(fooBar2)
                .hasToString(fooBar2.toString());
    }

    private static class MySchemaVisitor extends SchemaVisitor {
        public final String pathWhichThrows;
        public final List<Context> contextsPreorder = new ArrayList<>();
        public final List<Context> contextsPostorder = new ArrayList<>();

        MySchemaVisitor(@Nullable String pathWhichThrows) {
            this.pathWhichThrows = pathWhichThrows;
        }

        @Override
        public void enterSchema(
                Context context,
                @NonNull SchemaObject schema) {
            super.enterSchema(context, schema);
            contextsPreorder.add(context);
            if (context.fullPath().equals(pathWhichThrows)) {
                throw new RuntimeException();
            }
        }

        @Override
        public void exitSchema(
                Context context,
                @NonNull SchemaObject schema
        ) {
            super.exitSchema(context, schema);
            contextsPostorder.add(context);
        }
    }
}
