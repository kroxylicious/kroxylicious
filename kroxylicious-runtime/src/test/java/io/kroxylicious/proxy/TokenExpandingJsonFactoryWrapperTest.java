/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.function.Function;
import java.util.stream.Stream;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.module.paramnames.ParameterNamesModule;

import static io.kroxylicious.proxy.TokenExpandingJsonFactoryWrapper.wrap;
import static org.assertj.core.api.Assertions.assertThat;

class TokenExpandingJsonFactoryWrapperTest {

    private static final TypeReference<SimpleBean> OBJECT_TYPE_REF = new TypeReference<>() {
    };
    private static final TypeReference<SimpleBean[]> ARRAY_OF_OBJECTS_TYPE_REF = new TypeReference<>() {
    };
    private static final TypeReference<String[]> ARRAY_OF_SCALARS_TYPE_REF = new TypeReference<>() {
    };
    private static final TypeReference<String> SCALAR_TYPE_REF = new TypeReference<>() {
    };

    static Stream<JsonFactory> factories() {
        return Stream.of(new YAMLFactory(), new JsonFactory());
    }

    @ParameterizedTest
    @MethodSource(value = "factories")
    void wrappedFactoryIsSameType(JsonFactory f) {
        var wrapped = wrap(f);
        assertThat(wrapped)
                .isNotNull()
                .isInstanceOf(f.getClass());
    }

    static Stream<Arguments> deserializationInput() {
        return Stream.of(
                Arguments.of("yaml - obj - 1 field - no expansion", new YAMLFactory(), """
                        myString: foo
                        """, OBJECT_TYPE_REF, new SimpleBean("foo", false, 0)),
                Arguments.of("yaml - obj - 3 fields - no expansion", new YAMLFactory(), """
                        myString: foo
                        myBoolean: true
                        myInt: 1
                        """, OBJECT_TYPE_REF, new SimpleBean("foo", true, 1)),
                Arguments.of("yaml - obj - 1 field - expanded", new YAMLFactory(), """
                        myString: ${sys:foostr}
                        """, OBJECT_TYPE_REF, new SimpleBean("bar", false, 0)),
                Arguments.of("yaml - obj - 3 fields - expanded", new YAMLFactory(), """
                        myString: ${sys:foostr}
                        myBoolean: ${sys:foobool}
                        myInt: ${sys:fooint}
                        """, OBJECT_TYPE_REF, new SimpleBean("bar", true, 5)),
                Arguments.of("yaml - array of objects", new YAMLFactory(), """
                        - myString: ${sys:foostr}
                          myBoolean: ${sys:foobool}
                          myInt: ${sys:fooint}
                        - myString: foo
                          myBoolean: false
                          myInt: 4
                        """, ARRAY_OF_OBJECTS_TYPE_REF, new SimpleBean[]{ new SimpleBean("bar", true, 5), new SimpleBean("foo", false, 4) }),
                Arguments.of("yaml - array of scalars", new YAMLFactory(), """
                        - ${sys:foostr}
                        - foo
                        """, ARRAY_OF_SCALARS_TYPE_REF, new String[]{ "bar", "foo" }),
                Arguments.of("yaml - scalar - expanded", new YAMLFactory(), """
                        ${sys:foostr}
                        """, SCALAR_TYPE_REF, "bar"),
                Arguments.of("json - obj - 3 fields - expanded", new JsonFactory(), """
                        {
                            "myString": "${sys:foostr}",
                            "myBoolean": "${sys:foobool}",
                            "myInt": "${sys:fooint}"
                        }
                        """, OBJECT_TYPE_REF, new SimpleBean("bar", true, 5))

        );
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource(value = "deserializationInput")
    void deserialization(String name, JsonFactory f, String serialisedInput, TypeReference<?> typeReference, Object expected) throws Exception {
        System.setProperty("foostr", "bar");
        System.setProperty("foobool", "true");
        System.setProperty("fooint", "5");

        try {
            var factory = wrap(f);
            var om = new ObjectMapper(factory).registerModule(new ParameterNamesModule());
            var actual = om.readValue(serialisedInput, typeReference);
            assertThat(actual).isEqualTo(expected);
        }
        finally {
            System.clearProperty("foostr");
            System.clearProperty("foobool");
            System.clearProperty("fooint");
        }
    }

    static Stream<Function<ObjectMapper, String>> readValueOverloads() {
        return Stream.of(
                (om) -> {
                    try {
                        return om.readValue("mystring", String.class);
                    }
                    catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                },
                (om) -> {
                    try {
                        return om.readValue("XXXXmystringXXXX".getBytes(StandardCharsets.UTF_8), 4, 8, String.class);
                    }
                    catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                },
                (om) -> {
                    try {
                        return om.readValue(new ByteArrayInputStream("mystring".getBytes(StandardCharsets.UTF_8)), String.class);
                    }
                    catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                });
    }

    @ParameterizedTest
    @MethodSource
    void readValueOverloads(Function<ObjectMapper, String> f) {
        var factory = wrap(new YAMLFactory());
        var om = new ObjectMapper(factory).registerModule(new ParameterNamesModule());
        var actual = f.apply(om);
        assertThat(actual).isEqualTo("mystring");
    }

    record SimpleBean(String myString, boolean myBoolean, int myInt) {}
}
