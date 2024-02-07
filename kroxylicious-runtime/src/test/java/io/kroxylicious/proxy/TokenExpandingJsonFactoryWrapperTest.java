/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy;

import java.util.stream.Stream;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.module.paramnames.ParameterNamesModule;

import static io.kroxylicious.proxy.TokenExpandingJsonFactoryWrapper.wrap;
import static org.assertj.core.api.Assertions.assertThat;

class TokenExpandingJsonFactoryWrapperTest {

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
                Arguments.of("yaml - no expansion one prop", new YAMLFactory(), """
                        myString: foo
                        """, new SimpleBean("foo", false, 0)),
                Arguments.of("yaml- no expansion three props", new YAMLFactory(), """
                        myString: foo
                        myBoolean: true
                        myInt: 1
                        """, new SimpleBean("foo", true, 1)),
                Arguments.of("yaml - string prop expanded", new YAMLFactory(), """
                        myString: ${sys:foostr}
                        """, new SimpleBean("bar", false, 0)),
                Arguments.of("yaml - all props expanded", new YAMLFactory(), """
                        myString: ${sys:foostr}
                        myBoolean: ${sys:foobool}
                        myInt: ${sys:fooint}
                        """, new SimpleBean("bar", true, 5)),
                Arguments.of("json - all props expanded", new JsonFactory(), """
                        {
                            "myString": "${sys:foostr}",
                            "myBoolean": "${sys:foobool}",
                            "myInt": "${sys:fooint}"
                        }
                        """, new SimpleBean("bar", true, 5))

        );
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource(value = "deserializationInput")
    void deserialise(String name, JsonFactory f, String serialisedInput, SimpleBean expected) throws Exception {

        System.setProperty("foostr", "bar");
        System.setProperty("foobool", "true");
        System.setProperty("fooint", "5");

        var factory = wrap(f);
        var om = new ObjectMapper(factory).registerModule(new ParameterNamesModule());
        var actual = om.readValue(serialisedInput, SimpleBean.class);
        assertThat(actual).isEqualTo(expected);

        System.clearProperty("foostr");
        System.clearProperty("foobool");
        System.clearProperty("fooint");

    }

    public record SimpleBean(String myString, boolean myBoolean, int myInt) {}
}
