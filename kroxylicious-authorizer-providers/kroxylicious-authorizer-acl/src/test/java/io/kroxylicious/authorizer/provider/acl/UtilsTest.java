/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.authorizer.provider.acl;

import java.util.List;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import static org.assertj.core.api.Assertions.assertThat;

class UtilsTest {

    @ParameterizedTest
    @MethodSource
    void unquoteRegex(String aclEscapedRe, String regex) {
        assertThat(Utils.unquoteRegex(aclEscapedRe)).isEqualTo(regex);
    }

    static List<Arguments> unquoteRegex() {
        return List.of(
                Arguments.of("abc", "abc"),
                Arguments.of("a\\/b", "a/b"),
                Arguments.of("a\\\\b", "a\\b"),
                Arguments.of("a\\.b", "a\\.b"),
                Arguments.of("\\d+", "\\d+"),
                Arguments.of("\\\\\\/", "\\/"),
                Arguments.of("trailing\\", "trailing\\"));
    }
}
