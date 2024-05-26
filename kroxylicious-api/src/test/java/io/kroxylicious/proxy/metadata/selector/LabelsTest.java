/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.metadata.selector;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class LabelsTest {

    @Test
    void testValidatesValue() {
        Map<String, String> ok = Map.of("foo", "x");
        assertThat(Labels.validate(ok)).isSameAs(ok);
        Map<String, String> emptyValue = Map.of("foo", "");
        assertThat(Labels.validate(emptyValue)).isSameAs(emptyValue);
        String maxLength = "x".repeat(63);
        assertThat(Labels.validate(Map.of("foo", maxLength))).isNotNull();
        Map<String, String> tooLongValue = Map.of("foo", maxLength + "x");
        assertThatThrownBy(() -> Labels.validate(tooLongValue))
                .isExactlyInstanceOf(LabelException.class)
                .hasMessage("value is too long: must be 63 characters or fewer");

        Map<String, String> nullValue = new HashMap<>();
        nullValue.put("foo", null);
        assertThatThrownBy(() -> Labels.validate(nullValue))
                .isExactlyInstanceOf(NullPointerException.class);
    }

    @Test
    void testValidatesKey() {
        Map<String, String> ok = Map.of("example.org/foo", "x");
        assertThat(Labels.validate(ok)).isSameAs(ok);

        // without prefix
        String maxLengthName = "y".repeat(63);
        assertThat(Labels.validate(Map.of(maxLengthName, "y"))).isNotNull();
        Map<String, String> tooLongName = Map.of(maxLengthName + "y", "x");
        assertThatThrownBy(() -> Labels.validate(tooLongName))
                .isExactlyInstanceOf(LabelException.class)
                .hasMessage("name in prefix is too long: must be 63 characters or fewer");

        // with prefix
        assertThat(Labels.validate(Map.of("org.example/" + maxLengthName, "y"))).isNotNull();
        Map<String, String> tooLongNameWithPrefix = Map.of("org.example/" + maxLengthName + "y", "x");
        assertThatThrownBy(() -> Labels.validate(tooLongNameWithPrefix))
                .isExactlyInstanceOf(LabelException.class)
                .hasMessage("name in prefix is too long: must be 63 characters or fewer");

        String maxLengthPrefix = "z".repeat(253);
        assertThat(Labels.validate(Map.of(maxLengthPrefix + "/name", "y"))).isNotNull();
        Map<String, String> tooLongPrefix = Map.of(maxLengthPrefix + "z" + "/name", "y");
        assertThatThrownBy(() -> Labels.validate(tooLongPrefix))
                .isExactlyInstanceOf(LabelException.class)
                .hasMessage("DNS subdomain in prefix is too long: must be 253 characters or fewer");

        Map<String, String> nullKey = new HashMap<>();
        nullKey.put(null, "x");
        assertThatThrownBy(() -> Labels.validate(nullKey))
                .isExactlyInstanceOf(NullPointerException.class);

        Map<String, String> doubleDot = Map.of("x..", "z");
        assertThatThrownBy(() -> Labels.validate(doubleDot))
                .isExactlyInstanceOf(LabelException.class)
                .hasMessage("invalid selector syntax at 1:3: extraneous input '<EOF>' expecting {'-', '_', '.', LETTER, DIGIT}");
    }

}
