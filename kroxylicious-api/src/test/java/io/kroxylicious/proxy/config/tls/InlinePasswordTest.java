/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config.tls;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class InlinePasswordTest {

    @Test
    public void testInlinePassword() {
        InlinePassword inline = new InlinePassword("pazz");
        assertThat(inline.getProvidedPassword()).isEqualTo("pazz");
    }

    @Test
    public void testInlinePasswordToStringDoesNotExposePassword() {
        InlinePassword inline = new InlinePassword("shouldNotBeExposed");
        assertThat(inline.toString()).doesNotContain("shouldNotBeExposed");
    }

}