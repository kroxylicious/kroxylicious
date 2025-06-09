/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.service;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class HostPortTest {

    @Test
    void validBareHost() {
        var hp = HostPort.parse("localhost:12345");
        assertThat(hp.host()).isEqualTo("localhost");
        assertThat(hp.port()).isEqualTo(12345);
    }

    @Test
    void asString() {
        var hp = HostPort.asString("localhost", 12345);
        assertThat(hp).isEqualTo("localhost:12345");
    }

    @Test
    void validFQDN() {
        var hp = HostPort.parse("kafka.example.com:12345");
        assertThat(hp.host()).isEqualTo("kafka.example.com");
        assertThat(hp.port()).isEqualTo(12345);
    }

    @Test
    void validIpv4() {
        var hp = HostPort.parse("192.168.0.1:12345");
        assertThat(hp.host()).isEqualTo("192.168.0.1");
        assertThat(hp.port()).isEqualTo(12345);
    }

    @Test
    void validIpv6() {
        var hp = HostPort.parse("[2001:db8::1]:12345");
        assertThat(hp.host()).isEqualTo("[2001:db8::1]");
        assertThat(hp.port()).isEqualTo(12345);
    }

    @ParameterizedTest
    @CsvSource({ "foo.example.net:80,Foo.ExamplE.net:80",
            "aol.com:80,AOL.COM:80",
            "www.gnu.ai.mit.edu:80,WWW.gnu.AI.mit.EDU:80",
            "69.2.0.192.in-addr.arpa:80,69.2.0.192.in-ADDR.ARPA:80" })
    void caseInsensitivityRfc4343(String left, String right) {
        var l = HostPort.parse(left);
        var r = HostPort.parse(right);
        assertThat(l).isEqualTo(r);
        assertThat(r).isEqualTo(l);
        assertThat(r.hashCode()).isEqualTo(l.hashCode());
    }

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = { " ", "localhost", ":1000", ":onethousand", "something:really:odd" })
    public void shouldThrowExceptionForMalformedInput(String input) {
        assertThatThrownBy(() -> {
            HostPort.parse(input);
        }).isInstanceOf(IllegalArgumentException.class);
    }
}