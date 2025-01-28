/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.net;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

import org.junit.jupiter.api.Test;

import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import com.github.tomakehurst.wiremock.junit5.WireMockTest;

import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static org.assertj.core.api.Assertions.assertThat;

@WireMockTest
public class PassthroughProxyTest {

    public static final String BODY = "hello";

    @Test
    void testProxy(WireMockRuntimeInfo wmRuntimeInfo) throws Exception {
        try (var proxy = new PassthroughProxy(wmRuntimeInfo.getHttpPort(), "localhost");
                var httpClient = HttpClient.newHttpClient();) {
            stubFor(WireMock.get(urlEqualTo("/")).willReturn(WireMock.aResponse().withBody(BODY)));
            URI uri = URI.create("http://localhost:" + proxy.getLocalPort());
            HttpRequest request = HttpRequest.newBuilder(uri).GET().build();
            HttpResponse<String> send = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            assertThat(send.body()).isEqualTo(BODY);
        }
    }
}
