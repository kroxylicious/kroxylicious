/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.test.tester;

import java.util.List;
import java.util.function.Function;

import org.apache.kafka.common.protocol.ApiKeys;
import org.hamcrest.Matcher;

import io.kroxylicious.proxy.config.ConfigurationBuilder;
import io.kroxylicious.test.Request;
import io.kroxylicious.test.Response;
import io.kroxylicious.test.server.MockServer;

/**
 * A kroxylicious tester for a kroxylicious instance that is proxying a mock kafka broker.
 * <p>When working with a mock broker, use the {@link #mockRequestClient()} or {@link #mockRequestClient(String)}
 * methods to interact with the proxy as the {@link MockServer} currently can only mock a single response at a time,
 * it does not mock out any complex broker behaviour such that you could connect a Producer or Consumer to it
 * and see anything useful happen.</p>
 */
public class MockServerKroxyliciousTester extends DefaultKroxyliciousTester {

    private final MockServer mockServer;

    MockServerKroxyliciousTester(MockServer mockServer, Function<String, ConfigurationBuilder> configurationForMockBootstrap) {
        super(configurationForMockBootstrap.apply("localhost:" + mockServer.port()));
        this.mockServer = mockServer;
    }

    /**
     * Add a response to be served by the MockServer for the ApiKey in the response
     * @param response the response
     */
    public void addMockResponseForApiKey(Response response) {
        mockServer.addMockResponseForApiKey(response);
    }

    /**
     * Add a response to be served by the MockServer for the ApiKey in the response
     * @param requestMatcher the matcher
     * @param response the response
     */
    public void addMockResponse(Matcher<Request> requestMatcher, Response response) {
        mockServer.addMockResponse(requestMatcher, response);
    }

    /**
     * Clear the mock response and instruct the mock server to drop stored invocations.
     */
    public void clearMock() {
        mockServer.clear();
    }

    /**
     * Get the only request recorded by the MockServer
     * @return the only request
     * @throws IllegalStateException if the mock server has recorded more than one request received
     */
    public Request getOnlyRequest() {
        List<Request> requests = mockServer.getReceivedRequests();
        if (requests.size() != 1) {
            throw new IllegalStateException("mock server has recorded " + requests.size() + " requests, expected one");
        }
        return requests.get(0);
    }

    /**
     * Assert that all configured interactions has at least one interaction.
     * @throws AssertionError if any mocked interaction has not been invoked
     */
    public void assertAllMockInteractionsInvoked() {
        this.mockServer.assertAllMockInteractionsInvoked();
    }

    /**
     * Get the only request recorded by the MockServer for a given ApiKey
     * @return the only request
     * @throws IllegalStateException if the mock server has recorded more than one request received for that key
     */
    public Request getOnlyRequestForApiKey(ApiKeys apiKeys) {
        List<Request> requests = mockServer.getReceivedRequests().stream().filter(request -> request.apiKeys() == apiKeys).toList();
        if (requests.size() != 1) {
            throw new IllegalStateException("mock server has recorded " + requests.size() + " requests, expected one");
        }
        return requests.get(0);
    }

    /**
     * Close the mocks erver and kroxylicious proxy
     */
    @Override
    public void close() {
        mockServer.close();
        super.close();
    }
}
