/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.test.tester;

import java.util.List;
import java.util.function.Function;

import io.kroxylicious.proxy.KroxyliciousConfigBuilder;
import io.kroxylicious.test.Request;
import io.kroxylicious.test.Response;
import io.kroxylicious.test.server.MockServer;

/**
 * A kroxylicious tester for a kroxylicious instance that is proxying a mock kafka broker.
 * <p>When working with a mock broker, use the {@link #singleRequestClient()} or {@link #singleRequestClient(String)}
 * methods to interact with the proxy as the {@link MockServer} currently can only mock a single response at a time,
 * it does not mock out any complex broker behaviour such that you could connect a Producer or Consumer to it
 * and see anything useful happen.</p>
 */
public class MockServerKroxyliciousTester extends DefaultKroxyliciousTester {

    private final MockServer mockServer;

    MockServerKroxyliciousTester(MockServer mockServer, Function<String, KroxyliciousConfigBuilder> configurationForMockBootstrap) {
        super(configurationForMockBootstrap.apply("localhost:" + mockServer.port()));
        this.mockServer = mockServer;
    }

    /**
     * Set the response to be served by the MockServer
     * @param response the response (nullable)
     */
    public void setMockResponse(Response response) {
        mockServer.setResponse(response);
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
    public Request onlyRequest() {
        List<Request> requests = mockServer.getReceivedRequests();
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
