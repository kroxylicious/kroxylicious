/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.router;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(MockitoExtension.class)
class RouterResultTest {

    @Mock
    Response response;

    @Test
    void completedHoldsResponse() {
        var result = new RouterResult.Completed(response);

        assertThat(result.response()).isSameAs(response);
        assertThat(result).isInstanceOf(RouterResult.class);
    }

    @Test
    void completedNoResponseIsRouterResult() {
        var result = new RouterResult.CompletedNoResponse();

        assertThat(result).isInstanceOf(RouterResult.class);
    }

    @Test
    void disconnectIsRouterResult() {
        var result = new RouterResult.Disconnect();

        assertThat(result).isInstanceOf(RouterResult.class);
    }
}
