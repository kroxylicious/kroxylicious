/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.routing;

import java.util.Map;

import org.apache.kafka.common.message.FetchRequestData;
import org.apache.kafka.common.message.MetadataRequestData;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;

import io.kroxylicious.proxy.frame.DecodedRequestFrame;
import io.kroxylicious.proxy.frame.DecodedResponseFrame;
import io.kroxylicious.proxy.frame.OpaqueRequestFrame;
import io.kroxylicious.proxy.internal.ClientConnectionStateMachine;
import io.kroxylicious.proxy.router.Router;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class RouterDispatchHandlerTest {

    private static final int CORRELATION_ID = 42;
    private static final String DEFAULT_ROUTE = "default";

    @Mock
    private Router router;

    @Mock
    private ClientConnectionStateMachine ccsm;

    private EmbeddedChannel channel;

    private RouterDispatchHandler handlerWithIdentityMapping(Map<ApiKeys, String> staticRoutes) {
        return new RouterDispatchHandler(router, staticRoutes, ccsm, new IdentityNodeIdMapping(DEFAULT_ROUTE));
    }

    @Test
    void shouldThrowForNonFrameMessage() {
        var handler = handlerWithIdentityMapping(Map.of());
        channel = new EmbeddedChannel(handler);

        assertThatThrownBy(() -> channel.writeInbound("not-a-frame"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Unexpected non-frame message");
    }

    @Test
    void shouldThrowForDynamicallyRoutedDecodedFrame() {
        var handler = handlerWithIdentityMapping(Map.of());
        channel = new EmbeddedChannel(handler);

        var frame = new DecodedRequestFrame<>((short) 12, CORRELATION_ID, true,
                new org.apache.kafka.common.message.RequestHeaderData(), new FetchRequestData());

        assertThatThrownBy(() -> channel.writeInbound(frame))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Dynamic routing is not supported");
    }

    @Test
    void shouldForwardStaticallyRoutedDecodedFrameViaForwardToRoute() {
        var staticRoutes = Map.of(ApiKeys.FETCH, DEFAULT_ROUTE);
        var handler = handlerWithIdentityMapping(staticRoutes);
        channel = new EmbeddedChannel(handler);

        var frame = new DecodedRequestFrame<>((short) 12, CORRELATION_ID, true,
                new org.apache.kafka.common.message.RequestHeaderData(), new FetchRequestData());

        channel.writeInbound(frame);

        verify(ccsm).forwardToRoute(DEFAULT_ROUTE, frame);
    }

    @Test
    void shouldForwardStaticallyRoutedOpaqueFrameViaForwardToRoute() {
        var staticRoutes = Map.of(ApiKeys.FETCH, DEFAULT_ROUTE);
        var handler = handlerWithIdentityMapping(staticRoutes);
        channel = new EmbeddedChannel(handler);

        var buf = Unpooled.buffer();
        var opaqueFrame = new OpaqueRequestFrame(
                buf, (short) ApiKeys.FETCH.id, (short) 12, CORRELATION_ID, false, 0, true);

        channel.writeInbound(opaqueFrame);

        verify(ccsm).forwardToRoute(DEFAULT_ROUTE, opaqueFrame);
        buf.release();
    }

    @Test
    void shouldThrowForOpaqueFrameNotInStaticRoutes() {
        var staticRoutes = Map.of(ApiKeys.PRODUCE, DEFAULT_ROUTE);
        var handler = handlerWithIdentityMapping(staticRoutes);
        channel = new EmbeddedChannel(handler);

        var buf = Unpooled.buffer();
        var opaqueFrame = new OpaqueRequestFrame(
                buf, (short) ApiKeys.FETCH.id, (short) 12, CORRELATION_ID, false, 0, true);

        assertThatThrownBy(() -> channel.writeInbound(opaqueFrame))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Dynamic routing is not supported");
        buf.release();
    }

    @Test
    void shouldTranslateNodeIdsInMetadataResponse() {
        // Given: bijective mapping with two routes; METADATA statically routed to route-a
        var mapping = new BijectiveNodeIdMapping(Map.of("route-a", 0, "route-b", 1), 2);
        var handler = new RouterDispatchHandler(router, Map.of(ApiKeys.METADATA, "route-a"), ccsm, mapping);
        channel = new EmbeddedChannel(handler);

        // Record the pending METADATA request
        var requestFrame = new DecodedRequestFrame<>((short) 12, CORRELATION_ID, true,
                new org.apache.kafka.common.message.RequestHeaderData(), new MetadataRequestData());
        channel.writeInbound(requestFrame);

        // When: METADATA response arrives with upstream node IDs 0 and 1
        var md = new MetadataResponseData();
        md.setControllerId(0);
        md.brokers().add(new MetadataResponseData.MetadataResponseBroker().setNodeId(0).setHost("h0").setPort(9092));
        md.brokers().add(new MetadataResponseData.MetadataResponseBroker().setNodeId(1).setHost("h1").setPort(9093));
        var responseFrame = new DecodedResponseFrame<>((short) 12, CORRELATION_ID, new ResponseHeaderData(), md);
        channel.writeOutbound(responseFrame);

        // Then: the outbound frame has translated node IDs
        DecodedResponseFrame<?> out = channel.readOutbound();
        assertThat(out).isNotNull();
        var translatedMd = (MetadataResponseData) out.body();
        // route-a has id=0, totalRoutes=2: virtual(0,0)=0, virtual(0,1)=2
        assertThat(translatedMd.brokers().find(0)).isNotNull(); // node 0 → virtual 0
        assertThat(translatedMd.brokers().find(2)).isNotNull(); // node 1 → virtual 2
        assertThat(translatedMd.controllerId()).isEqualTo(0); // virtual 0
    }

    @Test
    void shouldPassThroughUnknownCorrelationIdInResponse() {
        // Given: a handler with no pending requests
        var handler = handlerWithIdentityMapping(Map.of(ApiKeys.METADATA, DEFAULT_ROUTE));
        channel = new EmbeddedChannel(handler);

        // When: a response arrives for an unknown correlation ID
        var md = new MetadataResponseData().setControllerId(5);
        var responseFrame = new DecodedResponseFrame<>((short) 12, 9999, new ResponseHeaderData(), md);
        channel.writeOutbound(responseFrame);

        // Then: it passes through untranslated
        DecodedResponseFrame<?> out = channel.readOutbound();
        assertThat(out).isNotNull();
        assertThat(((MetadataResponseData) out.body()).controllerId()).isEqualTo(5);
    }
}
