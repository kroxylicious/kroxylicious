/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.routing;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.errors.UnknownServerException;
import org.apache.kafka.common.message.FetchRequestData;
import org.apache.kafka.common.message.MetadataRequestData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import io.kroxylicious.proxy.authentication.Subject;
import io.kroxylicious.proxy.config.TargetCluster;
import io.kroxylicious.proxy.frame.DecodedRequestFrame;
import io.kroxylicious.proxy.internal.ClientConnectionStateMachine;
import io.kroxylicious.proxy.router.Router;
import io.kroxylicious.proxy.router.RouterResponse;
import io.kroxylicious.proxy.topology.VirtualNode;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

@ExtendWith(MockitoExtension.class)
class RouterContextImplTest {

    private static final String DEFAULT_ROUTE = "default";

    @Mock
    private Router router;

    @Mock
    private ClientConnectionStateMachine ccsm;

    private DecodedRequestFrame<?> clientFrame;
    private NodeIdMapping nodeIdMapping;
    private Map<String, RouteDescriptor> routes;

    @BeforeEach
    void setUp() {
        clientFrame = new DecodedRequestFrame<>((short) 12, 100, true,
                new RequestHeaderData(), new MetadataRequestData());
        nodeIdMapping = new IdentityNodeIdMapping(DEFAULT_ROUTE);
        routes = Map.of(DEFAULT_ROUTE, new RouteDescriptor(
                DEFAULT_ROUTE, 0,
                new TargetCluster("localhost:9092", Optional.empty()),
                null, List.of()));
    }

    private RouterContextImpl createContext() {
        return createContext(null);
    }

    private RouterContextImpl createContext(Integer endpointVirtualNodeId) {
        var handler = new RouterDispatchHandler(router, routes, Map.of(), ccsm, nodeIdMapping, null);
        return new RouterContextImpl(
                clientFrame, handler, "test-session", Subject.anonymous(),
                endpointVirtualNodeId);
    }

    @Test
    void anyNodeShouldReturnVirtualNodeForKnownRoute() {
        // Given
        var ctx = createContext();

        // When
        var node = ctx.anyNode(DEFAULT_ROUTE);

        // Then
        assertThat(node).isInstanceOf(VirtualNodeImpl.class);
        assertThat(((VirtualNodeImpl) node).route()).isEqualTo(DEFAULT_ROUTE);
        assertThat(((VirtualNodeImpl) node).nodeId()).isNull();
    }

    @Test
    void virtualNodeShouldBeEmptyForBootstrapConnection() {
        // Given: no endpoint virtual node ID (bootstrap connection)
        var ctx = createContext(null);

        // When / Then
        assertThat(ctx.virtualNode()).isEmpty();
    }

    @Test
    void virtualNodeShouldBePresentForBrokerConnection() {
        // Given: endpoint virtual node ID 0 (broker-specific connection)
        var ctx = createContext(0);

        // When
        var vn = ctx.virtualNode();

        // Then: IdentityNodeIdMapping: fromVirtual(0) → RouteAndNode(DEFAULT_ROUTE, 0)
        assertThat(vn).isPresent();
        assertThat(((VirtualNodeImpl) vn.get()).route()).isEqualTo(DEFAULT_ROUTE);
        assertThat(((VirtualNodeImpl) vn.get()).nodeId()).isZero();
    }

    @Test
    void anyNodeShouldThrowForUnknownRoute() {
        // Given
        var ctx = createContext();

        // When / Then
        assertThatThrownBy(() -> ctx.anyNode("no-such-route"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unknown route");
    }

    @Test
    void nodeForIdShouldConvertVirtualNodeIdToVirtualNode() {
        // Given
        var ctx = createContext();

        // When
        var node = ctx.nodeForId(3);

        // Then: IdentityNodeIdMapping maps virtual ID directly to target ID on the single route
        assertThat(node).isInstanceOf(VirtualNodeImpl.class);
        assertThat(((VirtualNodeImpl) node).route()).isEqualTo(DEFAULT_ROUTE);
        assertThat(((VirtualNodeImpl) node).nodeId()).isEqualTo(3);
    }

    @Test
    void respondWithBodyShouldBuildRespondWithResult() {
        // Given
        var ctx = createContext();
        var body = new MetadataRequestData();

        // When
        var stage = ctx.respondWith(body);
        RouterResponse response = stage.build();

        // Then
        assertThat(response).isInstanceOf(RouterResponseImpl.RespondWith.class);
        var rw = (RouterResponseImpl.RespondWith) response;
        assertThat(rw.body()).isSameAs(body);
        assertThat(rw.header()).isNull();
        assertThat(rw.closeConnection()).isFalse();
    }

    @Test
    void respondWithCloseConnectionShouldSetFlag() {
        // Given
        var ctx = createContext();
        var body = new MetadataRequestData();

        // When
        RouterResponse response = ctx.respondWith(body).withCloseConnection().build();

        // Then
        assertThat(response).isInstanceOf(RouterResponseImpl.RespondWith.class);
        assertThat(((RouterResponseImpl.RespondWith) response).closeConnection()).isTrue();
    }

    @Test
    void respondWithErrorShouldBuildErrorResult() {
        // Given
        var ctx = createContext();
        var header = new RequestHeaderData();
        var request = new MetadataRequestData();
        ApiException exception = new UnknownServerException("test error");

        // When
        RouterResponse response = ctx.respondWithError(header, request, exception).build();

        // Then
        assertThat(response).isInstanceOf(RouterResponseImpl.RespondWithError.class);
        var rwe = (RouterResponseImpl.RespondWithError) response;
        assertThat(rwe.exception()).isSameAs(exception);
        assertThat(rwe.closeConnection()).isFalse();
    }

    @Test
    void respondWithoutReplyShouldBuildNoReplyResult() {
        // Given
        var ctx = createContext();

        // When
        RouterResponse response = ctx.respondWithoutReply().build();

        // Then
        assertThat(response).isInstanceOf(RouterResponseImpl.RespondWithoutReply.class);
        assertThat(((RouterResponseImpl.RespondWithoutReply) response).closeConnection()).isFalse();
    }

    @Test
    void sessionIdAndSubjectShouldReturnConstructorValues() {
        // Given
        var ctx = createContext();

        // When / Then
        assertThat(ctx.sessionId()).isEqualTo("test-session");
        assertThat(ctx.authenticatedSubject()).isEqualTo(Subject.anonymous());
    }

    @Test
    void sendRequestShouldThrowForUnrecognisedVirtualNodeType() {
        // Given
        var ctx = createContext();
        var unknownNode = mock(VirtualNode.class);
        RequestHeaderData header = new RequestHeaderData();
        FetchRequestData data = new FetchRequestData();

        // When / Then
        assertThatThrownBy(() -> ctx.sendRequest(unknownNode, header, data))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unrecognised VirtualNode type");
    }

    @Test
    void respondWithTwoArgOverloadShouldSetExplicitHeader() {
        // Given
        var ctx = createContext();
        var body = new MetadataRequestData();
        var header = new ResponseHeaderData().setCorrelationId(999);

        // When
        RouterResponse response = ctx.respondWith(header, body).build();

        // Then
        assertThat(response).isInstanceOf(RouterResponseImpl.RespondWith.class);
        var rw = (RouterResponseImpl.RespondWith) response;
        assertThat(rw.header()).isSameAs(header);
        assertThat(rw.body()).isSameAs(body);
        assertThat(rw.closeConnection()).isFalse();
    }

    @Test
    void respondWithErrorWithCloseConnectionShouldSetFlag() {
        // Given
        var ctx = createContext();
        ApiException exception = new UnknownServerException("test error");

        // When
        RouterResponse response = ctx.respondWithError(new RequestHeaderData(), new MetadataRequestData(), exception)
                .withCloseConnection().build();

        // Then
        assertThat(response).isInstanceOf(RouterResponseImpl.RespondWithError.class);
        assertThat(((RouterResponseImpl.RespondWithError) response).closeConnection()).isTrue();
    }

    @Test
    void respondWithoutReplyWithCloseConnectionShouldSetFlag() {
        // Given
        var ctx = createContext();

        // When
        RouterResponse response = ctx.respondWithoutReply().withCloseConnection().build();

        // Then
        assertThat(response).isInstanceOf(RouterResponseImpl.RespondWithoutReply.class);
        assertThat(((RouterResponseImpl.RespondWithoutReply) response).closeConnection()).isTrue();
    }

    @Test
    void builderCompletedShouldReturnAlreadyCompletedFuture() {
        // Given
        var ctx = createContext();

        // When
        var stage = ctx.respondWithoutReply().completed();

        // Then
        assertThat(stage.toCompletableFuture()).isCompletedWithValueMatching(
                r -> r instanceof RouterResponseImpl.RespondWithoutReply rwr && !rwr.closeConnection());
    }
}
