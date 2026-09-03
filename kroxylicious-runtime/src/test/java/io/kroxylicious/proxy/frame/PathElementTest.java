/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.frame;

import java.util.concurrent.CompletableFuture;

import org.junit.jupiter.api.Test;

import io.kroxylicious.proxy.frame.PathElement.ClientOrigin;
import io.kroxylicious.proxy.frame.PathElement.FilterOriginator;
import io.kroxylicious.proxy.frame.PathElement.Route;
import io.kroxylicious.proxy.frame.PathElement.RouterOriginator;

import static org.assertj.core.api.Assertions.assertThat;

class PathElementTest {

    private static final Route OUTER_ROUTE = new Route("outer", ClientOrigin.INSTANCE);
    private static final Route INNER_ROUTE = new Route("inner", OUTER_ROUTE);

    // --- routePosition() ---

    @Test
    void routePositionOfClientOriginIsItself() {
        // Given/When/Then
        assertThat(ClientOrigin.INSTANCE.routePosition()).isSameAs(ClientOrigin.INSTANCE);
    }

    @Test
    void routePositionOfRouteIsItself() {
        // Given/When/Then
        assertThat(INNER_ROUTE.routePosition()).isSameAs(INNER_ROUTE);
    }

    @Test
    void routePositionOfFilterOriginatorIsItsAnchor() {
        // Given
        var originator = new FilterOriginator("my-filter", 0, new CompletableFuture<>(), INNER_ROUTE);

        // When
        var routePosition = originator.routePosition();

        // Then
        assertThat(routePosition).isSameAs(INNER_ROUTE);
    }

    @Test
    void routePositionOfRouterOriginatorIsItsAnchor() {
        // Given
        var originator = new RouterOriginator(new CompletableFuture<>(), INNER_ROUTE);

        // When
        var routePosition = originator.routePosition();

        // Then
        assertThat(routePosition).isSameAs(INNER_ROUTE);
    }

    // --- pendingPromise() ---

    @Test
    void pendingPromiseOfClientOriginIsEmpty() {
        // Given/When/Then
        assertThat(ClientOrigin.INSTANCE.pendingPromise()).isEmpty();
    }

    @Test
    void pendingPromiseOfRouteIsEmpty() {
        // Given/When/Then
        assertThat(INNER_ROUTE.pendingPromise()).isEmpty();
    }

    @Test
    void pendingPromiseOfFilterOriginatorIsItsPromise() {
        // Given
        var promise = new CompletableFuture<>();
        var originator = new FilterOriginator("my-filter", 0, promise, INNER_ROUTE);

        // When
        var pendingPromise = originator.pendingPromise();

        // Then
        assertThat(pendingPromise).contains(promise);
    }

    @Test
    void pendingPromiseOfRouterOriginatorIsItsPromise() {
        // Given
        var promise = new CompletableFuture<>();
        var originator = new RouterOriginator(promise, INNER_ROUTE);

        // When
        var pendingPromise = originator.pendingPromise();

        // Then
        assertThat(pendingPromise).contains(promise);
    }

    // --- graft() ---

    @Test
    void graftOntoRoutePositionDiscardsOriginalPositionOutright() {
        // Given
        var newPosition = new Route("resolved", ClientOrigin.INSTANCE);

        // When
        var grafted = INNER_ROUTE.graft(newPosition);

        // Then
        assertThat(grafted).isSameAs(newPosition);
    }

    @Test
    void graftOntoClientOriginDiscardsItOutright() {
        // Given
        var newPosition = new Route("resolved", ClientOrigin.INSTANCE);

        // When
        var grafted = ClientOrigin.INSTANCE.graft(newPosition);

        // Then
        assertThat(grafted).isSameAs(newPosition);
    }

    @Test
    void graftOntoFilterOriginatorPreservesItsIdentity() {
        // Given
        var promise = new CompletableFuture<>();
        var originator = new FilterOriginator("my-filter", 3, promise, ClientOrigin.INSTANCE);
        var newPosition = new Route("resolved", ClientOrigin.INSTANCE);

        // When
        var grafted = originator.graft(newPosition);

        // Then
        assertThat(grafted).isEqualTo(new FilterOriginator("my-filter", 3, promise, newPosition));
    }

    @Test
    void graftOntoRouterOriginatorPreservesItsIdentity() {
        // Given
        var promise = new CompletableFuture<>();
        var originator = new RouterOriginator(promise, OUTER_ROUTE);
        var newPosition = new Route("resolved", OUTER_ROUTE);

        // When
        var grafted = originator.graft(newPosition);

        // Then
        assertThat(grafted).isEqualTo(new RouterOriginator(promise, newPosition));
    }

    // --- Originator.reposition() ---

    @Test
    void repositionOfFilterOriginatorReturnsFilterOriginator() {
        // Given
        var promise = new CompletableFuture<>();
        var originator = new FilterOriginator("my-filter", 0, promise, ClientOrigin.INSTANCE);
        var newPosition = new Route("resolved", ClientOrigin.INSTANCE);

        // When
        var repositioned = originator.reposition(newPosition);

        // Then
        assertThat(repositioned).isEqualTo(new FilterOriginator("my-filter", 0, promise, newPosition));
    }

    @Test
    void repositionOfRouterOriginatorReturnsRouterOriginator() {
        // Given
        var promise = new CompletableFuture<>();
        var originator = new RouterOriginator(promise, OUTER_ROUTE);
        var newPosition = new Route("resolved", OUTER_ROUTE);

        // When
        var repositioned = originator.reposition(newPosition);

        // Then
        assertThat(repositioned).isEqualTo(new RouterOriginator(promise, newPosition));
    }

    // --- RoutePosition.parent() ---

    @Test
    void clientOriginIsItsOwnParent() {
        // Given/When/Then
        assertThat(ClientOrigin.INSTANCE.parent()).isSameAs(ClientOrigin.INSTANCE);
    }

    @Test
    void routeParentIsTheGivenParent() {
        // Given/When/Then
        assertThat(INNER_ROUTE.parent()).isEqualTo(OUTER_ROUTE);
    }

    // --- RoutePosition.isAncestorOfOrSameAs() ---

    @Test
    void clientOriginIsAncestorOfADeeplyNestedRoute() {
        // Given/When/Then
        assertThat(ClientOrigin.INSTANCE.isAncestorOfOrSameAs(INNER_ROUTE)).isTrue();
    }

    @Test
    void clientOriginIsAncestorOfAnOriginatorAnchoredAnywhere() {
        // Given
        var originator = new FilterOriginator("my-filter", 0, new CompletableFuture<>(), INNER_ROUTE);

        // When/Then
        assertThat(ClientOrigin.INSTANCE.isAncestorOfOrSameAs(originator)).isTrue();
    }

    @Test
    void routeIsAncestorOfItself() {
        // Given/When/Then
        assertThat(INNER_ROUTE.isAncestorOfOrSameAs(INNER_ROUTE)).isTrue();
    }

    @Test
    void routeIsAncestorOfARouteNestedBeneathIt() {
        // Given
        var deeplyNested = new Route("deepest", INNER_ROUTE);

        // When/Then
        assertThat(OUTER_ROUTE.isAncestorOfOrSameAs(deeplyNested)).isTrue();
    }

    @Test
    void routeIsNotAncestorOfASiblingRoute() {
        // Given
        var sibling = new Route("sibling", ClientOrigin.INSTANCE);

        // When/Then
        assertThat(OUTER_ROUTE.isAncestorOfOrSameAs(sibling)).isFalse();
    }

    @Test
    void routeIsNotAncestorOfClientOrigin() {
        // Given/When/Then
        assertThat(INNER_ROUTE.isAncestorOfOrSameAs(ClientOrigin.INSTANCE)).isFalse();
    }

    @Test
    void routeIsAncestorOfAnOriginatorAnchoredBeneathIt() {
        // Given
        var originator = new RouterOriginator(new CompletableFuture<>(), INNER_ROUTE);

        // When/Then
        assertThat(OUTER_ROUTE.isAncestorOfOrSameAs(originator)).isTrue();
    }

    @Test
    void routeIsNotAncestorOfAnOriginatorAnchoredOnAnUnrelatedRoute() {
        // Given
        var unrelatedRoute = new Route("unrelated", ClientOrigin.INSTANCE);
        var originator = new RouterOriginator(new CompletableFuture<>(), unrelatedRoute);

        // When/Then
        assertThat(OUTER_ROUTE.isAncestorOfOrSameAs(originator)).isFalse();
    }

    // --- describe() ---

    @Test
    void describeOfClientOriginIsEmpty() {
        // Given/When/Then
        assertThat(ClientOrigin.INSTANCE.describe()).isEmpty();
    }

    @Test
    void describeOfSingleRouteIsItsName() {
        // Given/When/Then
        assertThat(OUTER_ROUTE.describe()).isEqualTo("outer");
    }

    @Test
    void describeOfNestedRouteIsSlashSeparatedFromRoot() {
        // Given/When/Then
        assertThat(INNER_ROUTE.describe()).isEqualTo("outer/inner");
    }

    @Test
    void describeOfFilterOriginatorOnClientOriginIsFilterNameAndOrdinal() {
        // Given
        var originator = new FilterOriginator("marker-filter", 2, new CompletableFuture<>(), ClientOrigin.INSTANCE);

        // When/Then
        assertThat(originator.describe()).isEqualTo("marker-filter[2]");
    }

    @Test
    void describeOfFilterOriginatorOnARouteIncludesTheRoute() {
        // Given
        var originator = new FilterOriginator("marker-filter", 0, new CompletableFuture<>(), INNER_ROUTE);

        // When/Then
        assertThat(originator.describe()).isEqualTo("outer/inner/marker-filter[0]");
    }

    @Test
    void describeOfRouterOriginatorIncludesTheRoute() {
        // Given
        var originator = new RouterOriginator(new CompletableFuture<>(), INNER_ROUTE);

        // When/Then
        assertThat(originator.describe()).isEqualTo("outer/inner/<router>");
    }

    @Test
    void toStringOfRouteDelegatesToDescribe() {
        // Given/When/Then
        assertThat(INNER_ROUTE).hasToString(INNER_ROUTE.describe());
    }

    @Test
    void toStringOfFilterOriginatorDelegatesToDescribe() {
        // Given
        var originator = new FilterOriginator("marker-filter", 0, new CompletableFuture<>(), INNER_ROUTE);

        // When/Then
        assertThat(originator).hasToString(originator.describe());
    }

    // --- equality ---

    @Test
    void separatelyConstructedClientOriginsAreEqual() {
        // Given/When/Then
        assertThat(new ClientOrigin()).isEqualTo(ClientOrigin.INSTANCE);
    }
}
