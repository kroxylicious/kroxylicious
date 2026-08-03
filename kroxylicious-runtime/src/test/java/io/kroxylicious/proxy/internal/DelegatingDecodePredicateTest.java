/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal;

import org.apache.kafka.common.protocol.ApiKeys;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import io.kroxylicious.proxy.internal.codec.DecodePredicate;
import io.kroxylicious.proxy.router.Router;
import io.kroxylicious.proxy.router.RouterContext;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyShort;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class DelegatingDecodePredicateTest {

    private static final DecodePredicate TARGET_NOTHING = new DecodePredicate() {
        @Override
        public boolean shouldDecodeRequest(ApiKeys apiKey, short apiVersion) {
            return false;
        }

        @Override
        public boolean shouldDecodeResponse(ApiKeys apiKey, short apiVersion) {
            return false;
        }
    };

    private static final DecodePredicate TARGET_ALL = new DecodePredicate() {
        @Override
        public boolean shouldDecodeRequest(ApiKeys apiKey, short apiVersion) {
            return true;
        }

        @Override
        public boolean shouldDecodeResponse(ApiKeys apiKey, short apiVersion) {
            return true;
        }
    };

    private DelegatingDecodePredicate predicate;

    @Test
    void testApiVersionAlwaysDecoded_BeforeDelegateSet() {
        givenPredicate();
        assertPredicateTargetsRequestKey(ApiKeys.API_VERSIONS);
    }

    @Test
    void testApiVersionAlwaysDecoded_DelegateSet() {
        givenPredicate();
        givenDelegateTargetsNothing();
        assertPredicateTargetsRequestKey(ApiKeys.API_VERSIONS);
    }

    @ParameterizedTest
    @EnumSource(ApiKeys.class)
    void testAllRequestKeysDecodedBeforeDelegateSet(ApiKeys keys) {
        givenPredicate();
        assertPredicateTargetsRequestKey(keys);
    }

    @ParameterizedTest
    @EnumSource(ApiKeys.class)
    void testAllResponseKeysDecodedBeforeDelegateSet(ApiKeys keys) {
        givenPredicate();
        assertPredicateTargetsResponseKey(keys);
    }

    @EnumSource(value = ApiKeys.class, mode = EnumSource.Mode.EXCLUDE, names = { "API_VERSIONS" })
    @ParameterizedTest
    void testAllKeysCanBeDeniedByDelegate(ApiKeys apiKeys) {
        givenPredicate();
        givenDelegateTargetsNothing();
        assertPredicateDoesNotTargetRequestKey(apiKeys);
    }

    @EnumSource(value = ApiKeys.class, mode = EnumSource.Mode.EXCLUDE, names = { "API_VERSIONS" })
    @ParameterizedTest
    void testAllResponseKeysCanBeDeniedByDelegate(ApiKeys apiKeys) {
        givenPredicate();
        givenDelegateTargetsNothing();
        assertPredicateDoesNotTargetResponseKey(apiKeys);
    }

    @EnumSource(value = ApiKeys.class, mode = EnumSource.Mode.EXCLUDE, names = { "API_VERSIONS" })
    @ParameterizedTest
    void testAllKeysCanBeTargetedByDelegate(ApiKeys apiKeys) {
        givenPredicate();
        givenDelegateTargetsAll();
        assertPredicateTargetsRequestKey(apiKeys);
    }

    @EnumSource(value = ApiKeys.class, mode = EnumSource.Mode.EXCLUDE, names = { "API_VERSIONS" })
    @ParameterizedTest
    void testAllResponseKeysCanBeTargetedByDelegate(ApiKeys apiKeys) {
        givenPredicate();
        givenDelegateTargetsAll();
        assertPredicateTargetsResponseKey(apiKeys);
    }

    @Test
    void testApiVersionsResponseAlwaysDecoded_WhenDelegateTargetsNothing() {
        givenPredicate();
        givenDelegateTargetsNothing();
        assertPredicateTargetsResponseKey(ApiKeys.API_VERSIONS);
    }

    private void givenDelegateTargetsAll() {
        givenPredicateDelegateSet(TARGET_ALL);
    }

    private void givenDelegateTargetsNothing() {
        givenPredicateDelegateSet(TARGET_NOTHING);
    }

    private void givenPredicateDelegateSet(DecodePredicate predicate) {
        this.predicate.setDelegate(predicate);
    }

    private void givenPredicate() {
        predicate = new DelegatingDecodePredicate();
    }

    private void assertPredicateTargetsRequestKey(ApiKeys key) {
        assertTrue(predicate.shouldDecodeRequest(key, key.latestVersion()), "predicate did not target key " + key);
    }

    private void assertPredicateDoesNotTargetRequestKey(ApiKeys key) {
        assertFalse(predicate.shouldDecodeRequest(key, key.latestVersion()), "predicate unexpectedly targeted key " + key);
    }

    @Test
    void testRouterInterceptForcesDecodeForInterceptedKeys() {
        givenPredicate();
        givenDelegateTargetsNothing();

        Router router = mock(Router.class);
        RouterContext context = mock(RouterContext.class);
        when(router.shouldIntercept(any(), anyShort(), any())).thenAnswer(inv -> {
            ApiKeys key = inv.getArgument(0);
            return key == ApiKeys.FETCH;
        });

        predicate.setRouterInterceptionDelegate(router, context);
        assertPredicateTargetsRequestKey(ApiKeys.FETCH);
    }

    @Test
    void testRouterInterceptDoesNotForceDecodeForNonInterceptedKeys() {
        givenPredicate();
        givenDelegateTargetsNothing();

        Router router = mock(Router.class);
        RouterContext context = mock(RouterContext.class);
        when(router.shouldIntercept(any(), anyShort(), any())).thenAnswer(inv -> {
            ApiKeys key = inv.getArgument(0);
            return key == ApiKeys.FETCH;
        });

        predicate.setRouterInterceptionDelegate(router, context);
        assertPredicateDoesNotTargetRequestKey(ApiKeys.PRODUCE);
    }

    @Test
    void testRouterNeverInterceptDoesNotForceDecoding() {
        givenPredicate();
        givenDelegateTargetsNothing();

        Router router = mock(Router.class);
        RouterContext context = mock(RouterContext.class);
        when(router.shouldIntercept(any(), anyShort(), any())).thenReturn(false);

        predicate.setRouterInterceptionDelegate(router, context);
        assertPredicateDoesNotTargetRequestKey(ApiKeys.FETCH);
    }

    @Test
    void testRouterInterceptAffectsResponseDecoding() {
        givenPredicate();
        givenDelegateTargetsNothing();

        Router router = mock(Router.class);
        RouterContext context = mock(RouterContext.class);
        when(router.shouldIntercept(any(), anyShort(), any())).thenReturn(true);

        predicate.setRouterInterceptionDelegate(router, context);
        // Router intercept affects response decoding: router-internal requests (sendToAnyNode)
        // need decoded response bodies for node-ID translation and to complete pending futures.
        assertPredicateTargetsResponseKey(ApiKeys.METADATA);
    }

    private void assertPredicateTargetsResponseKey(ApiKeys key) {
        assertTrue(predicate.shouldDecodeResponse(key, key.latestVersion()), "predicate did not target key " + key);
    }

    private void assertPredicateDoesNotTargetResponseKey(ApiKeys key) {
        assertFalse(predicate.shouldDecodeResponse(key, key.latestVersion()), "predicate unexpectedly targeted key " + key);
    }
}
