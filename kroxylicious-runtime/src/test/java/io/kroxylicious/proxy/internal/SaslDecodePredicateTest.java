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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SaslDecodePredicateTest {

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

    private SaslDecodePredicate predicate;

    @Test
    void testApiVersionAlwaysDecoded_SaslNotHandledAndBeforeDelegateSet() {
        givenSaslHandlingDisabled();
        assertPredicateTargetsRequestKey(ApiKeys.API_VERSIONS);
    }

    @Test
    void testApiVersionAlwaysDecoded_SaslHandledAndBeforeDelegateSet() {
        givenSaslHandlingEnabled();
        assertPredicateTargetsRequestKey(ApiKeys.API_VERSIONS);
    }

    @Test
    void testApiVersionAlwaysDecoded_SaslNotHandledAndDelegateSet() {
        givenSaslHandlingDisabled();
        givenDelegateTargetsNothing();
        assertPredicateTargetsRequestKey(ApiKeys.API_VERSIONS);
    }

    @Test
    void testApiVersionAlwaysDecoded_SaslHandledAndDelegateSet() {
        givenSaslHandlingEnabled();
        givenDelegateTargetsNothing();
        assertPredicateTargetsRequestKey(ApiKeys.API_VERSIONS);
    }

    @ParameterizedTest
    @EnumSource(ApiKeys.class)
    void testAllRequestKeysDecodedBeforeDelegateSet(ApiKeys keys) {
        givenSaslHandlingDisabled();
        assertPredicateTargetsRequestKey(keys);
        givenSaslHandlingEnabled();
        assertPredicateTargetsRequestKey(keys);
    }

    @ParameterizedTest
    @EnumSource(ApiKeys.class)
    void testAllResponseKeysDecodedBeforeDelegateSet(ApiKeys keys) {
        givenSaslHandlingDisabled();
        assertPredicateTargetsResponseKey(keys);
        givenSaslHandlingEnabled();
        assertPredicateTargetsResponseKey(keys);
    }

    @Test
    void testSaslRequestKeysTargetedForDecodeWithOffloadingAuth() {
        givenSaslHandlingEnabled();
        givenDelegateTargetsNothing();
        assertPredicateTargetsRequestKey(ApiKeys.SASL_AUTHENTICATE);
        assertPredicateTargetsRequestKey(ApiKeys.SASL_HANDSHAKE);
    }

    @EnumSource(value = ApiKeys.class, mode = EnumSource.Mode.EXCLUDE, names = { "SASL_AUTHENTICATE", "SASL_HANDSHAKE", "API_VERSIONS" })
    @ParameterizedTest
    void testNonSaslKeysNotTargetedForDecodeWithOffloadingAuthWhenPredicateDeniesThem(ApiKeys apiKeys) {
        givenSaslHandlingEnabled();
        givenDelegateTargetsNothing();
        assertPredicateDoesNotTargetRequestKey(apiKeys);
    }

    @EnumSource(value = ApiKeys.class, mode = EnumSource.Mode.EXCLUDE, names = { "API_VERSIONS" })
    @ParameterizedTest
    void testAllKeysCanBeDeniedByDelegateWhenNotOffloadingAuth(ApiKeys apiKeys) {
        givenSaslHandlingDisabled();
        givenDelegateTargetsNothing();
        assertPredicateDoesNotTargetRequestKey(apiKeys);
    }

    @EnumSource(value = ApiKeys.class, mode = EnumSource.Mode.EXCLUDE, names = { "API_VERSIONS" })
    @ParameterizedTest
    void testAllKeysCanBeTargetedByDelegateWhenNotOffloadingAuth(ApiKeys apiKeys) {
        givenSaslHandlingDisabled();
        givenDelegateTargetsAll();
        assertPredicateTargetsRequestKey(apiKeys);
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

    private void givenSaslHandlingDisabled() {
        givenPredicateWithHandleSasl(false);
    }

    private void givenSaslHandlingEnabled() {
        givenPredicateWithHandleSasl(true);
    }

    private void givenPredicateWithHandleSasl(boolean handleSasl) {
        predicate = new SaslDecodePredicate(handleSasl);
    }

    private void assertPredicateTargetsRequestKey(ApiKeys key) {
        assertTrue(predicate.shouldDecodeRequest(key, key.latestVersion()), "predicate did not target key " + key);
    }

    private void assertPredicateDoesNotTargetRequestKey(ApiKeys key) {
        assertFalse(predicate.shouldDecodeRequest(key, key.latestVersion()), "predicate unexpectedly targeted key " + key);
    }

    private void assertPredicateTargetsResponseKey(ApiKeys key) {
        assertTrue(predicate.shouldDecodeResponse(key, key.latestVersion()), "predicate did not target key " + key);
    }
}
