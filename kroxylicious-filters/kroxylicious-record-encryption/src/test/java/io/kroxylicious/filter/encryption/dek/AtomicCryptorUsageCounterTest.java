/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption.dek;

import org.assertj.core.api.AbstractBooleanAssert;
import org.junit.jupiter.api.Test;

import edu.umd.cs.findbugs.annotations.NonNull;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class AtomicCryptorUsageCounterTest {

    private final AtomicCryptorUsageCounter counter = new AtomicCryptorUsageCounter();

    @Test
    void testAcquireEncryptor() {
        assertCanAcquireEncryptorUsage();
    }

    @Test
    void testAcquireMultipleEncryptors() {
        assertCanAcquireEncryptorUsage();
        assertCanAcquireEncryptorUsage();
    }

    @Test
    void testAwaitsAllEncryptorsBeforeEnding() {
        assertCanAcquireEncryptorUsage();
        assertCanAcquireEncryptorUsage();
        assertNotEnded(counter.destroyForBoth());
        assertNotEnded(counter.releaseEncryptorUsage());
        assertEnded(counter.releaseEncryptorUsage());
    }

    @Test
    void testUnexpectedEncryptorRelease() {
        assertThatThrownBy(counter::releaseEncryptorUsage)
                                                          .isInstanceOf(IllegalStateException.class)
                                                          .hasMessageContaining("cannot decrement at START or END");
    }

    @Test
    void testUnexpectedEncryptorReleaseAfterAcquire() {
        assertCanAcquireEncryptorUsage();
        assertNotEnded(counter.releaseEncryptorUsage());
        assertThatThrownBy(counter::releaseEncryptorUsage)
                                                          .isInstanceOf(IllegalStateException.class)
                                                          .hasMessageContaining("cannot decrement at START or END");
    }

    @Test
    void testUnexpectedDecryptorReleaseAfterAcquire() {
        assertCanAcquireDecryptorUsage();
        assertNotEnded(counter.releaseDecryptorUsage());
        assertThatThrownBy(counter::releaseDecryptorUsage)
                                                          .isInstanceOf(IllegalStateException.class)
                                                          .hasMessageContaining("cannot decrement at START or END");
    }

    @Test
    void testUnexpectedDecryptorRelease() {
        assertThatThrownBy(counter::releaseDecryptorUsage)
                                                          .isInstanceOf(IllegalStateException.class)
                                                          .hasMessageContaining("cannot decrement at START or END");
    }

    @Test
    void testAcquireDecryptor() {
        assertCanAcquireDecryptorUsage();
    }

    @Test
    void testAcquireMultipleDecryptors() {
        assertCanAcquireDecryptorUsage();
        assertCanAcquireDecryptorUsage();
    }

    @Test
    void testAcquireAndReleaseEncryptor() {
        assertCanAcquireEncryptorUsage();
        assertNotEnded(counter.releaseEncryptorUsage());
    }

    @Test
    void testAcquireAfterReleaseEncryptor() {
        assertCanAcquireEncryptorUsage();
        assertNotEnded(counter.releaseEncryptorUsage());
        assertCanAcquireEncryptorUsage();
    }

    @Test
    void testAcquireAfterReleaseDecryptor() {
        assertCanAcquireDecryptorUsage();
        assertNotEnded(counter.releaseDecryptorUsage());
        assertCanAcquireDecryptorUsage();
    }

    @Test
    void testAcquireAndReleaseDecryptor() {
        assertCanAcquireDecryptorUsage();
        assertNotEnded(counter.releaseDecryptorUsage());
    }

    @Test
    void testAcquireAndReleaseAndDestroyEncryptor() {
        assertCanAcquireEncryptorUsage();
        assertNotEnded(counter.releaseEncryptorUsage());
        assertNotEnded(counter.destroyForEncrypt());
        assertCanNotAcquireEncryptorUsage();
    }

    @Test
    void testAcquireAndReleaseAndDestroyDecryptor() {
        assertCanAcquireDecryptorUsage();
        assertNotEnded(counter.releaseDecryptorUsage());
        assertNotEnded(counter.destroyForDecrypt());
        assertCanNotAcquireDecryptorUsage();
    }

    @Test
    void testCannotAcquireEncryptorAfterDestroyedForEncrypt() {
        assertNotEnded(counter.destroyForEncrypt());
        assertCanNotAcquireEncryptorUsage();
    }

    @Test
    void testCanAcquireDecryptorAfterDestroyedForEncrypt() {
        assertNotEnded(counter.destroyForEncrypt());
        assertCanAcquireDecryptorUsage();
    }

    @Test
    void testCanAcquireEncryptorAfterDestroyedForDecrypt() {
        assertNotEnded(counter.destroyForDecrypt());
        assertCanAcquireEncryptorUsage();
    }

    @Test
    void testAcquireAndDestroyAndReleaseEncryptor() {
        assertCanAcquireEncryptorUsage();
        assertNotEnded(counter.destroyForEncrypt());
        assertNotEnded(counter.releaseEncryptorUsage());
        assertCanNotAcquireEncryptorUsage();
    }

    @Test
    void testDestroyBothImmediately() {
        assertEnded(counter.destroyForBoth());
        assertCanNotAcquireEncryptorUsage();
        assertCanNotAcquireDecryptorUsage();
    }

    @Test
    void testDestroyEncryptFirst() {
        assertNotEnded(counter.destroyForEncrypt());
        assertEnded(counter.destroyForDecrypt());
    }

    @Test
    void testEndsAfterEncryptorsReleased() {
        assertCanAcquireEncryptorUsage();
        assertNotEnded(counter.destroyForEncrypt());
        assertNotEnded(counter.destroyForDecrypt());
        assertEnded(counter.releaseEncryptorUsage());
    }

    @Test
    void testEndsAfterDecryptorsReleased() {
        assertCanAcquireDecryptorUsage();
        assertNotEnded(counter.destroyForEncrypt());
        assertNotEnded(counter.destroyForDecrypt());
        assertEnded(counter.releaseDecryptorUsage());
    }

    @Test
    void testEndsAfterBothUsageTypesReleased_EncryptorsLast() {
        assertCanAcquireDecryptorUsage();
        assertCanAcquireEncryptorUsage();
        assertNotEnded(counter.destroyForBoth());
        assertNotEnded(counter.releaseDecryptorUsage());
        assertEnded(counter.releaseEncryptorUsage());
    }

    @Test
    void testEndsAfterBothUsageTypesReleased_DecryptorsLast() {
        assertCanAcquireDecryptorUsage();
        assertCanAcquireEncryptorUsage();
        assertNotEnded(counter.destroyForBoth());
        assertNotEnded(counter.releaseEncryptorUsage());
        assertEnded(counter.releaseDecryptorUsage());
    }

    private void assertCanAcquireDecryptorUsage() {
        assertPermitDecryptorUsage().isTrue();
    }

    private void assertCanNotAcquireDecryptorUsage() {
        assertPermitDecryptorUsage().isFalse();
    }

    private void assertCanAcquireEncryptorUsage() {
        assertPermitEncryptoUsage().isTrue();
    }

    private void assertCanNotAcquireEncryptorUsage() {
        assertPermitEncryptoUsage().isFalse();
    }

    @NonNull
    private AbstractBooleanAssert<?> assertPermitDecryptorUsage() {
        return assertThat(counter.acquireDecryptorUsage());
    }

    @NonNull
    private AbstractBooleanAssert<?> assertPermitEncryptoUsage() {
        return assertThat(counter.acquireEncryptorUsage());
    }

    private static void assertNotEnded(boolean result) {
        assertThat(result).isFalse();
    }

    private static void assertEnded(boolean result) {
        assertThat(result).isTrue();
    }

}
