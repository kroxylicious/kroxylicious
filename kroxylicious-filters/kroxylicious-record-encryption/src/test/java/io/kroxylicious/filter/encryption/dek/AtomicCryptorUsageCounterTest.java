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
    public void testAcquireEncryptor() {
        assertCanAcquireEncryptorUsage();
    }

    @Test
    public void testAcquireMultipleEncryptors() {
        assertCanAcquireEncryptorUsage();
        assertCanAcquireEncryptorUsage();
    }

    @Test
    public void testAwaitsAllEncryptorsBeforeEnding() {
        assertCanAcquireEncryptorUsage();
        assertCanAcquireEncryptorUsage();
        assertNotEnded(counter.destroyForBoth());
        assertNotEnded(counter.releaseEncryptorUsage());
        assertEnded(counter.releaseEncryptorUsage());
    }

    @Test
    public void testUnexpectedEncryptorRelease() {
        assertThatThrownBy(counter::releaseEncryptorUsage)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("cannot decrement at START or END");
    }

    @Test
    public void testUnexpectedEncryptorReleaseAfterAcquire() {
        assertCanAcquireEncryptorUsage();
        assertNotEnded(counter.releaseEncryptorUsage());
        assertThatThrownBy(counter::releaseEncryptorUsage)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("cannot decrement at START or END");
    }

    @Test
    public void testUnexpectedDecryptorReleaseAfterAcquire() {
        assertCanAcquireDecryptorUsage();
        assertNotEnded(counter.releaseDecryptorUsage());
        assertThatThrownBy(counter::releaseDecryptorUsage)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("cannot decrement at START or END");
    }

    @Test
    public void testUnexpectedDecryptorRelease() {
        assertThatThrownBy(counter::releaseDecryptorUsage)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("cannot decrement at START or END");
    }

    @Test
    public void testAcquireDecryptor() {
        assertCanAcquireDecryptorUsage();
    }

    @Test
    public void testAcquireMultipleDecryptors() {
        assertCanAcquireDecryptorUsage();
        assertCanAcquireDecryptorUsage();
    }

    @Test
    public void testAcquireAndReleaseEncryptor() {
        assertCanAcquireEncryptorUsage();
        assertNotEnded(counter.releaseEncryptorUsage());
    }

    @Test
    public void testAcquireAfterReleaseEncryptor() {
        assertCanAcquireEncryptorUsage();
        assertNotEnded(counter.releaseEncryptorUsage());
        assertCanAcquireEncryptorUsage();
    }

    @Test
    public void testAcquireAfterReleaseDecryptor() {
        assertCanAcquireDecryptorUsage();
        assertNotEnded(counter.releaseDecryptorUsage());
        assertCanAcquireDecryptorUsage();
    }

    @Test
    public void testAcquireAndReleaseDecryptor() {
        assertCanAcquireDecryptorUsage();
        assertNotEnded(counter.releaseDecryptorUsage());
    }

    @Test
    public void testAcquireAndReleaseAndDestroyEncryptor() {
        assertCanAcquireEncryptorUsage();
        assertNotEnded(counter.releaseEncryptorUsage());
        assertNotEnded(counter.destroyForEncrypt());
        assertCanNotAcquireEncryptorUsage();
    }

    @Test
    public void testAcquireAndReleaseAndDestroyDecryptor() {
        assertCanAcquireDecryptorUsage();
        assertNotEnded(counter.releaseDecryptorUsage());
        assertNotEnded(counter.destroyForDecrypt());
        assertCanNotAcquireDecryptorUsage();
    }

    @Test
    public void testCannotAcquireEncryptorAfterDestroyedForEncrypt() {
        assertNotEnded(counter.destroyForEncrypt());
        assertCanNotAcquireEncryptorUsage();
    }

    @Test
    public void testCanAcquireDecryptorAfterDestroyedForEncrypt() {
        assertNotEnded(counter.destroyForEncrypt());
        assertCanAcquireDecryptorUsage();
    }

    @Test
    public void testCanAcquireEncryptorAfterDestroyedForDecrypt() {
        assertNotEnded(counter.destroyForDecrypt());
        assertCanAcquireEncryptorUsage();
    }

    @Test
    public void testAcquireAndDestroyAndReleaseEncryptor() {
        assertCanAcquireEncryptorUsage();
        assertNotEnded(counter.destroyForEncrypt());
        assertNotEnded(counter.releaseEncryptorUsage());
        assertCanNotAcquireEncryptorUsage();
    }

    @Test
    public void testDestroyBothImmediately() {
        assertEnded(counter.destroyForBoth());
        assertCanNotAcquireEncryptorUsage();
        assertCanNotAcquireDecryptorUsage();
    }

    @Test
    public void testDestroyEncryptFirst() {
        assertNotEnded(counter.destroyForEncrypt());
        assertEnded(counter.destroyForDecrypt());
    }

    @Test
    public void testEndsAfterEncryptorsReleased() {
        assertCanAcquireEncryptorUsage();
        assertNotEnded(counter.destroyForEncrypt());
        assertNotEnded(counter.destroyForDecrypt());
        assertEnded(counter.releaseEncryptorUsage());
    }

    @Test
    public void testEndsAfterDecryptorsReleased() {
        assertCanAcquireDecryptorUsage();
        assertNotEnded(counter.destroyForEncrypt());
        assertNotEnded(counter.destroyForDecrypt());
        assertEnded(counter.releaseDecryptorUsage());
    }

    @Test
    public void testEndsAfterBothUsageTypesReleased_EncryptorsLast() {
        assertCanAcquireDecryptorUsage();
        assertCanAcquireEncryptorUsage();
        assertNotEnded(counter.destroyForBoth());
        assertNotEnded(counter.releaseDecryptorUsage());
        assertEnded(counter.releaseEncryptorUsage());
    }

    @Test
    public void testEndsAfterBothUsageTypesReleased_DecryptorsLast() {
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