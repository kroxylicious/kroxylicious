/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption.inband;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.security.auth.DestroyFailedException;
import javax.security.auth.Destroyable;

import io.kroxylicious.filter.encryption.dek.ExhaustedDekException;

import org.junit.jupiter.api.Test;

import io.kroxylicious.kms.provider.kroxylicious.inmemory.InMemoryKms;
import io.kroxylicious.kms.provider.kroxylicious.inmemory.UnitTestingKmsService;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class KeyContextTest {

    @Test
    void shouldRejectInvalidNumberOfEncryptions() {
        assertThrows(IllegalArgumentException.class, () -> new KeyContext(null, 101011, 0, null));
        assertThrows(IllegalArgumentException.class, () -> new KeyContext(null, 101011, -1, null));
    }

    @Test
    void shouldRejectInvalidRemainingOfEncryptions() throws ExecutionException, InterruptedException {
        InMemoryKms kms = UnitTestingKmsService.newInstance().buildKms(new UnitTestingKmsService.Config());
        var kek = kms.generateKey();
        var pair = kms.generateDekPair(kek).get();
        ByteBuffer prefix = ByteBuffer.wrap(new byte[]{ 1, 2, 3 });
        var context = new KeyContext(prefix, 101011, 2,
                AesGcmEncryptor.forEncrypt(new AesGcmIvGenerator(new SecureRandom()), pair.dek()));

        assertThrows(IllegalArgumentException.class, () -> context.hasAtLeastRemainingEncryptions(0));
        assertThrows(IllegalArgumentException.class, () -> context.hasAtLeastRemainingEncryptions(-1));
    }

    @Test
    void testDestroy() {
        AtomicBoolean called = new AtomicBoolean();
        KeyContext.destroy(new Destroyable() {
            @Override
            public void destroy() throws DestroyFailedException {
                called.set(true);
                throw new DestroyFailedException("Eeek!");
            }
        });

        assertTrue(called.get());
    }

    @Test
    void testLifecycle() throws ExecutionException, InterruptedException, DestroyFailedException {
        InMemoryKms kms = UnitTestingKmsService.newInstance().buildKms(new UnitTestingKmsService.Config());
        var kek = kms.generateKey();
        var pair = kms.generateDekPair(kek).get();
        ByteBuffer prefix = ByteBuffer.wrap(new byte[]{ 1, 2, 3 });
        var context = new KeyContext(prefix, 101011, 2,
                AesGcmEncryptor.forEncrypt(new AesGcmIvGenerator(new SecureRandom()), pair.dek()));

        assertTrue(context.hasAtLeastRemainingEncryptions(1));

        ByteBuffer bb = ByteBuffer.wrap("hello, world!".getBytes(StandardCharsets.UTF_8));
        int size = context.encodedSize(bb.capacity());
        assertEquals(43, size);
        var output = ByteBuffer.allocate(size);

        // do an encryption
        context.encode(bb, output);
        assertFalse(output.hasRemaining());
        output.flip();
        bb.flip();
        context.recordEncryptions(1);

        assertTrue(context.hasAtLeastRemainingEncryptions(1));
        assertFalse(context.isExpiredForEncryption(101010));
        assertFalse(context.isExpiredForEncryption(101011));
        assertTrue(context.isExpiredForEncryption(101012));

        // do a second encryption
        context.encodedSize(bb.capacity());
        context.encode(bb, output);
        assertFalse(output.hasRemaining());
        output.flip();
        bb.flip();
        context.recordEncryptions(1);

        assertFalse(context.hasAtLeastRemainingEncryptions(1));
        context.encodedSize(bb.capacity());
        // try to do a third encryption
        var e = assertThrows(ExhaustedDekException.class, () -> context.encode(bb, output));
        assertEquals("No more encryptions", e.getMessage());

        assertFalse(context.isDestroyed());
        // destroy
        context.destroy();
        assertTrue(context.isDestroyed());
    }
}
