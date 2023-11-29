/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption.inband;

import java.security.SecureRandom;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AesGcmIvGeneratorTest {

    @Test
    void shouldHave12ByteSize() {
        assertEquals(12, new AesGcmIvGenerator(new SecureRandom()).sizeBytes());
    }

    @Test
    void shouldIncrement() {

        var generator = new AesGcmIvGenerator(random(0, 0, 0));
        byte[] iv = new byte[generator.sizeBytes()];
        generator.generateIv(iv);
        assertArrayEquals(new byte[]{ 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 }, iv);
        generator.generateIv(iv);
        assertArrayEquals(new byte[]{ 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, (byte) 1 }, iv);
        generator.generateIv(iv);
        assertArrayEquals(new byte[]{ 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, (byte) 2 }, iv);
    }

    @Test
    void shouldCarry1() {
        var generator = new AesGcmIvGenerator(random(Integer.MAX_VALUE, 0, 0));
        byte[] iv = new byte[generator.sizeBytes()];
        generator.generateIv(iv);
        assertArrayEquals(new byte[]{ 0, 0, 0, 0, 0, 0, 0, 0, (byte) 127, (byte) -1, (byte) -1, (byte) -1 }, iv);
        generator.generateIv(iv);
        assertArrayEquals(new byte[]{ 0, 0, 0, 0, 0, 0, 0, 1, (byte) -128, 0, 0, 0 }, iv);
        generator.generateIv(iv);
        assertArrayEquals(new byte[]{ 0, 0, 0, 0, 0, 0, 0, 1, (byte) -128, 0, 0, (byte) 1 }, iv);
    }

    @Test
    void shouldCarry2() {
        var generator = new AesGcmIvGenerator(random(Integer.MAX_VALUE, Integer.MAX_VALUE, 0));
        byte[] iv = new byte[generator.sizeBytes()];
        generator.generateIv(iv);
        assertArrayEquals(new byte[]{ 0, 0, 0, 0, (byte) 127, (byte) -1, (byte) -1, (byte) -1, (byte) 127, (byte) -1, (byte) -1, (byte) -1 }, iv);
        generator.generateIv(iv);
        assertArrayEquals(new byte[]{ 0, 0, 0, 1, (byte) -128, 0, 0, 0, (byte) -128, 0, 0, 0 }, iv);
        generator.generateIv(iv);
        assertArrayEquals(new byte[]{ 0, 0, 0, 1, (byte) -128, 0, 0, 0, (byte) -128, 0, 0, (byte) 1 }, iv);
    }

    @Test
    void shouldCarry3() {
        var generator = new AesGcmIvGenerator(random(Integer.MAX_VALUE, Integer.MAX_VALUE, Integer.MAX_VALUE));
        byte[] iv = new byte[generator.sizeBytes()];
        generator.generateIv(iv);
        assertArrayEquals(
                new byte[]{ (byte) 127, (byte) -1, (byte) -1, (byte) -1, (byte) 127, (byte) -1, (byte) -1, (byte) -1, (byte) 127, (byte) -1, (byte) -1, (byte) -1 }, iv);
        generator.generateIv(iv);
        assertArrayEquals(new byte[]{ (byte) -128, 0, 0, 0, (byte) -128, 0, 0, 0, (byte) -128, 0, 0, 0 }, iv);
        generator.generateIv(iv);
        assertArrayEquals(new byte[]{ (byte) -128, 0, 0, 0, (byte) -128, 0, 0, 0, (byte) -128, 0, 0, (byte) 1 }, iv);
    }

    private static SecureRandom random(int first, int second, int third) {
        return new SecureRandom() {
            private int count = 0;

            @Override
            public int nextInt() {
                var count = this.count;
                this.count++;
                if (count == 0) {
                    return first;
                }
                else if (count == 1) {
                    return second;
                }
                else if (count == 2) {
                    return third;
                }
                else {
                    throw new IllegalStateException();
                }
            }
        };
    }

    @Test
    void shouldBeDestroyable() {

        var generator = new AesGcmIvGenerator(random(0, 0, 0));
        generator.destroy();
        assertTrue(generator.isDestroyed());
        assertThrows(IllegalStateException.class, () -> generator.generateIv(null));
    }

}
