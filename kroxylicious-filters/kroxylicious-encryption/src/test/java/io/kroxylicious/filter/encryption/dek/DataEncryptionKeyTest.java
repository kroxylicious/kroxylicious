/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption.dek;

import javax.crypto.SecretKey;
import javax.security.auth.DestroyFailedException;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;

class DataEncryptionKeyTest {

    @Test
    void destroyUnusedDek() throws DestroyFailedException {
        // Given
        SecretKey mock = mock(SecretKey.class);
        doReturn(false).when(mock).isDestroyed();
        doNothing().when(mock).destroy();
        var dek = new DataEncryptionKey<>("edek", mock, 100);

        // When
        dek.destroy();

        // Then
        Mockito.verify(mock).destroy();

        dek.destroy();
    }

    @Test
    void destroy1Encryptor() throws DestroyFailedException {
        // Given
        SecretKey mock = mock(SecretKey.class);
        doReturn(false).when(mock).isDestroyed();
        doNothing().when(mock).destroy();
        var dek = new DataEncryptionKey<>("edek", mock, 100);
        var cryptor = dek.encryptor(100);

        // When
        dek.destroy();

        // When
        Mockito.verify(mock, never()).destroy();

        dek.destroy(); // try destroying again

        cryptor.close();

        Mockito.verify(mock).destroy();
    }

    @Test
    void destroy2Encryptor() throws DestroyFailedException {
        // Given
        SecretKey mock = mock(SecretKey.class);
        doReturn(false).when(mock).isDestroyed();
        doNothing().when(mock).destroy();
        var dek = new DataEncryptionKey<>("edek", mock, 100);
        var cryptor1 = dek.encryptor(50);
        var cryptor2 = dek.encryptor(50);

        // When
        dek.destroy();
        dek.destroy();

        // When
        Mockito.verify(mock, never()).destroy();

        cryptor1.close();

        Mockito.verify(mock, never()).destroy();

        cryptor2.close();

        Mockito.verify(mock).destroy();
    }

    @Test
    void destroy2EncryptorMultiClose() throws DestroyFailedException {
        // Given
        SecretKey mock = mock(SecretKey.class);
        doReturn(false).when(mock).isDestroyed();
        doNothing().when(mock).destroy();
        var dek = new DataEncryptionKey<>("edek", mock, 100);
        var cryptor1 = dek.encryptor(50);
        var cryptor2 = dek.encryptor(50);

        // When
        dek.destroy();
        dek.destroy();

        // When
        Mockito.verify(mock, never()).destroy();

        cryptor1.close();
        cryptor1.close();

        Mockito.verify(mock, never()).destroy();

        cryptor2.close();

        Mockito.verify(mock).destroy();

        cryptor1.close();
    }

    @Test
    void destroy1Decryptor() throws DestroyFailedException {
        // Given
        SecretKey mock = mock(SecretKey.class);
        doReturn(false).when(mock).isDestroyed();
        doNothing().when(mock).destroy();
        var dek = new DataEncryptionKey<>("edek", mock, 0);
        var cryptor = dek.decryptor();

        // When
        dek.destroy();

        // When
        Mockito.verify(mock, never()).destroy();

        dek.destroy(); // try destroying again

        cryptor.close();

        Mockito.verify(mock).destroy();
    }

    @Test
    void destroy2Decryptor() throws DestroyFailedException {
        // Given
        SecretKey mock = mock(SecretKey.class);
        doReturn(false).when(mock).isDestroyed();
        doNothing().when(mock).destroy();
        var dek = new DataEncryptionKey<>("edek", mock, 0);
        var cryptor1 = dek.decryptor();
        var cryptor2 = dek.decryptor();

        // When
        dek.destroy();
        dek.destroy();

        // When
        Mockito.verify(mock, never()).destroy();

        cryptor1.close();

        Mockito.verify(mock, never()).destroy();

        cryptor2.close();

        Mockito.verify(mock).destroy();
    }

    @Test
    void destroy2DecryptorMultiClose() throws DestroyFailedException {
        // Given
        SecretKey mock = mock(SecretKey.class);
        doReturn(false).when(mock).isDestroyed();
        doNothing().when(mock).destroy();
        var dek = new DataEncryptionKey<>("edek", mock, 0);
        var cryptor1 = dek.decryptor();
        var cryptor2 = dek.decryptor();

        // When
        dek.destroy();
        dek.destroy();

        // When
        Mockito.verify(mock, never()).destroy();

        cryptor1.close();
        cryptor1.close();

        Mockito.verify(mock, never()).destroy();

        cryptor2.close();

        Mockito.verify(mock).destroy();

        cryptor1.close();
    }

    @Test
    void destroy1Encryptor1Decryptor() throws DestroyFailedException {
        // Given
        SecretKey mock = mock(SecretKey.class);
        doReturn(false).when(mock).isDestroyed();
        doNothing().when(mock).destroy();
        var dek = new DataEncryptionKey<>("edek", mock, 100);
        var cryptor1 = dek.encryptor(50);
        var cryptor2 = dek.decryptor();

        // When
        dek.destroy();
        dek.destroy();

        // When
        Mockito.verify(mock, never()).destroy();

        cryptor1.close();

        Mockito.verify(mock, never()).destroy();

        cryptor2.close();

        Mockito.verify(mock).destroy();

        cryptor2.close();
    }

}