/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption;

import java.nio.ByteBuffer;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;

import io.kroxylicious.kms.service.DekPair;
import io.kroxylicious.kms.service.DestroyableRawSecretKey;
import io.kroxylicious.kms.service.Kms;
import io.kroxylicious.kms.service.KmsService;
import io.kroxylicious.kms.service.Serde;

import edu.umd.cs.findbugs.annotations.NonNull;

public class FixedDekKmsService implements KmsService<Void, ByteBuffer, ByteBuffer> {

    private static final ByteBuffer KEK_ID = ByteBuffer.wrap(new byte[]{ 1, 2, 3 });
    private final SecretKey dek;
    private final ByteBuffer edek;
    private final FixedEdekKms fixedEdekKms = new FixedEdekKms();

    public FixedDekKmsService(int keysize) {
        try {
            KeyGenerator generator = KeyGenerator.getInstance("AES");
            generator.init(keysize);
            dek = DestroyableRawSecretKey.toDestroyableKey(generator.generateKey());
            edek = ByteBuffer.wrap(dek.getEncoded()); // not concerned with wrapping/unwrapping
        }
        catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void initialize(@NonNull Void config) {
        // this KMS requires no config
    }

    @NonNull
    @Override
    public Kms<ByteBuffer, ByteBuffer> buildKms() {
        return fixedEdekKms;
    }

    public ByteBuffer getKekId() {
        return KEK_ID;
    }

    public Serde<ByteBuffer> edekSerde() {
        return fixedEdekKms.edekSerde();
    }

    private class FixedEdekKms implements Kms<ByteBuffer, ByteBuffer> {

        @NonNull
        @Override
        public CompletionStage<DekPair<ByteBuffer>> generateDekPair(@NonNull ByteBuffer kekRef) {
            return CompletableFuture.completedFuture(new DekPair<>(edek, dek));
        }

        @NonNull
        @Override
        public CompletionStage<SecretKey> decryptEdek(@NonNull ByteBuffer edek) {
            return CompletableFuture.completedFuture(dek);
        }

        @NonNull
        @Override
        public Serde<ByteBuffer> edekSerde() {
            return new Serde<>() {
                @Override
                public int sizeOf(ByteBuffer object) {
                    return object.remaining();
                }

                @Override
                public void serialize(ByteBuffer object, @NonNull ByteBuffer buffer) {
                    var p0 = object.position();
                    buffer.put(object);
                    object.position(p0);
                }

                @Override
                public ByteBuffer deserialize(@NonNull ByteBuffer buffer) {
                    throw new UnsupportedOperationException();
                }
            };
        }

        @NonNull
        @Override
        public CompletionStage<ByteBuffer> resolveAlias(@NonNull String alias) {
            return CompletableFuture.completedFuture(KEK_ID);
        }
    }

}
