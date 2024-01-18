/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption.dek;

import java.nio.ByteBuffer;

public class Use {

    public static <K, E> void useEncrypt(DekManager<K, E> dekManager, K kek) {
        dekManager.generateDek(kek).thenAccept(dek -> {
            try (var encryptor = dek.encryptor(1_000)) {

                ByteBuffer plaintext = null;
                ByteBuffer aad = null;

                ByteBuffer output = null;

                int startingPosition = output.position();
                CipherSpec cipherSpec = null; // TODO how do we get this? What owns the decision about the cipher?
                output.putInt(startingPosition, cipherSpec.persistentId());

                encryptor.encrypt(plaintext, aad, paramSize -> {
                    output.putInt(startingPosition + 4,
                            paramSize);
                    return output.slice(startingPosition + 8,
                            paramSize);
                }, (paramSize, ciphertextLength) -> {
                    return output.slice(startingPosition + 8 + paramSize,
                            ciphertextLength);
                });
                // TODO issues ^^:
                //      need to pre-allocate the ciphertext to be the right size
                //      should the caller be able to decide what cipher to use? Or should it be part of the dekManager
            }
        });
    }

    public static <K, E> void useDecrypt(DekManager<K, E> dekManager, E edek) {
        dekManager.decryptEdek(edek).thenAccept(dek -> {
            try (var decryptor = dek.decryptor()) {
                ByteBuffer input = null;
                ByteBuffer aad = null;
                ByteBuffer plaintext = null;

                int startingPosition = input.position();
                var cipherSpec = CipherSpec.fromPersistentId(input.getInt(startingPosition));

                int paramSize = input.getInt(startingPosition + 4);
                var parameterBuffer = input.slice(startingPosition + 8, paramSize);
                var ciphertext = input.slice(startingPosition + 8 + paramSize, input.remaining());

                decryptor.decrypt(ciphertext, aad, parameterBuffer, plaintext);
            }
        });
    }
}
