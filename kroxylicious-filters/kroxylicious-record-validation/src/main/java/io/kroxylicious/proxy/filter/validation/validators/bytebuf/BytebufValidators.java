/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter.validation.validators.bytebuf;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Static factory methods for creating/getting {@link BytebufValidator} instances
 */
public class BytebufValidators {

    private BytebufValidators() {

    }

    private static final AllValidBytebufValidator ALL_VALID = new AllValidBytebufValidator();

    /**
     * get validator that validates all {@link java.nio.ByteBuffer}s
     * @return validator
     */
    public static BytebufValidator allValid() {
        return ALL_VALID;
    }

    /**
     * get validator that validates null/empty {@link java.nio.ByteBuffer}s and then delegates non-null/non-empty {@link java.nio.ByteBuffer}s to a delegate
     * @param nullValid are null buffers valide\
     * @param emptyValid are empty buffers valid
     * @param delegate delegate to call if buffer is non-null/non-empty
     * @return validator
     */
    public static BytebufValidator nullEmptyValidator(boolean nullValid, boolean emptyValid, BytebufValidator delegate) {
        return new NullEmptyBytebufValidator(nullValid, emptyValid, delegate);
    }

    /**
     * get validator that validates if a non-null/non-empty buffer contains syntactically correct JSON
     *
     * @param validateObjectKeysUnique optionally check if JSON Objects contain unique keys
     * @return validator
     */
    public static BytebufValidator jsonSyntaxValidator(boolean validateObjectKeysUnique) {
        return new JsonSyntaxBytebufValidator(validateObjectKeysUnique);
    }

    /**
     * get validator that validates if a non-null/non-empty buffer contains data that matches a JSONSchema registered in the Schema Registry
     * @return validator
     */
    public static BytebufValidator jsonSchemaValidator(Map<String, Object> schemaResolverConfig, Long globalId) {
        return new JsonSchemaBytebufValidator(schemaResolverConfig, globalId);
    }

    /**
     * A chain of {@link BytebufValidators}.  Validators are executed in the order
     *  * they are defined.  Validation stops after the first validation failure.
     * @param elements list of validators
     *
     * @return BytebufValidator that will validate against all.
     */
    public static BytebufValidator chainOf(@NonNull
    List<BytebufValidator> elements) {
        Objects.nonNull(elements);

        if (elements.isEmpty()) {
            return allValid();
        } else {
            return new ChainingByteBufferValidator(elements);
        }
    }
}
