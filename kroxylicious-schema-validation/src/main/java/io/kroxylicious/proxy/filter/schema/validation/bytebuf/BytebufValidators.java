/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter.schema.validation.bytebuf;

/**
 * Static factory methods for creating/getting ${@link BytebufValidator} instances
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
     * @param validateObjectKeysUnique optionally check if JSON Objects contain unique keys
     * @return validator
     */
    public static BytebufValidator jsonSyntaxValidator(boolean validateObjectKeysUnique) {
        return new JsonSyntaxBytebufValidator(validateObjectKeysUnique);
    }
}
