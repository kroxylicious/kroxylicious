/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.vendoring.rewrite;

import org.junit.jupiter.api.Test;
import org.openrewrite.java.JavaParser;
import org.openrewrite.test.RecipeSpec;
import org.openrewrite.test.RewriteTest;

import static org.openrewrite.java.Assertions.java;

class PruneKafkaErrorsEnumRecipeTest implements RewriteTest {

    @Override
    public void defaults(RecipeSpec spec) {
        spec.recipe(new PruneKafkaErrorsEnumRecipe())
                .parser(JavaParser.fromJavaVersion()
                        .classpath("kafka-clients"));
    }

    @SuppressWarnings("java:S2699") // rewriteRun contains assertions
    @Test
    void shouldRemoveExceptionsFromErrorsEnum() {
        rewriteRun(
                java(
                        """
                                package org.apache.kafka.common.protocol;

                                import java.util.function.Function;

                                import org.apache.kafka.common.errors.ApiException;
                                import org.apache.kafka.common.errors.InvalidRequestException;
                                import org.apache.kafka.common.errors.UnknownServerException;

                                public enum Errors {
                                    UNKNOWN_SERVER_ERROR(-1, "Unexpected error", UnknownServerException::new),
                                    INVALID_REQUEST(42, "Invalid request", InvalidRequestException::new);

                                    private final short code;
                                    private final String message;
                                    private final Function<String, ApiException> builder;

                                    Errors(int code, String defaultMessage, Function<String, ApiException> builder) {
                                        this.code = (short) code;
                                        this.message = defaultMessage;
                                        this.builder = builder;
                                    }

                                    public ApiException exception() {
                                        return builder.apply(message);
                                    }

                                    public ApiException exception(String message) {
                                        return builder.apply(message);
                                    }

                                    public static Errors forException(Throwable cause) {
                                        return UNKNOWN_SERVER_ERROR;
                                    }

                                    public short code() {
                                        return code;
                                    }

                                    public String message() {
                                        return message;
                                    }
                                }
                                """,
                        """
                                package org.apache.kafka.common.protocol;

                                public enum Errors {
                                    UNKNOWN_SERVER_ERROR(-1, "Unexpected error"),
                                    INVALID_REQUEST(42, "Invalid request");

                                    private final short code;
                                    private final String message;

                                    Errors(int code, String defaultMessage) {
                                        this.code = (short) code;
                                        this.message = defaultMessage;
                                    }

                                    public short code() {
                                        return code;
                                    }

                                    public String message() {
                                        return message;
                                    }
                                }
                                """));
    }
}