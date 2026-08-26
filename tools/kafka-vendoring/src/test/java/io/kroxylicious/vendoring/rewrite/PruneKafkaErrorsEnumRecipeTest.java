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
                                
                                  import java.util.HashMap;
                                  import java.util.Map;
                                  import java.util.function.Function;
                                
                                  import org.apache.kafka.common.errors.ApiException;
                                  import org.apache.kafka.common.errors.InvalidRequestException;
                                  import org.apache.kafka.common.errors.UnknownServerException;
                                
                                  /**
                                  * some javadoc
                                  */
                                  public enum Errors {
                                      UNKNOWN_SERVER_ERROR(-1, "Unexpected error", UnknownServerException::new),
                                      INVALID_REQUEST(42, "Invalid request", InvalidRequestException::new);
                                
                                      private final short code;
                                      private String message;
                                
                                      private final Function<String, ApiException> builder;
                                
                                      private static final Map<Class<?>, Errors> CLASS_TO_ERROR = new HashMap<>();
                                      private static final Map<Short, Errors> CODE_TO_ERROR = new HashMap<>();
                                
                                      private ApiException exception;
                                
                                      static {
                                          for (Errors error : Errors.values()) {
                                              if (CODE_TO_ERROR.put(error.code(), error) != null)
                                                  throw new ExceptionInInitializerError("Code " + error.code() + " for error " +
                                                          error + " has already been used");
                                
                                              if (error.exception != null)
                                                  CLASS_TO_ERROR.put(error.exception.getClass(), error);
                                          }
                                      }
                                
                                      Errors(int code, String defaultMessage, Function<String, ApiException> builder) {
                                          this.code = (short) code;
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
                                          if (exception != null)
                                              return exception.getMessage();
                                          return toString();
                                      }
                                
                                      public static Errors forCode(short code) {
                                          Errors error = CODE_TO_ERROR.get(code);
                                          if (error != null) {
                                              return error;
                                          } else {
                                              return UNKNOWN_SERVER_ERROR;
                                          }
                                      }
                                
                                      public void maybeThrow() {
                                          if (exception != null) {
                                              throw this.exception;
                                          }
                                      }
                                
                                      public String exceptionName() {
                                          return exception == null ? null : exception.getClass().getName();
                                      }
                                
                                      private static String toHtml() {
                                          final StringBuilder b = new StringBuilder();
                                          b.append("<table class=\\"data-table\\"><tbody>\\n");
                                          b.append("<tr>");
                                          b.append("<th>Error</th>\\n");
                                          b.append("<th>Code</th>\\n");
                                          b.append("<th>Retriable</th>\\n");
                                          b.append("<th>Description</th>\\n");
                                          b.append("</tr>\\n");
                                          b.append("</tbody></table>\\n");
                                          return b.toString();
                                      }
                                
                                      public static void main(String[] args) {
                                      }
                                  }
                                """,
                        """
                                package org.apache.kafka.common.protocol;
                                
                                import java.util.HashMap;
                                import java.util.Map;
                                
                                public enum Errors {
                                    UNKNOWN_SERVER_ERROR(-1, "Unexpected error"),
                                    INVALID_REQUEST(42, "Invalid request");
                                
                                    private final short code;
                                    private String message;
                                    private static final Map<Short, Errors> CODE_TO_ERROR = new HashMap<>();
                                
                                    static {
                                        for (Errors error : Errors.values()) {
                                            if (CODE_TO_ERROR.put(error.code(), error) != null)
                                                throw new ExceptionInInitializerError("Code " + error.code() + " for error " +
                                                        error + " has already been used");
                                        }
                                    }
                                
                                    Errors(int code, String defaultMessage) {
                                        this.code = (short) code;
                                        this.message = defaultMessage;
                                    }
                                
                                    public short code() {
                                        return code;
                                    }
                                
                                    public String message() {
                                        return this.message;
                                    }
                                
                                    public static Errors forCode(short code) {
                                        Errors error = CODE_TO_ERROR.get(code);
                                        if (error != null) {
                                            return error;
                                        } else {
                                            return UNKNOWN_SERVER_ERROR;
                                        }
                                    }
                                }
                                """));
    }

    @SuppressWarnings("java:S2699") // rewriteRun contains assertions
    @Test
    void shouldAtSeeTagFromJavaDoc() {
        rewriteRun(
                java(
                        """
                                package org.apache.kafka.common.protocol;
                                
                                /**
                                 * javadoc text
                                 * @see org.apache.kafka.common.network.SslTransportLayer
                                 */
                                public enum Errors {
                                    UNKNOWN_SERVER_ERROR,
                                    INVALID_REQUEST;
                                }
                                """,
                        """
                                package org.apache.kafka.common.protocol;
                                
                                /**
                                 * javadoc text
                                 */
                                public enum Errors {
                                    UNKNOWN_SERVER_ERROR,
                                    INVALID_REQUEST;
                                }
                                """));
    }
}