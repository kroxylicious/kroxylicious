/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.migrations.v0_24;

import org.junit.jupiter.api.Test;
import org.openrewrite.java.JavaParser;
import org.openrewrite.test.RecipeSpec;
import org.openrewrite.test.RewriteTest;

import static org.openrewrite.java.Assertions.java;

@SuppressWarnings("java:S2699") // rewriteRun contains assertions
class UseErrorsInsteadOfExceptionsTest implements RewriteTest {
    @Override
    public void defaults(RecipeSpec spec) {
        spec.recipe(new UseErrorsInsteadOfExceptions())
                .parser(JavaParser.fromJavaVersion()
                        .dependsOn(
                                """
                                        package io.kroxylicious.proxy.filter;
                                        
                                        public interface RequestFilterResult {}
                                        """,
                                """
                                        package io.kroxylicious.proxy.filter;
                                        
                                        import org.apache.kafka.common.errors.ApiException;
                                        import org.apache.kafka.common.message.ProduceRequestData;
                                        import org.apache.kafka.common.message.RequestHeaderData;
                                        import org.apache.kafka.common.protocol.Errors;
                                        
                                        public interface RequestFilterResultBuilder {
                                            // Old overload used in "Before" code
                                            RequestFilterResultBuilder errorResponse(RequestHeaderData header, ProduceRequestData request, ApiException exception);
                                        
                                            // New overload used in "After" code
                                            RequestFilterResultBuilder errorResponse(RequestHeaderData header, ProduceRequestData request, Errors error);
                                        
                                            RequestFilterResult completed();
                                        }
                                        """,
                                """
                                        package io.kroxylicious.proxy.filter;
                                        
                                        public interface FilterContext {
                                            RequestFilterResultBuilder requestFilterResultBuilder();
                                        }
                                        """,
                                """
                                        package io.kroxylicious.proxy.filter;
                                        
                                        import java.util.concurrent.CompletionStage;
                                        
                                        import org.apache.kafka.common.message.ProduceRequestData;
                                        import org.apache.kafka.common.message.RequestHeaderData;
                                        
                                        public interface ProduceRequestFilter extends Filter {
                                        
                                            default boolean shouldHandleProduceRequest(short apiVersion) {
                                                return true;
                                            }
                                        
                                             CompletionStage<RequestFilterResult> onProduceRequest(short apiVersion, RequestHeaderData header, ProduceRequestData request, FilterContext context);
                                        
                                        }
                                        """)
                        .classpath("kafka-clients"));
    }

    @Test
    void shouldRemoveExceptionInvocationFromApacheErrors() {
        rewriteRun(
                java(
                        // Before (Input code)
                        """
                                package com.example;
                                
                                import java.util.concurrent.CompletionStage;
                                
                                import org.apache.kafka.common.message.ProduceRequestData;
                                import org.apache.kafka.common.message.RequestHeaderData;
                                import org.apache.kafka.common.protocol.Errors;
                                
                                import io.kroxylicious.proxy.filter.FilterContext;
                                import io.kroxylicious.proxy.filter.ProduceRequestFilter;
                                import io.kroxylicious.proxy.filter.RequestFilterResult;
                                
                                public class SampleFilter implements ProduceRequestFilter {
                                
                                    @Override
                                    public CompletionStage<RequestFilterResult> onProduceRequest(short apiVersion, RequestHeaderData header, ProduceRequestData request, FilterContext context) {
                                        return context.requestFilterResultBuilder().errorResponse(header, request, Errors.GROUP_AUTHORIZATION_FAILED.exception()).completed();
                                    }
                                }
                                """,
                        // After (Expected transformed code)
                        """
                                package com.example;
                                
                                import java.util.concurrent.CompletionStage;
                                
                                import org.apache.kafka.common.message.ProduceRequestData;
                                import org.apache.kafka.common.message.RequestHeaderData;
                                import org.apache.kafka.common.protocol.Errors;
                                
                                import io.kroxylicious.proxy.filter.FilterContext;
                                import io.kroxylicious.proxy.filter.ProduceRequestFilter;
                                import io.kroxylicious.proxy.filter.RequestFilterResult;
                                
                                public class SampleFilter implements ProduceRequestFilter {
                                
                                    @Override
                                    public CompletionStage<RequestFilterResult> onProduceRequest(short apiVersion, RequestHeaderData header, ProduceRequestData request, FilterContext context) {
                                        return context.requestFilterResultBuilder().errorResponse(header, request, Errors.GROUP_AUTHORIZATION_FAILED).completed();
                                    }
                                }
                                """));
    }

}