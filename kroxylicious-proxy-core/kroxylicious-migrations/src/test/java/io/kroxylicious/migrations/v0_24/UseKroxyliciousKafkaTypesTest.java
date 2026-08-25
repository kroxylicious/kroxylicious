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

class UseKroxyliciousKafkaTypesTest implements RewriteTest {
    @Override
    public void defaults(RecipeSpec spec) {
        spec.recipeFromResources("io.kroxylicious.migrations.v0_24.UseKroxyliciousKafkaTypes");
        spec.parser(JavaParser
                .fromJavaVersion()
                .dependsOn("""
                        package org.apache.kafka.common.message;
                        public class ProduceRequestData {}
                        """,
                        """
                                package io.kroxylicious.kafka.common.message;
                                public class ProduceRequestData {}
                                """,
                        """
                                package org.apache.kafka.common.protocol;
                                public interface Readable {}
                                """,
                        """
                                package org.apache.kafka.common.protocol;
                                public class ByteBufferAccessor implements Readable {}
                                """,
                        """
                                package io.kroxylicious.kafka.common.protocol;
                                public class ByteBufferAccessor {}
                                """));
    }

    @Test
    void shouldMigrateApacheMessagePackageToKroxyliciousPackage() {
        rewriteRun(
                java(
                        // Before (Input code)
                        """
                                package com.example;

                                import org.apache.kafka.common.message.ProduceRequestData;

                                public class SampleFilter {
                                    private ProduceRequestData data;
                                }
                                """,
                        // After (Expected transformed code)
                        """
                                package com.example;

                                import io.kroxylicious.kafka.common.message.ProduceRequestData;

                                public class SampleFilter {
                                    private ProduceRequestData data;
                                }
                                """));
    }

    @Test
    void shouldMigrateApacheProtocolPackageToKroxyliciousPackage() {
        rewriteRun(
                java(
                        // Before (Input code)
                        """
                                package com.example;

                                import java.nio.ByteBuffer;
                                import org.apache.kafka.common.protocol.ByteBufferAccessor;

                                public class SampleFilter {
                                    protected static void shouldBuildAccessor(short headerVersion, ByteBuffer buffer) {
                                        new ByteBufferAccessor(buffer);
                                    }
                                }
                                """,
                        // After (Expected transformed code)
                        """
                                package com.example;

                                import java.nio.ByteBuffer;
                                import io.kroxylicious.kafka.common.protocol.ByteBufferAccessor;

                                public class SampleFilter {
                                    protected static void shouldBuildAccessor(short headerVersion, ByteBuffer buffer) {
                                        new ByteBufferAccessor(buffer);
                                    }
                                }
                                """));
    }
}