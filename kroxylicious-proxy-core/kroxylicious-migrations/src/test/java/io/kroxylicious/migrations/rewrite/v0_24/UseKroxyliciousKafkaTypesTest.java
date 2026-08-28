/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.migrations.rewrite.v0_24;

import org.junit.jupiter.api.Test;
import org.openrewrite.java.JavaParser;
import org.openrewrite.test.RecipeSpec;
import org.openrewrite.test.RewriteTest;

import static org.openrewrite.java.Assertions.java;

@SuppressWarnings("java:S2699") // rewriteRun contains assertions
class UseKroxyliciousKafkaTypesTest implements RewriteTest {

    @Override
    public void defaults(RecipeSpec spec) {
        spec.recipeFromResources("io.kroxylicious.migrations.rewrite.v0_24.UseKroxyliciousKafkaTypes");
        spec.parser(JavaParser
                .fromJavaVersion()
                .classpath("kafka-clients"));
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
    void shouldMigrateApacheCompressPackageToKroxyliciousPackage() {
        rewriteRun(
                java(
                        """
                                package com.example;

                                import org.apache.kafka.common.compress.Compression;

                                public class SampleFilter {
                                    private Compression compression;
                                }
                                """,
                        """
                                package com.example;

                                import io.kroxylicious.kafka.common.compress.Compression;

                                public class SampleFilter {
                                    private Compression compression;
                                }
                                """));
    }

    @Test
    void shouldMigrateApacheHeaderPackageToKroxyliciousPackage() {
        rewriteRun(
                java(
                        """
                                package com.example;

                                import org.apache.kafka.common.header.Header;

                                public class SampleFilter {
                                    private Header header;
                                }
                                """,
                        """
                                package com.example;

                                import io.kroxylicious.kafka.common.header.Header;

                                public class SampleFilter {
                                    private Header header;
                                }
                                """));
    }

    @Test
    void shouldMigrateApacheUtilsPackageToKroxyliciousPackage() {
        rewriteRun(
                java(
                        """
                                package com.example;

                                import org.apache.kafka.common.utils.ByteUtils;

                                public class SampleFilter {
                                    private final Class<?> type = ByteUtils.class;
                                }
                                """,
                        """
                                package com.example;

                                import io.kroxylicious.kafka.common.utils.ByteUtils;

                                public class SampleFilter {
                                    private final Class<?> type = ByteUtils.class;
                                }
                                """));
    }

    @Test
    void shouldMigrateApacheRecordTypesToKroxyliciousInternalPackage() {
        rewriteRun(
                java(
                        """
                                package com.example;

                                import org.apache.kafka.common.record.MemoryRecords;

                                public class SampleFilter {
                                    private MemoryRecords records;
                                }
                                """,
                        """
                                package com.example;

                                import io.kroxylicious.kafka.common.record.internal.MemoryRecords;

                                public class SampleFilter {
                                    private MemoryRecords records;
                                }
                                """));
    }

    @Test
    void shouldMigrateApacheTimestampTypeToKroxyliciousRecordPackage() {
        rewriteRun(
                java(
                        """
                                package com.example;

                                import org.apache.kafka.common.record.TimestampType;

                                public class SampleFilter {
                                    private TimestampType timestampType;
                                }
                                """,
                        """
                                package com.example;

                                import io.kroxylicious.kafka.common.record.TimestampType;

                                public class SampleFilter {
                                    private TimestampType timestampType;
                                }
                                """));
    }

    @Test
    void shouldMigrateApacheCommonTopLevelClassesToKroxyliciousPackage() {
        rewriteRun(
                java(
                        """
                                package com.example;

                                import org.apache.kafka.common.Uuid;

                                public class SampleFilter {
                                    private Uuid id;
                                }
                                """,
                        """
                                package com.example;

                                import io.kroxylicious.kafka.common.Uuid;

                                public class SampleFilter {
                                    private Uuid id;
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