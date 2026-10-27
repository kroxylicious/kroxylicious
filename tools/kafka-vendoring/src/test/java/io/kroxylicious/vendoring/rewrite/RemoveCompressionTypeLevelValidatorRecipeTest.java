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

@SuppressWarnings("java:S2699") // rewriteRun contains assertions
class RemoveCompressionTypeLevelValidatorRecipeTest implements RewriteTest {

    @Override
    public void defaults(RecipeSpec spec) {
        spec.recipe(new RemoveCompressionTypeLevelValidatorRecipe())
                .parser(JavaParser.fromJavaVersion()
                        .classpath("kafka-clients"));
    }

    @Test
    void shouldRemoveLevelValidatorFromCompressionType() {
        rewriteRun(
                java(
                        """
                                package org.apache.kafka.common.compress;

                                public enum CompressionType {
                                    NONE,
                                    GZIP,
                                    SNAPPY;

                                    public Object levelValidator() {
                                        return null;
                                    }

                                    public static void main(String[] args) {
                                        // do something
                                    }
                                }
                                """,
                        """
                                package org.apache.kafka.common.compress;

                                public enum CompressionType {
                                    NONE,
                                    GZIP,
                                    SNAPPY;

                                    public static void main(String[] args) {
                                        // do something
                                    }
                                }
                                """));
    }

    @Test
    void shouldRemoveAllLevelValidatorVariants() {
        rewriteRun(
                java(
                        """
                                package org.apache.kafka.common.compress;

                                public enum CompressionType {
                                    NONE,
                                    GZIP;

                                    public Object levelValidator() {
                                        return null;
                                    }

                                    public Object levelValidator(int level) {
                                        return null;
                                    }

                                    public String toString() {
                                        return "test";
                                    }
                                }
                                """,
                        """
                                package org.apache.kafka.common.compress;

                                public enum CompressionType {
                                    NONE,
                                    GZIP;

                                    public String toString() {
                                        return "test";
                                    }
                                }
                                """));
    }

    @Test
    void shouldNotRemoveOtherMethods() {
        rewriteRun(
                java(
                        """
                                package org.apache.kafka.common.compress;

                                public enum CompressionType {
                                    NONE,
                                    GZIP;

                                    public Object levelValidator() {
                                        return null;
                                    }

                                    public String getName() {
                                        return "test";
                                    }
                                }
                                """,
                        """
                                package org.apache.kafka.common.compress;

                                public enum CompressionType {
                                    NONE,
                                    GZIP;

                                    public String getName() {
                                        return "test";
                                    }
                                }
                                """));
    }

    @Test
    void shouldRemoveUnusedConfigDefImportWhenRemovingLevelValidator() {
        rewriteRun(
                java(
                        """
                                package org.apache.kafka.common.compress;

                                import org.apache.kafka.common.config.ConfigDef;

                                public enum CompressionType {
                                    NONE,
                                    GZIP;

                                    public ConfigDef.Validator levelValidator() {
                                        return null;
                                    }
                                }
                                """,
                        """
                                package org.apache.kafka.common.compress;

                                public enum CompressionType {
                                    NONE,
                                    GZIP;
                                }
                                """));
    }

    @Test
    void shouldKeepImportWhenOnlyRemovingOneOverload() {
        rewriteRun(
                java(
                        """
                                package org.apache.kafka.common.compress;

                                import org.apache.kafka.common.config.ConfigDef;

                                public enum CompressionType {
                                    NONE,
                                    GZIP;

                                    public ConfigDef.Validator levelValidator() {
                                        return null;
                                    }

                                    public ConfigDef.Validator anotherValidator() {
                                        return null;
                                    }
                                }
                                """,
                        """
                                package org.apache.kafka.common.compress;

                                import org.apache.kafka.common.config.ConfigDef;

                                public enum CompressionType {
                                    NONE,
                                    GZIP;

                                    public ConfigDef.Validator anotherValidator() {
                                        return null;
                                    }
                                }
                                """));
    }

    @Test
    void shouldFailIfWeDontRemoveTheMethodRegression() {
        // This test proves the recipe catches regressions — if we accidentally broke
        // the removal logic, this would fail because levelValidator() would still be there
        rewriteRun(
                java(
                        """
                                package org.apache.kafka.common.compress;

                                public enum CompressionType {
                                    NONE,
                                    GZIP;

                                    public Object levelValidator() {
                                        return null;
                                    }
                                }
                                """,
                        """
                                package org.apache.kafka.common.compress;

                                public enum CompressionType {
                                    NONE,
                                    GZIP;
                                }
                                """));
    }
}
