/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.doctools.asciidoc;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Stream;

import org.asciidoctor.Attributes;
import org.asciidoctor.ast.StructuralNode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import static org.assertj.core.api.Assertions.assertThat;

class BlockExtractorTest {

    private static final FileAttribute<Set<PosixFilePermission>> OWNER_RW = PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-------"));
    @TempDir
    Path tempDir;

    public static Stream<Arguments> sourceBlocks() {
        return Stream.of(
                Arguments.argumentSet("extract single top-level block",
                        """
                                [source,mylang]
                                ----
                                foo: {}
                                ----
                                """,
                        (Predicate<StructuralNode>) sn -> true,
                        (Consumer<List<Block>>) blocks -> assertThat(blocks)
                                .singleElement()
                                .satisfies(b -> assertThat(b.content()).isEqualTo("foo: {}"))),
                Arguments.argumentSet("extracts many top-level blocks",
                        """
                                [source,mylang]
                                ----
                                foo: {}
                                ----
                                [source,mylang]
                                ----
                                bar: {}
                                ----
                                """,
                        (Predicate<StructuralNode>) sn -> true,
                        (Consumer<List<Block>>) blocks -> assertThat(blocks)
                                .satisfies(bs -> assertThat(bs)
                                        .map(Block::content)
                                        .containsExactly("foo: {}", "bar: {}"))),

                Arguments.argumentSet("selects single block",
                        """
                                [source,mylang1]
                                ----
                                foo: {}
                                ----
                                [source,mylang2]
                                ----
                                bar: {}
                                ----
                                """,
                        (Predicate<StructuralNode>) sn -> hasAttributeMatching(sn, "language", "mylang2"),
                        (Consumer<List<Block>>) blocks -> assertThat(blocks)
                                .singleElement()
                                .satisfies(b -> assertThat(b.content()).isEqualTo("bar: {}"))),
                Arguments.argumentSet("supports comments with callouts",
                        """
                                [source,yaml]
                                ----
                                foo: {} # <1>
                                ----
                                <1>: foothing
                                """,
                        (Predicate<StructuralNode>) sn -> hasAttributeMatching(sn, "language", "yaml"),
                        (Consumer<List<Block>>) blocks -> assertThat(blocks)
                                .singleElement()
                                .satisfies(b -> assertThat(b.content()).isEqualTo("foo: {} # <1>"))),
                Arguments.argumentSet("extracts source blocks nested in other blocks",
                        """
                                = Title

                                Foobar

                                [source,mylang1]
                                ----
                                foo: {}
                                ----
                                """,
                        (Predicate<StructuralNode>) sn -> hasAttributeMatching(sn, "language", "mylang1"),
                        (Consumer<List<Block>>) blocks -> assertThat(blocks)
                                .singleElement()
                                .satisfies(b -> assertThat(b.content()).isEqualTo("foo: {}"))),
                Arguments.argumentSet("reports line number",
                        """
                                Mary had a little lamb,
                                Its fleece was white as snow;
                                And everywhere that Mary went,
                                The lamb was sure to go.

                                [source,mylang]
                                ----
                                bar: {}
                                ----
                                """,
                        (Predicate<StructuralNode>) sn -> hasAttributeMatching(sn, "language", "mylang"),
                        (Consumer<List<Block>>) blocks -> assertThat(blocks)
                                .singleElement()
                                .satisfies(bs -> assertThat(bs.lineNumber()).isEqualTo(7))));
    }

    private static boolean hasAttributeMatching(StructuralNode sn, String name, String value) {
        return Objects.equals(sn.getAttribute(name), value);
    }

    @ParameterizedTest
    @MethodSource("sourceBlocks")
    @SuppressWarnings("java:S2699") // Tests should include assertion - they are being provided by the consumer.
    void canExtractSourceBlocks(String asciiDoc, Predicate<StructuralNode> snPredicate, Consumer<List<Block>> assertion) throws Exception {
        // Given
        var input = Files.createTempFile(tempDir, "input", ".adoc", OWNER_RW);
        Files.writeString(input, asciiDoc);

        try (var blockExtractor = new BlockExtractor()) {
            // When
            var blocks = blockExtractor.extract(input, snPredicate);

            // Then
            assertion.accept(blocks);
        }
    }

    @Test
    void shouldInterpolatePassedAttributes() throws Exception {
        // Given
        var input = Files.createTempFile(tempDir, "input", ".adoc", OWNER_RW);
        Files.writeString(input, """
                Mary had a little {animal}
                """);

        Attributes attrs = Attributes.builder().attribute("animal", "lamb").build();
        try (var blockExtractor = new BlockExtractor().withAttributes(attrs)) {
            // When
            var blocks = blockExtractor.extract(input, sn -> true);

            // Then
            assertThat(blocks)
                    .singleElement()
                    .satisfies(b -> {
                        assertThat(b.content()).isEqualTo("Mary had a little lamb");
                    });
        }
    }
}