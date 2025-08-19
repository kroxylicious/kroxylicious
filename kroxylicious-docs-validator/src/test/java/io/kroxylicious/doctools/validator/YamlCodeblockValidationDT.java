/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.doctools.validator;

import java.util.Objects;
import java.util.stream.Stream;

import org.asciidoctor.ast.StructuralNode;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import io.kroxylicious.doctools.asciidoc.Block;
import io.kroxylicious.doctools.asciidoc.BlockExtractor;
import org.yaml.snakeyaml.Yaml;

import static org.assertj.core.api.Assertions.assertThatNoException;

/** Tests that all yaml source blocks within the documentation are valid yaml */
class YamlCodeblockValidationDT {

    private final Yaml yaml = new Yaml();

    static Stream<Arguments> yamlSourceBlocks() {
        try (var extractor = new BlockExtractor()) {
            return Utils.asciiDocFilesMatching(Utils.ALL_ASCIIDOC_FILES)
                    .flatMap(p -> extractor.extract(p, YamlCodeblockValidationDT::isYamlBlock).stream())
                    .map(Arguments::of);
        }
    }

    private static boolean isYamlBlock(StructuralNode sn) {
        return Objects.equals(sn.getAttribute("style", null), "source") &&
                Objects.equals(sn.getAttribute("language", null), "yaml");
    }

    @ParameterizedTest
    @MethodSource("yamlSourceBlocks")
    void shouldBeValidYaml(Block yamlBlock) {
        var content = yamlBlock.content();
        assertThatNoException()
                .as("Failed to parse yaml at {} line {}", yamlBlock.asciiDocFile(), yamlBlock.lineNumber())
                .isThrownBy(() -> this.yaml.load(content));
    }
}
