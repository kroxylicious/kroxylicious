/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.doctools.validator;

import java.nio.file.Path;
import java.util.stream.Stream;

import org.asciidoctor.Asciidoctor;
import org.asciidoctor.Options;
import org.asciidoctor.ast.Document;
import org.asciidoctor.ast.StructuralNode;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests that the initial block of each Asciidoc files meets the conformance criteria
 * described in kroxylicious-docs/README.md
 */
@SuppressWarnings("java:S3577") // ignoring naming convention for the test class
class ConformanceDT {

    static Stream<Arguments> asciiDocFiles() {
        return Utils.asciiDocFilesMatching(f -> f.getFileName().toString().matches("(proc|con|assembly)-.*\\.adoc$"))
                .map(Arguments::of);
    }

    @ParameterizedTest
    @MethodSource("asciiDocFiles")
    void anchorIdsFormation(Path asciiDocFile) {
        Options book = Options.builder()
                .sourcemap(true)
                .docType("book")
                .build();

        try (var asciidoctor = Asciidoctor.Factory.create()) {
            var doc = asciidoctor.loadFile(asciiDocFile.toFile(), book);
            assertThat(getId(doc))
                    .withFailMessage("Asciidoc file %s lacks a top-level id that conforms to the expected format", asciiDocFile.toFile())
                    .isNotNull()
                    .isEqualTo(expectedId(asciiDocFile));
        }
    }

    private static String getId(Document doc) {

        if (doc.getId() != null) {
            return doc.getId();
        }
        else {
            var outer = doc.getBlocks().stream().findFirst();
            var inner = outer.map(StructuralNode::getBlocks)
                    .flatMap(b -> b.stream().findFirst());
            return inner.map(StructuralNode::getId).orElse(null);
        }
    }

    private static String expectedId(Path asciiDocFile) {
        return asciiDocFile.getFileName().toString().replaceFirst("\\.adoc", "") + "-{context}";
    }

}
