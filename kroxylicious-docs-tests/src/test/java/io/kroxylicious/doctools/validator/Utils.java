/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.doctools.validator;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Stream;

public class Utils {

    static final Predicate<Path> ALL_ASCIIDOC_FILES = f -> f.getFileName().toString().endsWith(".adoc");
    static final Path MODULE_ROOT = Path.of("").toAbsolutePath();
    static final Path DOCS_ROOTDIR = MODULE_ROOT.getParent().resolve("kroxylicious-docs").resolve("docs");

    // Install manifest and examples for the Operator. The Maven copy-kroxylicious-operator-artifacts copies these
    // from the kroxylicious-operator-dist module to this module with stable names.
    static final Path OPERATOR_INSTALL_MANIFEST = MODULE_ROOT.resolve("target").resolve("kroxylicious-operator-dist").resolve("kroxylicious-operator-install.yaml");
    static final Path OPERATOR_EXAMPLES_ZIP = MODULE_ROOT.resolve("target").resolve("kroxylicious-operator-dist").resolve("kroxylicious-operator-examples.zip");

    static Stream<Path> asciiDocFilesMatching(final Predicate<Path> pathPredicate) {

        try {
            List<Path> adocs = new ArrayList<>();
            Files.walkFileTree(DOCS_ROOTDIR, new SimpleFileVisitor<>() {
                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
                    if (pathPredicate.test(file)) {
                        adocs.add(file);
                    }
                    return FileVisitResult.CONTINUE;
                }

            });
            return adocs.stream();
        }
        catch (IOException e) {
            throw new UncheckedIOException("Error walking directory tree", e);
        }
    }
}
