/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package org.kroxylicious.doctools.validator;

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
    static final Path DOCS_ROOTDIR = Path.of("").toAbsolutePath().getParent().resolve("docs");

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
