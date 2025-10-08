/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.doctools.asciidoc;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Predicate;

import org.asciidoctor.Asciidoctor;
import org.asciidoctor.Attributes;
import org.asciidoctor.Options;
import org.asciidoctor.SafeMode;
import org.asciidoctor.ast.Document;
import org.asciidoctor.ast.StructuralNode;
import org.asciidoctor.extension.Treeprocessor;

/**
 * Extract <a href="https://docs.asciidoctor.org/asciidoc/latest/blocks/">blocks</a> matching a predicate from an AsciiDoc source file.
 * Typically used to extract source code or config snippets, so they can be subjected to validation.
 * <br/>
 * Internally the {@link BlockExtractor} relies on the {@link Treeprocessor} plugged into the Asciidoctor parser. The parser processes
 * the source <code>.adoc</code> files, and tree processor implementation extracts the required blocks. We actually don't need the parsers
 * output, but there is no way to discard the output, so the output is written to a temporary which is discarded.
 * <br/>
 * The {@link BlockExtractor} uses a custom backend ({@link AdocConverter} which converts the AsciiDoc AST back into AsciiDoc. The avoids
 * the tree-processor seeing HTML for elements such as callouts.
 */
public class BlockExtractor implements AutoCloseable {

    private static final FileAttribute<Set<PosixFilePermission>> OWNER_DIR_RWX = PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwx------"));
    private final Asciidoctor asciidoctor;
    private Attributes attributes;

    /**
     * Constructs a {@link BlockExtractor}.
     */
    public BlockExtractor() {
        asciidoctor = Asciidoctor.Factory.create();
        asciidoctor.javaConverterRegistry().register(AdocConverter.class);
    }

    /**
     * Sets the attributes to be passed to the AsciiDoc conversion.
     *
     * @param attributes attributes
     * @return this
     */
    public BlockExtractor withAttributes(Attributes attributes) {
        this.attributes = attributes;
        return this;
    }

    /**
     * Extracts the blocks from the specified AsciiDoc file which match the predicate.
     *
     * @param asciiDocFile asciiDoc file
     * @param blockPredicate predicate
     * @return list of blocks, or empty list if the no blocks match the predicate.
     */
    public List<Block> extract(Path asciiDocFile, final Predicate<StructuralNode> blockPredicate) {
        List<Block> blocks = new ArrayList<>();
        asciidoctor.javaExtensionRegistry().treeprocessor(new Treeprocessor() {
            @Override
            public Document process(Document document) {
                document.getBlocks()
                        .forEach(sn -> recurseBlocks(sn, blocks::add, blockPredicate));
                return document;
            }
        });

        Path tempDirectory;
        try {
            var fileName = Optional.ofNullable(asciiDocFile.getFileName()).map(Path::toString)
                    .orElseThrow(() -> new IllegalStateException("Unable to determine filename from path : " + asciiDocFile));
            tempDirectory = Files.createTempDirectory(fileName, OWNER_DIR_RWX);
            try {
                var optionsBuilder = Options.builder()
                        .option(Options.SOURCEMAP, "true") // required so source file/line number information is available
                        .option(Options.TO_DIR, tempDirectory.toString()) // don't need the output files
                        .safe(SafeMode.UNSAFE) // Required to write the output to temp location
                        .backend("adoc");

                Optional.ofNullable(attributes).ifPresent(optionsBuilder::attributes);

                asciidoctor.convertFile(asciiDocFile.toFile(), optionsBuilder.build(), String.class);
            }
            finally {
                deleteAll(tempDirectory);
            }
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return blocks;
    }

    private void deleteAll(Path tempDirectory) {
        try {
            try (var paths = Files.walk(tempDirectory)) {
                paths.sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
            }
            Files.deleteIfExists(tempDirectory);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void recurseBlocks(StructuralNode structuralNode, Consumer<Block> blockConsumer, Predicate<StructuralNode> blockPredicate) {
        if (structuralNode.getBlocks() != null) {
            structuralNode.getBlocks().forEach(child -> recurseBlocks(child, blockConsumer, blockPredicate));
        }

        if (blockPredicate.test(structuralNode)) {
            processBlock(structuralNode, blockConsumer);
        }

    }

    private static void processBlock(StructuralNode structuralNode, Consumer<Block> blockConsumer) {
        var content = String.valueOf(structuralNode.getContent());
        // https://github.com/asciidoctor/asciidoctor/issues/1061
        content = deHtmlEntities(content);
        var sourceLocation = structuralNode.getSourceLocation();
        var block = new Block(new File(sourceLocation.getFile()).toPath(), sourceLocation.getLineNumber(), deHtmlEntities(content));
        blockConsumer.accept(block);
    }

    private static String deHtmlEntities(String content) {
        return content.replace("&lt;", "<")
                .replace("&gt;", ">");
    }

    /**
     * Closes this resource, relinquishing any underlying resources.
     */
    @Override
    public void close() {
        asciidoctor.close();
    }
}
