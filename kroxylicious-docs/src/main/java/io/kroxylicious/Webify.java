/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious;

///usr/bin/env jbang "$0" "$@" ; exit $?
//JAVA 21+
//DEPS org.jsoup:jsoup:${jsoup.version}
//DEPS info.picocli:picocli:${picocli.version}
//DEPS com.fasterxml.jackson.core:jackson-core:${jackson.version}
//DEPS com.fasterxml.jackson.core:jackson-databind:${jackson.version}
//DEPS com.fasterxml.jackson.dataformat:jackson-dataformat-yaml:${jackson.version}
//DEPS org.slf4j:slf4j-api:${slf4j.version}
//DEPS org.slf4j:slf4j-simple:${slf4j.version}
import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.function.Consumer;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator.Feature;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@SuppressWarnings("java:S101") // lowercase name to match jbang conventions
// https://www.jbang.dev/documentation/jbang/latest/faq.html#:~:text=Why%20is%20JBang%20scripting%20examples%20using%20lower%20case%20class%20names%20%3F
@Command(name = "webify", mixinStandardHelpOptions = true, version = "webify 0.1", description = "Converts Asciidoc standalone HTML output into content ready for kroxylicious.io")
public class Webify implements Callable<Integer> {

    public static final String PROJECT_VERSION_PLACEHOLDER = "${project.version}";
    @Option(names = { "--project-version" }, required = true, description = "The kroxy version.")
    private String projectVersion;

    @Option(names = { "--src-dir" }, required = true, description = "The source directory containing Asciidoc standalone HTML.")
    private Path srcDir;

    @Option(names = { "--dest-dir" }, required = true, description = "The output directory ready for copying to the website.")
    private Path destdir;

    @Option(names = { "--tocify-omit" }, description = "Glob matching file(s) to omit from the HTML output.")
    private List<String> omitGlobs = List.of();

    @Option(names = { "--tocify" }, description = "Glob matching HTML files within --src-dir to tocify.")
    private String tocifyGlob;

    @Option(names = { "--tocify-toc-file" }, description = "The name to give to output TOC files")
    private String tocifyTocName;

    @Option(names = { "--tocify-tocless-file" }, description = "The name to give to output TOC-less files")
    private String tocifyToclessName;

    @Option(names = { "--datafy" }, description = "Glob matching data yamls")
    private String datafyGlob;

    private final Logger logger = LoggerFactory.getLogger(Webify.class);

    private Path outdir;
    private Path dataDestPath;

    private final ObjectMapper mapper = new YAMLMapper()
            .disable(Feature.WRITE_DOC_START_MARKER)
            .enable(Feature.MINIMIZE_QUOTES)
            .enable(Feature.INDENT_ARRAYS_WITH_INDICATOR);

    public static void main(String... args) {
        int exitCode = execute(args);
        System.exit(exitCode);
    }

    static int execute(String... args) {
        return new CommandLine(new Webify()).execute(args);
    }

    @Override
    public Integer call() {
        this.outdir = this.destdir.resolve("documentation").resolve(this.projectVersion).resolve("html");
        this.dataDestPath = this.destdir.resolve("_data/documentation").resolve(this.projectVersion.replace(".", "_") + ".yaml");

        try (FileSystem fs = FileSystems.getDefault()) {
            PathMatcher tocifyGlobMatcher = globPathMatcher(fs, this.tocifyGlob);
            List<PathMatcher> omitGlobsMatchers = this.omitGlobs.stream().map(glob -> globPathMatcher(fs, glob)).toList();
            PathMatcher datafyGlobMatcher = globPathMatcher(fs, this.datafyGlob);

            walk(omitGlobsMatchers, tocifyGlobMatcher, datafyGlobMatcher);

            Path parentDir = outdir.getParent();
            // If condition to keep spotbugs happy
            if (parentDir != null && Files.exists(parentDir)) {
                Files.writeString(parentDir.resolve("index.md"),
                        docIndexFrontMatter(), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
                return 0;
            }
            else {
                logger.warn("could not find a parent directory for {}", outdir);
                return 1;
            }
        }
        catch (UnsupportedOperationException ex) {
            // Only certain filesystem implementations are actually closeable (waves at ZipFileSystem) the rest tell us to get stuffed.
            logger.debug(ex.getMessage(), ex);
            return 0;

        }
        catch (Exception ex) {
            logger.error(ex.getMessage(), ex);
            return 2;
        }
    }

    private static PathMatcher globPathMatcher(FileSystem fs, String globPattern) {
        return fs.getPathMatcher("glob:" + globPattern);
    }

    String docIndexFrontMatter() {
        return """
                ---
                layout: released-documentation
                title: Documentation
                permalink: /documentation/${project.version}/
                ---
                       \s""".replace(PROJECT_VERSION_PLACEHOLDER, this.projectVersion);
    }

    private void walk(List<PathMatcher> omitGlobs,
                      PathMatcher tocifyGlob,
                      PathMatcher datafyGlob)
            throws IOException {
        var resultDocsList = new ArrayList<ObjectNode>();
        try (var stream = Files.walk(this.srcDir)) {
            stream.forEach(new DocConverter(omitGlobs, tocifyGlob, datafyGlob, resultDocsList));
        }

        Comparator<ObjectNode> byRank = Comparator.comparing(node -> node.get("rank").asText(null));
        Comparator<ObjectNode> byTitle = Comparator.comparing(node -> node.get("title").asText(null));
        resultDocsList.sort(Comparator.nullsLast(byRank.thenComparing(byTitle)));

        var resultRootObject = this.mapper.createObjectNode();
        var resultDocsArray = resultRootObject.putArray("docs");
        resultDocsArray.addAll(resultDocsList);
        if (logger.isInfoEnabled()) {
            logger.info(mapper.writeValueAsString(resultRootObject));
        }
        Files.createDirectories(Objects.requireNonNull(dataDestPath.getParent()));
        Files.writeString(dataDestPath, mapper.writeValueAsString(resultRootObject), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
    }

    String guideFrontMatter(ObjectNode dataDocObject, String relPath) throws IOException {
        return "---\n" + this.mapper.writeValueAsString(this.mapper.createObjectNode()
                .put("layout", "guide")
                .<ObjectNode> setAll(dataDocObject)
                .put("version", this.projectVersion)
                .put("permalink", "/documentation/" + PROJECT_VERSION_PLACEHOLDER + "/${relPath}/"
                        .replace(PROJECT_VERSION_PLACEHOLDER, this.projectVersion)
                        .replace("${relPath}", relPath)))
                + "---\n";
    }

    ObjectNode readMetadata(Path filePath)
            throws IOException {
        var dataDocObject = (ObjectNode) this.mapper.readTree(filePath.toFile());
        return buildDocNode(dataDocObject);
    }

    private ObjectNode buildDocNode(ObjectNode dataDocObject) {
        var resultDocObject = this.mapper.createObjectNode();
        Set<Map.Entry<String, JsonNode>> dataDoc = dataDocObject.properties();
        dataDoc.forEach(dataDocEntry -> {
            if (!"$schema".equals(dataDocEntry.getKey())) {
                resultDocObject.set(dataDocEntry.getKey(), dataDocEntry.getValue());
            }
        });
        return resultDocObject;
    }

    void tocify(Path filePath,
                Path outFilePath)
            throws IOException {
        var outDir = Objects.requireNonNull(outFilePath.getParent());
        Files.createDirectories(outDir);
        if (!splitOutToc(filePath,
                outDir.resolve(this.tocifyTocName),
                outDir.resolve(this.tocifyToclessName))) {
            logger.info("copying {} to {}", filePath, outFilePath);
            Files.copy(filePath, outFilePath, StandardCopyOption.REPLACE_EXISTING);
        }
    }

    static boolean splitOutToc(Path sourcePath,
                               Path tocPath,
                               Path toclessPath)
            throws IOException {
        // Parse the SOURCE
        Document doc = Jsoup.parse(sourcePath, "UTF-8");
        // Find the toc by its ID
        var toc = doc.getElementById("toc");
        if (toc == null) {
            return false;
        }
        // Remove the toc from the doc
        toc.remove();
        // Drop the "Table of Content" title
        Optional.ofNullable(toc.getElementById("toctitle")).ifPresent(Node::remove);

        // Write the two nodes
        writeRaw(toc, tocPath);
        writeRaw(doc, toclessPath);
        return true;
    }

    static void writeRaw(Node node,
                         Path path)
            throws IOException {
        // Jekyll/Liquid doesn't have a way of {% include ... %} which
        // *prevents* the included file processing as a liquid template
        // Bracketing with {% raw %}/{% endraw %} is about the best we can do
        // to prevent evaluation, but an {% endraw %} in the HMTL would be
        // enough to break it ☹
        try (var writer = Files.newBufferedWriter(path)) {
            writer.append("{% raw %}\n");
            writer.append(node.toString());
            writer.append("\n{% endraw %}\n");
            writer.flush();
        }
    }

    private class DocConverter implements Consumer<Path> {
        private final List<PathMatcher> omitGlobs;
        private final PathMatcher tocifyGlob;
        private final PathMatcher datafyGlob;
        private final List<ObjectNode> resultDocsList;

        DocConverter(List<PathMatcher> omitGlobs, PathMatcher tocifyGlob, PathMatcher datafyGlob, List<ObjectNode> resultDocsList) {
            this.omitGlobs = omitGlobs;
            this.tocifyGlob = tocifyGlob;
            this.datafyGlob = datafyGlob;
            this.resultDocsList = resultDocsList;
        }

        @Override
        public void accept(Path filePath) {
            try {
                if (!Files.isRegularFile(filePath)) {
                    return;
                }
                var relFilePath = Objects.requireNonNull(Webify.this.srcDir.relativize(filePath));
                var outFilePath = Objects.requireNonNull(Webify.this.outdir.resolve(relFilePath));
                var omitable = omitGlobs.stream().anyMatch(glob -> glob.matches(relFilePath));
                var tocifiable = tocifyGlob.matches(relFilePath);
                var datafiable = datafyGlob.matches(relFilePath);
                if (omitable && !tocifiable && !datafiable) {
                    // exit early when there is nothing to be done.
                    return;
                }
                if (!omitable && tocifiable && !datafiable) {
                    Webify.this.tocify(filePath, outFilePath);
                }
                else {
                    datifyOrCopy(filePath, outFilePath, omitable, tocifiable, datafiable, relFilePath);
                }
            }
            catch (Exception e) {
                throw new WebifyException(filePath.toString(), e);
            }
        }

        private void datifyOrCopy(Path filePath, Path outFilePath, boolean omitable, boolean tocifiable, boolean datafiable, Path relFilePath) throws IOException {
            Path outputDirectory = Objects.requireNonNull(outFilePath.getParent());
            if (!omitable && !tocifiable && datafiable) {
                resultDocsList.add(createDataDocObject(filePath, relFilePath, outputDirectory));
            }
            else if (!omitable && !tocifiable) {
                Files.createDirectories(outputDirectory);
                Files.copy(filePath, outFilePath, StandardCopyOption.REPLACE_EXISTING);
            }
            else {
                throw new IOException((filePath + " matched multiple globs: "
                        + (omitable ? "--tocify-omit " : "")
                        + (tocifiable ? "--tocify " : "")
                        + (datafiable ? "--datafy " : "")).trim());
            }
        }

        private ObjectNode createDataDocObject(Path filePath, Path relFilePath, Path outputDirectory) throws IOException {
            var dataDocObject = Webify.this.readMetadata(filePath);
            String relPath;
            if (!dataDocObject.has("path")) {
                Path relFilePathParent = Objects.requireNonNull(relFilePath.getParent());
                relPath = "html/" + relFilePathParent;
                Files.createDirectories(outputDirectory);
                Files.writeString(outputDirectory.resolve("index.html"),
                        Webify.this.guideFrontMatter(dataDocObject, "html/" + relFilePathParent),
                        StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
            }
            else {
                relPath = dataDocObject.get("path").textValue().replace(PROJECT_VERSION_PLACEHOLDER, Webify.this.projectVersion);
            }
            dataDocObject.put("path", relPath);
            return dataDocObject;
        }
    }

    public static class WebifyException extends RuntimeException {
        public WebifyException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
