///usr/bin/env jbang "$0" "$@" ; exit $?
//JAVA 21+
//DEPS org.jsoup:jsoup:1.20.1
//DEPS info.picocli:picocli:4.6.3
//DEPS com.fasterxml.jackson.core:jackson-core:2.18.3
//DEPS com.fasterxml.jackson.core:jackson-databind:2.18.3
//DEPS com.fasterxml.jackson.dataformat:jackson-dataformat-yaml:2.18.3

/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

import org.jsoup.*;
import org.jsoup.nodes.*;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator.Feature;
import com.fasterxml.jackson.databind.node.*;

@Command(name = "webify", mixinStandardHelpOptions = true, version = "webify 0.1",
        description = "Converts Asciidoc standalone HTML output into content ready for kroxylicious.io")
public class webify implements Callable<Integer> {

    @Option(names = {"--project-version"}, required = true, description = "The kroxy version.")
    private String projectVersion;

    @Option(names = {"--asciidoc-src-dir"}, required = true, description = "The source directory containing Asciidoc standalone HTML.")
    private Path asciidocSrcDir;

    @Option(names = {"--markdown-src-dir"}, required = true, description = "The source directory containing Asciidoc standalone HTML.")
    private Path markdownSrcDir;

    @Option(names = {"--dest-dir"}, required = true, description = "The output directory ready for copying to the website.")
    private Path destdir;

    @Option(names = {"--tocify-omit"}, description = "Glob matching file(s) to omit from the HTML output.")
    private List<String> omitGlobs = List.of();

    @Option(names = {"--tocify"}, description = "Glob matching HTML files within --src-dir to tocify.")
    private String tocifyGlob;

    @Option(names = {"--tocify-toc-file"}, description = "The name to give to output TOC files")
    private String tocifyTocName;

    @Option(names = {"--tocify-tocless-file"}, description = "The name to give to output TOC-less files")
    private String tocifyToclessName;

    @Option(names = {"--datafy"}, description = "Glob matching data yamls")
    private String datafyGlob;

    private Path outdir;
    private Path dataDestPath;

    private final ObjectMapper mapper = new YAMLMapper()
            .disable(Feature.WRITE_DOC_START_MARKER)
            .enable(Feature.MINIMIZE_QUOTES)
            .enable(Feature.INDENT_ARRAYS_WITH_INDICATOR);

    public static void main(String... args) {
        int exitCode = new CommandLine(new webify()).execute(args);
        System.exit(exitCode);
    }


    @Override
    public Integer call() throws Exception {
        System.out.println(this);
        this.outdir = this.destdir.resolve("documentation").resolve(this.projectVersion).resolve("html");
        this.dataDestPath = this.destdir.resolve("_data/documentation").resolve(this.projectVersion.replace(".", "_") + ".yaml");

        FileSystem fs = FileSystems.getDefault();
        var tocifyGlob = fs.getPathMatcher("glob:" + this.tocifyGlob);
        var omitGlobs = this.omitGlobs.stream().map(glob -> fs.getPathMatcher("glob:" + glob)).toList();
        var datafyGlob = fs.getPathMatcher("glob:" + this.datafyGlob);

        processStructuredAsciidoc(omitGlobs, tocifyGlob, datafyGlob);
        processSelfContainedMarkdown();
        buildDocumentationManifest(datafyGlob, List.of(asciidocSrcDir, markdownSrcDir));

        Files.writeString(outdir.getParent().resolve("index.md"),
                docIndexFrontMatter(), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);

        return 0;
    }

    private void processSelfContainedMarkdown() throws IOException {
        try (var stream = Files.walk(this.markdownSrcDir)) {
            stream.filter(Files::isRegularFile)
                    .filter(path -> path.getFileName().toString().endsWith(".markdown"))
            .forEach((Path filePath) -> {
                try {
                    var relFilePath = this.markdownSrcDir.relativize(filePath);
                    var outFilePath = this.outdir.resolve(relFilePath);
                    Files.createDirectories(outFilePath.getParent());
                    String markdown = Files.readString(filePath);
                    // we use the directory name to create the Jekyll frontmatter name
                    Path markdownDir = filePath.getParent();
                    Files.writeString(outFilePath, selfContainedMarkdownFrontMatter(markdownDir) + markdown, StandardCharsets.UTF_8, StandardOpenOption.CREATE_NEW);
                }
                catch (Exception e) {
                    throw new RuntimeException(filePath.toString(), e);
                }
            });
        }
    }

    String selfContainedMarkdownFrontMatter(Path path) {
        String name = path.getFileName().toString();
        String markdownName = toHumanPresentableName(name);
        return """
---
layout: quickstart
title: ${name}
version: ${project.version}
---
        """.replace("${project.version}", this.projectVersion).replace("${name}", markdownName);
    }

    public static String toHumanPresentableName(String fileName) {
        if (fileName == null || fileName.isEmpty()) {
            return "";
        }

        String baseName = fileName;
        int lastDotIndex = fileName.lastIndexOf('.');
        if (lastDotIndex > 0) {
            baseName = fileName.substring(0, lastDotIndex);
        }

        return Arrays.stream(baseName.split("-"))
                .filter(part -> !part.isEmpty())
                .map(part -> Character.toUpperCase(part.charAt(0)) + part.substring(1))
                .collect(Collectors.joining(" "));
    }

    String docIndexFrontMatter() {
        return """
---
layout: released-documentation
title: Documentation
permalink: /documentation/${project.version}/
---
        """.replace("${project.version}", this.projectVersion);
    }

    private void processStructuredAsciidoc(List<PathMatcher> omitGlobs,
                                           PathMatcher tocifyGlob,
                                           PathMatcher datafyGlob) throws IOException {
        try (var stream = Files.walk(this.asciidocSrcDir)) {
            stream.forEach((Path filePath) -> {
                try {
                    if (!Files.isRegularFile(filePath)) {
                        return;
                    }
                    var relFilePath = this.asciidocSrcDir.relativize(filePath);
                    var outFilePath = this.outdir.resolve(relFilePath);
                    var omitable = omitGlobs.stream().anyMatch(glob -> glob.matches(relFilePath));
                    var tocifiable = tocifyGlob.matches(relFilePath);
                    var datafiable = datafyGlob.matches(relFilePath);
                    if (omitable && !tocifiable && !datafiable) {
                        return;
                    }
                    else if (!omitable && tocifiable && !datafiable) {
                        tocify(filePath, outFilePath);
                    }
                    else if (!omitable && !tocifiable && datafiable) {
                        var dataDocObject = readMetadata(filePath, relFilePath);
                        if (!dataDocObject.has("path")) {
                            Files.createDirectories(outFilePath.getParent());
                            Files.writeString(outFilePath.getParent().resolve("index.html"),
                                    guideFrontMatter(dataDocObject, "html/" + relFilePath.getParent().toString()),
                                    StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
                        }
                    }
                    else if (!omitable && !tocifiable) {
                        Files.createDirectories(outFilePath.getParent());
                        Files.copy(filePath, outFilePath, StandardCopyOption.REPLACE_EXISTING);
                    }
                    else {
                        throw new IOException((filePath + " matched multiple globs: "
                                + (omitable ? "--tocify-omit " : "")
                                + (tocifiable ? "--tocify " : "")
                                + (datafiable ? "--datafy " : "")).trim());
                    }
                }
                catch (Exception e) {
                    throw new RuntimeException(filePath.toString(), e);
                }
            });
        }
    }

    private void buildDocumentationManifest(PathMatcher datafyGlob, List<Path> sourceDirs) throws IOException {
        var resultDocsList = new ArrayList<ObjectNode>();
        for (Path sourceDir : sourceDirs) {
            try (var stream = Files.walk(sourceDir)) {
                stream.forEach((Path filePath) -> {
                    try {
                        if (!Files.isRegularFile(filePath)) {
                            return;
                        }
                        var relFilePath = sourceDir.relativize(filePath);
                        var datafiable = datafyGlob.matches(relFilePath);
                        if (datafiable) {
                            var dataDocObject = readMetadata(filePath, relFilePath);
                            String relPath;
                            if (!dataDocObject.has("path")) {
                                relPath = "html/" + relFilePath.getParent().toString();
                            }
                            else {
                                relPath = dataDocObject.get("path").textValue().replace("${project.version}", this.projectVersion);
                            }
                            dataDocObject.put("path", relPath);
                            resultDocsList.add(dataDocObject);
                        }
                    }
                    catch (Exception e) {
                        throw new RuntimeException(filePath.toString(), e);
                    }
                });
            }
        }
        Collections.sort(resultDocsList, Comparator.nullsLast(Comparator.comparing(node -> node.get("rank").asText(null))));
        var resultRootObject = this.mapper.createObjectNode();
        var resultDocsArray = resultRootObject.putArray("docs");
        resultDocsArray.addAll(resultDocsList);
        System.out.println(mapper.writeValueAsString(resultRootObject));
        Files.createDirectories(dataDestPath.getParent());
        Files.writeString(dataDestPath, mapper.writeValueAsString(resultRootObject), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
    }

    String guideFrontMatter(ObjectNode dataDocObject, String relPath) throws IOException {
        return "---\n" + this.mapper.writeValueAsString(this.mapper.createObjectNode()
                .put("layout", "guide")
                .<ObjectNode>setAll(dataDocObject)
                .put("version", this.projectVersion)
                .put("permalink", "/documentation/${project.version}/${relPath}/"
                        .replace("${project.version}", this.projectVersion)
                        .replace("${relPath}", relPath))) + "---\n";
    }

    ObjectNode readMetadata(Path filePath,
                             Path relFilePath) throws IOException {
        var dataDocObject = (ObjectNode) this.mapper.readTree(filePath.toFile());
        var resultDocObject = this.mapper.createObjectNode();
        var dataDocFields = dataDocObject.fields();
        while (dataDocFields.hasNext()) {
            var entry = dataDocFields.next();
            if ("$schema".equals(entry.getKey())) {
                continue;
            }
            resultDocObject.put(entry.getKey(), entry.getValue());
        }
        return resultDocObject;
    }

    void tocify(Path filePath,
                Path outFilePath) throws IOException {
        var outDir = outFilePath.getParent();
        Files.createDirectories(outDir);
        if (!splitOutToc(filePath,
                outDir.resolve(this.tocifyTocName),
                outDir.resolve(this.tocifyToclessName))) {
            System.out.println("copying " + filePath + " to " + outFilePath);
            Files.copy(filePath, outFilePath, StandardCopyOption.REPLACE_EXISTING);
        }
    }

    static boolean splitOutToc(Path sourcePath,
                               Path tocPath,
                               Path toclessPath) throws IOException {
        // Parse the SOURCE
        Document doc = Jsoup.parse(sourcePath, "UTF-8");
        // Find the toc by it's ID
        var toc = doc.getElementById("toc");
        if (toc == null) {
            return false;
        }
        // Remove the toc from the doc
        toc.remove();
        // Drop the "Table of Content" title
        toc.getElementById("toctitle").remove();
        // Write the two nodes
        writeRaw(toc, tocPath);
        writeRaw(doc, toclessPath);
        return true;
    }
    
    static void writeRaw(Node node,
                         Path path) throws IOException {
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
}
