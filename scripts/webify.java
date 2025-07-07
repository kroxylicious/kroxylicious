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

import static java.lang.System.*;

import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.Callable;

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

    @Option(names = {"--rootdir"}, required = true, description = "The rootdir.")
    private String rootdir;

    @Option(names = {"--src-dir"}, required = true, description = "The source directory containing Asciidoc standalone HTML.")
    private Path srcDir;

    @Option(names = {"--dest-dir"}, required = true, description = "The output directory ready for copying to the website.")
    private Path destdir;

    @Option(names = {"--tocify-omit"}, description = "Glob matching file(s) to omit from the HTML output.")
    private List<String> omitGlobs = List.of();

    @Option(names = {"--tocify-toc-file"}, description = "The name to give to output TOC files")
    private String tocifyTocName;

    @Option(names = {"--tocify-tocless-file"}, description = "The name to give to output TOC-less files")
    private String tocifyToclessName;

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

        walk(omitGlobs, tocifyGlob, datafyGlob);

        Files.writeString(outdir.getParent().resolve("index.md"),
                docIndexFrontMatter(), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);

        return 0;
    }

    String interpolate(String str, String... keyValuePairs) {
        if (keyValuePairs.length % 2 != 0) {
            throw new IllegalArgumentException("keyValuePairs must be even");
        }
        str = str
                .replace("${project.version}", this.projectVersion)
                .replace("${rootdir}", this.rootdir);
        for (int i = 0; i < keyValuePairs.length; i += 2) {
            str = str.replace("${" + keyValuePairs[i] + "}", keyValuePairs[i + 1]);
        }
        return str;
    }

    String docIndexFrontMatter() {
        return interpolate("""
---
layout: released-documentation
title: Documentation
permalink: /documentation/${project.version}/
---
        """);
    }

    record DocYaml(String src,
                   List<String> transforms,
                   Map<String, String> frontmatter) {}

    DocYaml readDocYaml(Path docYamlPath) throws IOException {
        var docYaml = this.mapper.readValue(docYamlPath.toFile(), DocYaml.class);
        return docYaml;
    }

    private void walk(List<PathMatcher> omitGlobs,
                      PathMatcher tocifyGlob,
                      PathMatcher datafyGlob) throws IOException {
        var resultDocsList = new ArrayList<ObjectNode>();
        try (var stream = Files.walk(this.srcDir)) {
            stream.forEach((Path filePath) -> {
                try {
                    if (!Files.isRegularFile(filePath)) {
                        return;
                    }
                    var relFilePath = this.srcDir.relativize(filePath);
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
                        resultDocsList.add(datafy(filePath, outFilePath));
                    }
                    else if (!omitable && !tocifiable && !datafiable) {
                        //
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

        Collections.sort(resultDocsList, Comparator.nullsLast(Comparator.comparing(node -> node.get("rank").asText(null))));

        var resultRootObject = this.mapper.createObjectNode();
        var resultDocsArray = resultRootObject.putArray("docs");
        resultDocsArray.addAll(resultDocsList);
        System.out.println(mapper.writeValueAsString(resultRootObject));
        Files.createDirectories(dataDestPath.getParent());
        Files.writeString(dataDestPath, mapper.writeValueAsString(resultRootObject), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
    }

    ObjectNode datafy(Path docYamlPath, Path outFilePath) throws IOException {
        String docName = docYamlPath.getParent().getFileName().toString();
        var docYaml = readDocYaml(docYamlPath);
        String relPath;
        if (docYaml.src != null) {
            relPath = "html/" + docName;
            var bob = this.outdir.resolve(docName);
            //Files.createDirectories(outFilePath.getParent());
            var rr = docYamlPath.resolve(interpolate(docYaml.src.toString()));
            try (var s = Files.walk(rr)) {
                s.forEach(docPath -> {
                    try {
                        var to = bob.resolve(rr.relativize(docPath));
                        System.out.println("from: " + docPath);
                        System.out.println("to: " + to);

                        if (Files.isDirectory(docPath)) {
                            Files.createDirectories(to);
                        }
                        else if (Files.isRegularFile(docPath)) {
                            Files.copy(docPath, to, StandardCopyOption.REPLACE_EXISTING);
                        }
                        else {
                            throw new RuntimeException("Not a regular file or directory: " + docPath.toString());
                        }
                    }
                    catch (java.lang.Exception e) {
                        throw new RuntimeException(e);
                    }
                });
            }
        }
        else if (!docYaml.has("path")) {
            relPath = "html/" + docName;
            Files.createDirectories(outFilePath.getParent());
            Files.writeString(outFilePath.getParent().resolve("index.html"),
                    guideFrontMatter(docYaml, relPath),
                    StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
        }
        else {
            relPath = interpolate(docYaml.get("path").textValue());
        }
        docYaml.put("path", relPath);
        return docYaml;
    }



    String guideFrontMatter(ObjectNode dataDocObject, String relPath) throws IOException {
        return "---\n" + this.mapper.writeValueAsString(this.mapper.createObjectNode()
                .put("layout", "guide")
                .<ObjectNode>setAll(dataDocObject)
                .put("version", this.projectVersion)
                .put("permalink", interpolate("/documentation/${project.version}/${relPath}/", "relPath", relPath))) + "---\n";
    }


    void tocify(Path htmlFilePath,
                Path outFilePath) throws IOException {
        var outDir = outFilePath.getParent();
        Files.createDirectories(outDir);
        if (!splitOutToc(htmlFilePath,
                outDir.resolve(this.tocifyTocName),
                outDir.resolve(this.tocifyToclessName))) {
            System.out.println("copying " + htmlFilePath + " to " + outFilePath);
            Files.copy(htmlFilePath, outFilePath, StandardCopyOption.REPLACE_EXISTING);
        }
    }

    static boolean splitOutToc(Path htmlFilePath,
                               Path outputTocHtmlPath,
                               Path outputToclessHtmlPath) throws IOException {
        // Parse the SOURCE
        Document doc = Jsoup.parse(htmlFilePath, "UTF-8");
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
        writeRaw(toc, outputTocHtmlPath);
        writeRaw(doc, outputToclessHtmlPath);
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
