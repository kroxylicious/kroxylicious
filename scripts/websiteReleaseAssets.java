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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.Callable;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "webify", mixinStandardHelpOptions = true, version = "webify 0.1",
        description = "Creates release metadata for kroxylicious.io")
public class websiteReleaseAssets implements Callable<Integer> {

    @Option(names = { "--project-version" }, required = true, description = "The kroxy version.")
    private String projectVersion;

    @Option(names = { "--release-manifest-template-file" }, required = true, description = "The release manifest template file")
    private Path releaseManifestTemplateFile;

    @Option(names = { "--dest-dir" }, required = true, description = "The output directory ready for copying to the website.")
    private Path destdir;

    public static void main(String... args) {
        int exitCode = new CommandLine(new websiteReleaseAssets()).execute(args);
        System.exit(exitCode);
    }

    @Override
    public Integer call() throws Exception {
        System.out.println(this);
        createReleaseManifestFile();
        createDownloadPageFile();
        return 0;
    }

    private void createDownloadPageFile() throws IOException {
        var indexFile = """
                ---
                layout: download-release
                ---
                """;
        Path outputIndex = destdir.resolve("download").resolve(projectVersion).resolve("index.md");
        Files.createDirectories(outputIndex.getParent());
        System.out.println("Creating download index " + outputIndex);
        Files.writeString(outputIndex, indexFile, StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
    }

    public void createReleaseManifestFile() throws IOException {
        if (!Files.exists(releaseManifestTemplateFile)) {
            throw new RuntimeException(releaseManifestTemplateFile + " does not exist");
        }
        if (!Files.isRegularFile(releaseManifestTemplateFile)) {
            throw new RuntimeException(releaseManifestTemplateFile + " is not a regular file");
        }
        String underscoredVersion = projectVersion.replace(".", "_");
        Path outputManifest = destdir.resolve("_data").resolve("release").resolve(underscoredVersion + ".yaml");
        Files.createDirectories(outputManifest.getParent());
        System.out.println("Creating release manifest " + outputManifest);
        Files.copy(releaseManifestTemplateFile, outputManifest, StandardCopyOption.REPLACE_EXISTING);
    }

}
