/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.tombentley.krpccodegen.main;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UncheckedIOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Spliterator;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.github.tombentley.krpccodegen.model.KrpcSchemaObjectWrapper;
import com.github.tombentley.krpccodegen.model.SnakeCase;
import com.github.tombentley.krpccodegen.schema.MessageSpec;
import com.github.tombentley.krpccodegen.schema.MessageSpecType;
import com.github.tombentley.krpccodegen.schema.StructRegistry;
import freemarker.template.Configuration;
import freemarker.template.TemplateException;
import freemarker.template.TemplateExceptionHandler;
import freemarker.template.Version;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KrpcGenerator {
    private static final Logger LOGGER = LoggerFactory.getLogger(KrpcGenerator.class);

    static final String JSON_SUFFIX = ".json";

    static final String JSON_GLOB = "*" + JSON_SUFFIX;

    static final ObjectMapper JSON_SERDE = new ObjectMapper();
    static {
        JSON_SERDE.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
        JSON_SERDE.configure(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY, true);
        JSON_SERDE.configure(DeserializationFeature.FAIL_ON_TRAILING_TOKENS, true);
        JSON_SERDE.configure(JsonParser.Feature.ALLOW_COMMENTS, true);
        JSON_SERDE.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);
    }

    private File schemaDir = new File(".");
    private File outputDir = new File(".");
    private File templateDir = new File(".");
    private Charset templateEncoding = StandardCharsets.UTF_8;
    private List<String> templateNames;
    private String outputFilePattern;
    private Charset outputEncoding = StandardCharsets.UTF_8;
    private EnumSet<MessageSpecType> acceptedTypes = EnumSet.allOf(MessageSpecType.class);

    /** @return The source directory containing the schema (json) files. */
    public File getSchemaDir() {
        return schemaDir;
    }

    /** @param schemaDir The source directory containing the schema (json) files. */
    public void setSchemaDir(File schemaDir) {
        this.schemaDir = schemaDir;
    }

    /** @return The output directory */
    public File getOutputDir() {
        return outputDir;
    }

    /** @param outputDir The output directory */
    public void setOutputDir(File outputDir) {
        this.outputDir = outputDir;
    }

    /** @return Directory containing the templates to apply */
    public File getTemplateDir() {
        return templateDir;
    }

    /** @param templateDir Directory containing the templates to apply */
    public void setTemplateDir(File templateDir) {
        this.templateDir = templateDir;
    }

    /** @return The names of the templates to be applied */
    public List<String> getTemplateNames() {
        return templateNames;
    }

    /** @param templateNames The names of the templates to be applied */
    public void setTemplateNames(List<String> templateNames) {
        this.templateNames = templateNames;
    }

    /** @return The output file pattern */
    public String getOutputFilePattern() {
        return outputFilePattern;
    }

    /** @param outputFilePattern The output file pattern */
    public void setOutputFilePattern(String outputFilePattern) {
        this.outputFilePattern = outputFilePattern;
    }

    String outputFile(String pattern, String schemaName, String templateName) {
        return pattern.replaceAll("\\$\\{schemaName\\}", schemaName)
                .replaceAll("\\$\\{templateName\\}", templateName);
    }

    public void generate() throws IOException {
        var cfg = buildFmConfiguration();

        messageSpecs().forEach(messageSpec -> {
            render(cfg, messageSpec);
        });

    }

    private void render(Configuration cfg, MessageSpec messageSpec) {
        if (!acceptedTypes.contains(messageSpec.type())) {
            LOGGER.info("Ignoring schema {} with type {}", messageSpec.name(), messageSpec.type());
            return;
        }
        LOGGER.info("Processing schema {}", messageSpec.name());
        var structRegistry = new StructRegistry();
        try {
            structRegistry.register(messageSpec);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        templateNames.forEach(templateName -> {
            try {
                LOGGER.info("Parsing template {}", templateName);
                var template = cfg.getTemplate(templateName);
                // TODO support output to stdout via `-`
                var outputFile = new File(outputDir, outputFile(outputFilePattern, messageSpec.name(), templateName));
                LOGGER.info("Opening output file {}", outputFile);
                try (var writer = new OutputStreamWriter(new FileOutputStream(outputFile), outputEncoding)) {
                    LOGGER.info("Processing schema {} with template {} to {}", messageSpec.name(), templateName, outputFile);
                    Map<String, Object> dataModel = Map.of(
                            "structRegistry", structRegistry,
                            "messageSpec", messageSpec,
                            "toSnakeCase", new SnakeCase());
                    template.process(dataModel, writer);
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            } catch (TemplateException e) {
                throw new RuntimeException(e);
            }
        });
    }

    private Stream<MessageSpec> messageSpecs() {
        LOGGER.info("Finding schemas in {}", schemaDir);
        LOGGER.info("{}", Arrays.toString(schemaDir.listFiles()));
        Set<Path> paths;
        try (DirectoryStream<Path> directoryStream = Files
                .newDirectoryStream(schemaDir.toPath(), JSON_GLOB)) {
            Spliterator<Path> spliterator = directoryStream.spliterator();
            paths = StreamSupport.stream(spliterator, false).collect(Collectors.toSet());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        return paths.stream().map(inputPath -> {
            try {
                LOGGER.info("Parsing schema {}", inputPath);
                MessageSpec messageSpec = JSON_SERDE.readValue(inputPath.toFile(), MessageSpec.class);
                LOGGER.info("Loaded {} from {}", messageSpec.name(), inputPath);
                return messageSpec;
            } catch (Exception e) {
                throw new RuntimeException("Exception while processing " + inputPath.toString(), e);
            }
        });
    }

    private Configuration buildFmConfiguration() throws IOException {
        // Create your Configuration instance, and specify if up to what FreeMarker
        // version (here 2.3.29) do you want to apply the fixes that are not 100%
        // backward-compatible. See the Configuration JavaDoc for details.
        Version version = Configuration.VERSION_2_3_31;
        Configuration cfg = new Configuration(version);

        // Specify the source where the template files come from. Here I set a
        // plain directory for it, but non-file-system sources are possible too:
        cfg.setDirectoryForTemplateLoading(templateDir);

        // From here we will set the settings recommended for new projects. These
        // aren't the defaults for backward compatibilty.

        // Set the preferred charset template files are stored in. UTF-8 is
        // a good choice in most applications:
        cfg.setDefaultEncoding(templateEncoding.name());

        // Sets how errors will appear.
        // During web page *development* TemplateExceptionHandler.HTML_DEBUG_HANDLER is better.
        cfg.setTemplateExceptionHandler(TemplateExceptionHandler.RETHROW_HANDLER);

        // Don't log exceptions inside FreeMarker that it will thrown at you anyway:
        cfg.setLogTemplateExceptions(false);

        // Wrap unchecked exceptions thrown during template processing into TemplateException-s:
        cfg.setWrapUncheckedExceptions(true);

        // Do not fall back to higher scopes when reading a null loop variable:
        cfg.setFallbackOnNullLoopVariable(false);

        cfg.setObjectWrapper(new KrpcSchemaObjectWrapper(version));

        LOGGER.info("Created FreeMarker config");
        return cfg;
    }

    public void main(String[] args) throws IOException {
        var gen = new KrpcGenerator();
        // TODO configure generator from args
        gen.generate();
    }
}
