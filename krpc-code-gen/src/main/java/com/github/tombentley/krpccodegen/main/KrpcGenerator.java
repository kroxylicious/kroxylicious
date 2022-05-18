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
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
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
import com.github.tombentley.krpccodegen.schema.StructRegistry;

import freemarker.template.Configuration;
import freemarker.template.TemplateException;
import freemarker.template.TemplateExceptionHandler;
import freemarker.template.Version;

public class KrpcGenerator {

    public static class Builder {

        private Logger logger;

        private File messageSpecDir;
        private String messageSpecFilter;

        private File templateDir;
        private List<String> templateNames;

        private File outputDir;
        private String outputFilePattern;


        public Builder withLogger(Logger logger) {
            this.logger = logger;
            return this;
        }

        public Builder withMessageSpecDir(File messageSpecDir) {
            this.messageSpecDir = messageSpecDir;
            return this;
        }

        public Builder withMessageSpecFilter(String messageSpecFilter) {
            this.messageSpecFilter = messageSpecFilter;
            return this;
        }

        public Builder withTemplateDir(File templateDir) {
            this.templateDir = templateDir;
            return this;
        }

        public Builder withTemplateNames(List<String> templateNames) {
            this.templateNames = templateNames;
            return this;
        }

        public Builder withOutputDir(File outputDir) {
            this.outputDir = outputDir;
            return this;
        }

        public Builder withOutputFilePattern(String outputFilePattern) {
            this.outputFilePattern = outputFilePattern;
            return this;
        }

        public KrpcGenerator build() {
            return new KrpcGenerator(logger, messageSpecDir, messageSpecFilter, templateDir, templateNames, outputDir, outputFilePattern);
        }
    }

    static final ObjectMapper JSON_SERDE = new ObjectMapper();
    static {
        JSON_SERDE.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
        JSON_SERDE.configure(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY, true);
        JSON_SERDE.configure(DeserializationFeature.FAIL_ON_TRAILING_TOKENS, true);
        JSON_SERDE.configure(JsonParser.Feature.ALLOW_COMMENTS, true);
        JSON_SERDE.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);
    }

    private final Logger logger;

    private final File messageSpecDir;
    private final String messageSpecFilter;

    private final File templateDir;
    private final Charset templateEncoding = StandardCharsets.UTF_8;
    private final List<String> templateNames;

    private final File outputDir;
    private final String outputFilePattern;
    private final Charset outputEncoding = StandardCharsets.UTF_8;

    public KrpcGenerator(Logger logger, File messageSpecDir, String messageSpecFilter, File templateDir, List<String> templateNames, File outputDir, String outputFilePattern) {
        this.logger = logger != null ? logger : System.getLogger(KrpcGenerator.class.getName());
        this.messageSpecDir = messageSpecDir != null ? messageSpecDir : new File(".");
        this.messageSpecFilter = messageSpecFilter;
        this.templateDir = templateDir;
        this.templateNames = templateNames;
        this.outputDir = outputDir;
        this.outputFilePattern = outputFilePattern;

        if (!outputDir.exists()) {
            outputDir.mkdirs();
        }
    }

    private String outputFile(String pattern, String messageSpecName, String templateName) {
        return pattern.replaceAll("\\$\\{messageSpecName\\}", messageSpecName)
                .replaceAll("\\$\\{templateName\\}", templateName);
    }

    public void generate() throws IOException {
        var cfg = buildFmConfiguration();

        messageSpecs().forEach(messageSpec -> {
            render(cfg, messageSpec);
        });

    }

    private void render(Configuration cfg, MessageSpec messageSpec) {
        logger.log(Level.INFO, "Processing message spec {0}", messageSpec.name());
        var structRegistry = new StructRegistry();
        try {
            structRegistry.register(messageSpec);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        templateNames.forEach(templateName -> {
            try {
                logger.log(Level.DEBUG, "Parsing template {0}", templateName);
                var template = cfg.getTemplate(templateName);
                // TODO support output to stdout via `-`
                var outputFile = new File(outputDir, outputFile(outputFilePattern, messageSpec.name(), templateName));
                logger.log(Level.DEBUG, "Opening output file {0}", outputFile);
                try (var writer = new OutputStreamWriter(new FileOutputStream(outputFile), outputEncoding)) {
                    logger.log(Level.DEBUG, "Processing message spec {0} with template {1} to {2}", messageSpec.name(), templateName, outputFile);
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
        logger.log(Level.INFO, "Finding message specs in {0}", messageSpecDir);
        logger.log(Level.DEBUG, "{0}", Arrays.toString(messageSpecDir.listFiles()));
        Set<Path> paths;
        try (DirectoryStream<Path> directoryStream = Files
                .newDirectoryStream(messageSpecDir.toPath(), messageSpecFilter)) {
            Spliterator<Path> spliterator = directoryStream.spliterator();
            paths = StreamSupport.stream(spliterator, false).collect(Collectors.toSet());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        return paths.stream().map(inputPath -> {
            try {
                logger.log(Level.DEBUG, "Parsing message spec {0}", inputPath);
                MessageSpec messageSpec = JSON_SERDE.readValue(inputPath.toFile(), MessageSpec.class);
                logger.log(Level.DEBUG, "Loaded {0} from {1}", messageSpec.name(), inputPath);
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

        logger.log(Level.DEBUG, "Created FreeMarker config");
        return cfg;
    }
}
