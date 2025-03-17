/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.krpccodegen.main;

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
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Spliterator;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import io.kroxylicious.krpccodegen.model.KrpcSchemaObjectWrapper;
import io.kroxylicious.krpccodegen.model.RetrieveApiKey;
import io.kroxylicious.krpccodegen.schema.MessageSpec;
import io.kroxylicious.krpccodegen.schema.StructRegistry;
import io.kroxylicious.krpccodegen.schema.Versions;

import freemarker.template.Configuration;
import freemarker.template.TemplateException;
import freemarker.template.TemplateExceptionHandler;
import freemarker.template.Version;

/**
 * Code generator driven by Apache Kafka message specifications definitions.
 */
public class KrpcGenerator {

    /**
     * Configures and instantiates the {@link KrpcGenerator}.
     */
    public static class Builder {

        private final GeneratorMode mode;
        private Logger logger;

        private File messageSpecDir;
        private String messageSpecFilter;

        private File templateDir;
        private List<String> templateNames;

        private String outputPackage;
        private File outputDir;
        private String outputFilePattern;

        private Builder(GeneratorMode mode) {
            this.mode = mode;
        }

        /**
         * configures logging.
         * @param logger logger.
         * @return this
         */
        public Builder withLogger(Logger logger) {
            this.logger = logger;
            return this;
        }

        /**
         * configures the message specification directory.
         *
         * @param messageSpecDir message specification directory.
         * @return this
         */
        public Builder withMessageSpecDir(File messageSpecDir) {
            this.messageSpecDir = messageSpecDir;
            return this;
        }

        /**
         * configures the glob pattern used to match message specification files.
         *
         * @param messageSpecFilter the glob pattern
         * @return this
         */
        public Builder withMessageSpecFilter(String messageSpecFilter) {
            this.messageSpecFilter = messageSpecFilter;
            return this;
        }

        /**
         * configures the directory contain the Apache Free Maker template.
         * @param templateDir template directory.
         * @return this
         */
        public Builder withTemplateDir(File templateDir) {
            this.templateDir = templateDir;
            return this;
        }

        /**
         * configures a list of template file names.
         *
         * @param templateNames list of template file names.
         * @return this
         */
        public Builder withTemplateNames(List<String> templateNames) {
            this.templateNames = templateNames;
            return this;
        }

        /**
         * configures the java package name to be used in the generated source.
         *
         * @param outputPackage output package name.
         * @return this
         */
        public Builder withOutputPackage(String outputPackage) {
            this.outputPackage = outputPackage;
            return this;
        }

        /**
         * configures the output directory to be used for the generated source files.
         * @param outputDir output directory.
         * @return this
         */
        public Builder withOutputDir(File outputDir) {
            this.outputDir = outputDir;
            return this;
        }

        /**
         * configures the pattern used to form the output file name.
         * This understands two pattern {@code ${messageSpecName}} and {@code ${templateName}}
         * which if present will be replaced by the message specification name the template
         * name respectively.
         *
         * @param outputFilePattern output filename pattern.
         * @return this
         */
        public Builder withOutputFilePattern(String outputFilePattern) {
            this.outputFilePattern = outputFilePattern;
            return this;
        }

        /**
         * Creates the generator.
         *
         * @return the generator.
         */
        public KrpcGenerator build() {
            return new KrpcGenerator(logger, mode, messageSpecDir, messageSpecFilter, templateDir, templateNames, outputPackage, outputDir, outputFilePattern);
        }
    }

    enum GeneratorMode {
        SINGLE,
        MULTI;
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
    private final GeneratorMode mode;

    private final File messageSpecDir;
    private final String messageSpecFilter;

    private final File templateDir;
    private final Charset templateEncoding = StandardCharsets.UTF_8;
    private final List<String> templateNames;

    private final String outputPackage;
    private final File outputDir;
    private final String outputFilePattern;
    private final Charset outputEncoding = StandardCharsets.UTF_8;

    @SuppressWarnings("java:S107") // Methods should not have too many parameters - ignored as use-case with builder seems reasonable.
    private KrpcGenerator(Logger logger, GeneratorMode mode, File messageSpecDir, String messageSpecFilter, File templateDir, List<String> templateNames,
                          String outputPackage, File outputDir,
                          String outputFilePattern) {
        this.logger = logger != null ? logger : System.getLogger(KrpcGenerator.class.getName());
        this.mode = mode;
        this.messageSpecDir = messageSpecDir != null ? messageSpecDir : new File(".");
        this.messageSpecFilter = messageSpecFilter;
        this.templateDir = templateDir;
        this.templateNames = templateNames;
        this.outputPackage = outputPackage;
        this.outputDir = outputDir.toPath().resolve(outputPackage.replace(".", File.separator)).toFile();
        this.outputFilePattern = outputFilePattern;

        if (!this.outputDir.exists()) {
            this.outputDir.mkdirs();
        }
    }

    /**
     * Constructs a generator in single mode. The generator passes a list of all
     * message specifications to the template which is used to produce a single
     * output file.
     * @return the builder
     */
    public static Builder single() {
        return new Builder(GeneratorMode.SINGLE);
    }

    /**
     * Constructs a generator in multi-mode. The generator each message specification
     * to the generator in turn, each of which produces a distinct output file.
     * @return the builder
     */
    public static Builder multi() {
        return new Builder(GeneratorMode.MULTI);
    }

    /**
     * Generates the sources.
     * @throws Exception exception during source generation.
     */
    public void generate() throws Exception {
        var cfg = buildFmConfiguration();
        Set<MessageSpec> messageSpecs = messageSpecs();

        if (mode == GeneratorMode.SINGLE) {
            for (MessageSpec messageSpec : messageSpecs) {
                renderSingle(cfg, messageSpec);
            }
        }
        else {
            renderMulti(cfg, messageSpecs);
        }

    }

    private void renderSingle(Configuration cfg, MessageSpec messageSpec) {
        logger.log(Level.INFO, "Processing message spec {0}", messageSpec.name());
        var structRegistry = new StructRegistry();
        try {
            structRegistry.register(messageSpec);
        }
        catch (Exception e) {
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
                            "messageSpec", messageSpec);
                    template.process(dataModel, writer);
                }
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            catch (TemplateException e) {
                throw new RuntimeException(e);
            }
        });
    }

    private void renderMulti(Configuration cfg, Set<MessageSpec> messageSpecs) {
        logger.log(Level.INFO, "Processing message specs");

        // TODO not actually used right now
        // var structRegistry = new StructRegistry();
        // try {
        // for (MessageSpec messageSpec : messageSpecs) {
        // structRegistry.register(messageSpec);
        // }
        // }
        // catch (Exception e) {
        // throw new RuntimeException(e);
        // }
        templateNames.forEach(templateName -> {
            try {
                logger.log(Level.DEBUG, "Parsing template {0}", templateName);
                var template = cfg.getTemplate(templateName);
                // TODO support output to stdout via `-`
                var outputFile = new File(outputDir, outputFile(outputFilePattern, null, templateName));
                logger.log(Level.DEBUG, "Opening output file {0}", outputFile);
                try (var writer = new OutputStreamWriter(new FileOutputStream(outputFile), outputEncoding)) {
                    Map<String, Object> dataModel = Map.of(
                            // "structRegistry", structRegistry,
                            "outputPackage", outputPackage,
                            "messageSpecs", messageSpecs,
                            "retrieveApiKey", new RetrieveApiKey());
                    template.process(dataModel, writer);
                }
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            catch (TemplateException e) {
                throw new RuntimeException(e);
            }
        });
    }

    private Set<MessageSpec> messageSpecs() {
        logger.log(Level.INFO, "Finding message specs in {0}", messageSpecDir);
        logger.log(Level.DEBUG, "{0}", Arrays.toString(messageSpecDir.listFiles()));
        Set<Path> paths;
        try (DirectoryStream<Path> directoryStream = Files
                .newDirectoryStream(messageSpecDir.toPath(), messageSpecFilter)) {
            Spliterator<Path> spliterator = directoryStream.spliterator();
            paths = StreamSupport.stream(spliterator, false).collect(Collectors.toSet());
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        return paths.stream()
                .map(inputPath -> {
                    try {
                        logger.log(Level.DEBUG, "Parsing message spec {0}", inputPath);
                        MessageSpec messageSpec = JSON_SERDE.readValue(inputPath.toFile(), MessageSpec.class);
                        logger.log(Level.DEBUG, "Loaded {0} from {1}", messageSpec.name(), inputPath);
                        return messageSpec;
                    }
                    catch (Exception e) {
                        throw new RuntimeException("Exception while processing " + inputPath.toString(), e);
                    }
                })
                .filter(Predicate.not(v -> v.validVersions().equals(Versions.NONE)))
                .sorted(Comparator.comparing(MessageSpec::name))
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    private Configuration buildFmConfiguration() throws Exception {
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

        cfg.setSharedVariable("outputPackage", outputPackage);

        logger.log(Level.DEBUG, "Created FreeMarker config");
        return cfg;
    }

    private String outputFile(String pattern, String messageSpecName, String templateName) {
        if (messageSpecName != null) {
            pattern = pattern.replaceAll("\\$\\{messageSpecName\\}", messageSpecName);
        }

        if (templateName != null) {
            templateName = templateName.substring(Math.max(0, templateName.lastIndexOf(File.separator) + 1), templateName.indexOf(".ftl"));
            pattern = pattern.replaceAll("\\$\\{templateName\\}", templateName);
        }

        return pattern;
    }
}
