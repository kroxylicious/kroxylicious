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
import java.io.Writer;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.Spliterator;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.TypeLiteral;

import io.kroxylicious.krpccodegen.model.EntityTypeSetFactory;
import io.kroxylicious.krpccodegen.model.KrpcSchemaObjectWrapper;
import io.kroxylicious.krpccodegen.model.MessageSpecFilters;
import io.kroxylicious.krpccodegen.model.MessageSpecParser;
import io.kroxylicious.krpccodegen.model.RetrieveApiKey;
import io.kroxylicious.krpccodegen.model.RetrieveApiListeners;
import io.kroxylicious.krpccodegen.schema.MessageSpec;
import io.kroxylicious.krpccodegen.schema.RequestListenerType;
import io.kroxylicious.krpccodegen.schema.StructRegistry;
import io.kroxylicious.krpccodegen.schema.Versions;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
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
        private String messageSpecPredicate;

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

        public Builder messageSpecFilterFunction(String messageSpecPredicate) {
            this.messageSpecPredicate = messageSpecPredicate;
            return this;
        }

        /**
         * Creates the generator.
         *
         * @return the generator.
         */
        public KrpcGenerator build() {
            return new KrpcGenerator(logger, mode, messageSpecDir, messageSpecFilter, templateDir, templateNames, outputPackage, outputDir, outputFilePattern,
                    messageSpecPredicate);
        }
    }

    enum GeneratorMode {
        SINGLE,
        MULTI;
    }

    private final Logger logger;
    private final GeneratorMode mode;

    private final File messageSpecDir;
    private final String messageSpecFilter;

    private final File templateDir;
    private final List<String> templateNames;

    private final String outputPackage;
    private final File outputDir;
    private final String outputFilePattern;
    private final String messageSpecFilterFunction;

    @SuppressWarnings("java:S107") // Methods should not have too many parameters - ignored as use-case with builder seems reasonable.
    private KrpcGenerator(Logger logger,
                          GeneratorMode mode,
                          File messageSpecDir,
                          String messageSpecFilter,
                          File templateDir,
                          List<String> templateNames,
                          String outputPackage,
                          File outputDir,
                          String outputFilePattern,
                          String messageSpecFilterFunction) {
        this.logger = logger != null ? logger : System.getLogger(KrpcGenerator.class.getName());
        this.mode = mode;
        this.messageSpecDir = messageSpecDir != null ? messageSpecDir : new File(".");
        this.messageSpecFilter = messageSpecFilter;
        this.templateDir = templateDir;
        this.templateNames = templateNames;
        this.outputPackage = outputPackage;
        this.outputDir = outputDir.toPath().resolve(outputPackage.replace(".", File.separator)).toFile();
        this.outputFilePattern = outputFilePattern;
        this.messageSpecFilterFunction = messageSpecFilterFunction;

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

        if (this.messageSpecFilterFunction != null) {
            try (Context context = Context.newBuilder("js")
                    .allowAllAccess(true)
                    .build()) {
                var bindings = context.getBindings("js");
                bindings.putMember("MessageSpecFilters", MessageSpecFilters.class);
                bindings.putMember("RequestListenerType", RequestListenerType.class);

                var predicate = context.eval("js", messageSpecFilterFunction);
                messageSpecs = predicate.execute(messageSpecs()).as(new TypeLiteral<Stream<MessageSpec>>() {
                })
                        .sorted(Comparator.comparing(MessageSpec::name))
                        .collect(Collectors.toCollection(LinkedHashSet::new));
            }
        }

        long generatedFiles;
        if (mode == GeneratorMode.SINGLE) {
            generatedFiles = messageSpecs.stream().mapToLong(messageSpec -> renderSingle(cfg, messageSpec)).sum();
        }
        else {
            generatedFiles = renderMulti(cfg, messageSpecs);
        }
        if (generatedFiles > 0) {
            logger.log(Level.INFO, "Generated {0} source files", generatedFiles);
        }
        else {
            logger.log(Level.INFO, "Nothing to generate - all sources up to date");
        }
    }

    /**
     * @return the number of files generated
     */
    private long renderSingle(Configuration cfg, MessageSpec messageSpec) {
        logger.log(Level.DEBUG, "Processing message spec {0}", messageSpec.name());
        var structRegistry = new StructRegistry();
        try {
            structRegistry.register(messageSpec);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
        Map<String, Object> dataModel = Map.of(
                "structRegistry", structRegistry,
                "messageSpec", messageSpec,
                "createEntityTypeSet", new EntityTypeSetFactory());
        return templateNames.stream().mapToLong(templateName -> {
            try {
                logger.log(Level.DEBUG, "Parsing template {0}", templateName);
                var template = cfg.getTemplate(templateName);
                // TODO support output to stdout via `-`
                return KrpcGenerator.this.writeIfChanged(outputDir, KrpcGenerator.this.outputFile(outputFilePattern, messageSpec.name(), templateName),
                        new ThrowingWriteAction() {
                            @SuppressFBWarnings("TEMPLATE_INJECTION_FREEMARKER") // we trust the user to supply a template
                            @Override
                            public void accept(Writer writer, File finalFile) throws TemplateException, IOException {
                                logger.log(Level.DEBUG, "Processing message spec {0} with template {1} to {2}", messageSpec.name(), templateName, finalFile);
                                template.process(dataModel, writer);
                            }
                        });
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            catch (TemplateException e) {
                throw new RuntimeException(e);
            }
        }).sum();
    }

    private interface ThrowingWriteAction {
        void accept(Writer writer, File outputFile) throws TemplateException, IOException;
    }

    /**
     * Execute a write action with a target final file. The intent is that the final file always ends up with the desired content, but that
     * we do not modify an existing file if it's already in the desired state. We write the content to a temporary file, then we only update
     * the final file if the final file does not exist, or it differs in any way from the temporary file.
     *
     * @return the count of final source files that were created or updated (should be 0 or 1).
     */
    @SuppressFBWarnings("PATH_TRAVERSAL_IN") // we trust the user-supplied file name
    private long writeIfChanged(File outputDir, String finalFileName, ThrowingWriteAction consumer) throws IOException, TemplateException {
        String tempOutputFileName = finalFileName + ".tmp";
        var outputFile = new File(outputDir, tempOutputFileName);
        Path outPath = outputDir.toPath();
        Path tempPath = outPath.resolve(tempOutputFileName);
        Path finalPath = outPath.resolve(finalFileName);
        logger.log(Level.DEBUG, "Opening output file {0}", outputFile);
        try (var writer = new OutputStreamWriter(new FileOutputStream(outputFile), StandardCharsets.UTF_8)) {
            consumer.accept(writer, finalPath.toFile());
        }
        if (!filesEqual(tempPath, finalPath)) {
            logger.log(Level.DEBUG, "File with new content generated, replacing {0}", finalPath);
            Files.move(tempPath, finalPath, StandardCopyOption.REPLACE_EXISTING);
            return 1;
        }
        else {
            logger.log(Level.DEBUG, "Contents unchanged, deleting temporary file {0}", tempPath);
            Files.delete(tempPath);
            return 0;
        }
    }

    static boolean filesEqual(Path generatedFile, Path finalFile) {
        Objects.requireNonNull(generatedFile, "generatedFile cannot be null");
        Objects.requireNonNull(finalFile, "finalFile cannot be null");
        if (!Files.exists(generatedFile)) {
            throw new IllegalArgumentException("File " + generatedFile + " does not exist");
        }
        if (Files.exists(generatedFile) && !Files.isRegularFile(generatedFile)) {
            throw new IllegalArgumentException(new IOException("File '" + generatedFile + "' exists but is not a regular file"));
        }
        if (Files.exists(finalFile) && !Files.isRegularFile(finalFile)) {
            throw new IllegalArgumentException(new IOException("File '" + finalFile + "' exists but is not a regular file"));
        }
        if (!Files.exists(finalFile)) {
            return false;
        }
        try {
            long mismatch = Files.mismatch(generatedFile, finalFile);
            return mismatch == -1;
        }
        catch (IOException e) {
            throw new UncheckedIOException("IO exception while comparing files for mismatch", e);
        }
    }

    /**
     * @return the number of files generated
     */

    private long renderMulti(Configuration cfg, Set<MessageSpec> messageSpecs) {
        logger.log(Level.DEBUG, "Processing message specs");

        return templateNames.stream().mapToLong(templateName -> {
            try {
                logger.log(Level.DEBUG, "Parsing template {0}", templateName);
                var template = cfg.getTemplate(templateName);
                // TODO support output to stdout via `-`
                return KrpcGenerator.this.writeIfChanged(outputDir, KrpcGenerator.this.outputFile(outputFilePattern, null, templateName), new ThrowingWriteAction() {
                    @SuppressFBWarnings("TEMPLATE_INJECTION_FREEMARKER") // we trust the user to supply a template
                    @Override
                    public void accept(Writer writer, File finalFile) throws TemplateException, IOException {
                        Map<String, Object> dataModel = Map.of(
                                "outputPackage", outputPackage,
                                "messageSpecs", messageSpecs,
                                "retrieveApiKey", new RetrieveApiKey(),
                                "createEntityTypeSet", new EntityTypeSetFactory(),
                                "retrieveApiListener", new RetrieveApiListeners(messageSpecs));
                        template.process(dataModel, writer);
                    }
                });
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            catch (TemplateException e) {
                throw new RuntimeException(e);
            }
        }).sum();
    }

    private Set<MessageSpec> messageSpecs() {
        logger.log(Level.DEBUG, "Finding message specs in {0}", messageSpecDir);
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

        var messageSpecParser = new MessageSpecParser();
        return paths.stream()
                .map(inputPath -> {
                    try {
                        logger.log(Level.DEBUG, "Parsing message spec {0}", inputPath);
                        MessageSpec messageSpec = messageSpecParser.getMessageSpec(inputPath);
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
        // aren't the defaults for backward compatibility.

        // Set the preferred charset template files are stored in. UTF-8 is
        // a good choice in most applications:
        cfg.setDefaultEncoding(StandardCharsets.UTF_8.name());

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
