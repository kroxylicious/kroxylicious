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
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.Spliterator;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.kafka.common.protocol.ApiKeys;
import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.TypeLiteral;

import io.kroxylicious.krpccodegen.KrpcCodeGenerationException;
import io.kroxylicious.krpccodegen.model.EntityTypeSetFactory;
import io.kroxylicious.krpccodegen.model.KrpcSchemaObjectWrapper;
import io.kroxylicious.krpccodegen.model.RetrieveApiKey;
import io.kroxylicious.krpccodegen.schema.ApiSpec;
import io.kroxylicious.krpccodegen.schema.EntityType;
import io.kroxylicious.krpccodegen.schema.MessageSpec;
import io.kroxylicious.krpccodegen.schema.MessageSpecType;
import io.kroxylicious.krpccodegen.schema.Named;
import io.kroxylicious.krpccodegen.schema.RequestListenerType;
import io.kroxylicious.krpccodegen.schema.StructRegistry;
import io.kroxylicious.krpccodegen.schema.Versions;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
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
        private File sourceDir;
        private String outputFilePattern;
        private String inputSpecFilter;
        private boolean apiSpecMode;
        private boolean skipOutputIfSourceExists;

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
         * The location of the project's source files (usually <code>src/main/java</code>.
         * When in skipOutputIfSourceExists mode, before generating an output file to
         * the output directory, it checks whether the equivalent file already exist
         * in sourceDir. If so, output is skipped.
         *
         * @param sourceDir source directory
         * @return this
         */
        public Builder withSourceDir(File sourceDir) {
            this.sourceDir = sourceDir;
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
         * javascript filtering function applied to the specifications before they are sent to the template.
         * This allows the source generation to be selective, generating sources for a
         * sub-set of message specifications.
         *
         * @param inputSpecFilter function defined in javascript, usually as a lambda function.
         * @return this
         */
        public Builder inputSpecFilter(String inputSpecFilter) {
            this.inputSpecFilter = inputSpecFilter;
            return this;
        }

        /**
         * In apiSpecMode, the processor pairs request and response specifications into {@link ApiSpec} objects.
         * The template will see {@link ApiSpec} objects rather than individual {@link MessageSpec} objects.
         *
         * @param apiSpecMode true to enable API spec mode
         * @return this
         */
        public Builder withApiSpecMode(boolean apiSpecMode) {
            this.apiSpecMode = apiSpecMode;
            return this;
        }

        /**
         * If true, the processor will skip the generation of a source if a source file of the same
         * name already exists in the project's source directory.
         * @param skipOutputIfSourceExists true for skip mode.
         * @return this
         */
        public Builder withSkipOutputIfSourceExists(boolean skipOutputIfSourceExists) {
            this.skipOutputIfSourceExists = skipOutputIfSourceExists;
            return this;
        }

        /**
         * Creates the generator.
         *
         * @return the generator.
         */
        public KrpcGenerator build() {
            return new KrpcGenerator(logger, mode, messageSpecDir, messageSpecFilter, templateDir, templateNames, outputPackage, outputDir, outputFilePattern,
                    sourceDir, inputSpecFilter, apiSpecMode, skipOutputIfSourceExists);
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
    private final File sourceDir;
    private final boolean apiSpecMode;
    private final boolean skipOutputIfSourceExists;
    private final File outputDir;
    private final String outputFilePattern;
    private final String inputSpecFilter;

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
                          File sourceDir,
                          String inputSpecFilter,
                          boolean apiSpecMode,
                          boolean skipOutputIfSourceExists) {
        if (skipOutputIfSourceExists && sourceDir == null) {
            throw new IllegalArgumentException("When in skipOutputIfSourceExists mode, sourceDir must be provided");
        }
        this.logger = logger != null ? logger : System.getLogger(KrpcGenerator.class.getName());
        this.mode = mode;
        this.messageSpecDir = messageSpecDir != null ? messageSpecDir : new File(".");
        this.messageSpecFilter = messageSpecFilter;
        this.templateDir = templateDir;
        this.templateNames = templateNames;
        this.outputPackage = outputPackage;
        this.apiSpecMode = apiSpecMode;
        this.skipOutputIfSourceExists = skipOutputIfSourceExists;
        this.outputDir = addPackageDirs(outputDir, outputPackage);
        this.sourceDir = addPackageDirs(sourceDir, outputPackage);
        this.outputFilePattern = outputFilePattern;
        this.inputSpecFilter = inputSpecFilter;
        if (!this.outputDir.exists()) {
            this.outputDir.mkdirs();
        }
    }

    @Nullable
    private static File addPackageDirs(File sourceDir, String outputPackage) {
        var dirize = outputPackage.replace(".", File.separator);
        return Optional.ofNullable(sourceDir).map(File::toPath).map(sd -> sd.resolve(dirize)).map(Path::toFile).orElse(null);
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
        var inputSpecs = inputSpecs();

        var dataModel = Map.<String, Object> of(
                "createEntityTypeSet", new EntityTypeSetFactory());

        long generatedFiles;
        if (mode == GeneratorMode.SINGLE) {
            generatedFiles = inputSpecs.stream().mapToLong(inputSpec -> {
                Map<String, Object> dm = new HashMap<>(dataModel);
                dm.put("inputSpec", inputSpec);
                if (inputSpec instanceof MessageSpec messageSpec) {
                    dm.put("structRegistry", buildStructRegistry(messageSpec));
                }
                return renderSingle(cfg, inputSpec, dm);
            }).sum();
        }
        else {
            Map<String, Object> dm = new HashMap<>(dataModel);
            dm.put("outputPackage", outputPackage);
            dm.put("retrieveApiKey", new RetrieveApiKey());
            dm.put("createEntityTypeSet", new EntityTypeSetFactory());
            dm.put("inputSpecs", inputSpecs);
            generatedFiles = renderMulti(cfg, dm);
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
    private long renderSingle(Configuration cfg, Named target, Map<String, Object> dataModel) {
        logger.log(Level.DEBUG, "Processing message spec {0}", target.name());
        return templateNames.stream().mapToLong(templateName -> {
            try {
                logger.log(Level.DEBUG, "Parsing template {0}", templateName);
                var template = cfg.getTemplate(templateName);
                var finalFileName = outputFile(outputFilePattern, target.name(), templateName);
                if (shouldSkip(finalFileName)) {
                    return 0;
                }
                return writeIfChanged(outputDir, finalFileName,
                        new ThrowingWriteAction() {
                            @SuppressFBWarnings("TEMPLATE_INJECTION_FREEMARKER") // we trust the user to supply a template
                            @Override
                            public void accept(Writer writer, File finalFile) throws TemplateException, IOException {
                                logger.log(Level.DEBUG, "Processing message spec {0} with template {1} to {2}", target.name(), templateName, finalFile);
                                template.process(dataModel, writer);
                            }
                        });
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            catch (TemplateException e) {
                throw new KrpcCodeGenerationException(e);
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
    private long renderMulti(Configuration cfg, final Map<String, Object> dataModel) {
        logger.log(Level.DEBUG, "Processing message specs");

        return templateNames.stream().mapToLong(templateName -> {
            try {
                logger.log(Level.DEBUG, "Parsing template {0}", templateName);
                var template = cfg.getTemplate(templateName);
                var finalFileName = outputFile(outputFilePattern, null, templateName);
                if (shouldSkip(finalFileName)) {
                    return 0;
                }
                return writeIfChanged(outputDir, finalFileName, new ThrowingWriteAction() {
                    @SuppressFBWarnings("TEMPLATE_INJECTION_FREEMARKER") // we trust the user to supply a template
                    @Override
                    public void accept(Writer writer, File finalFile) throws TemplateException, IOException {
                        template.process(dataModel, writer);
                    }
                });
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            catch (TemplateException e) {
                throw new KrpcCodeGenerationException(e);
            }
        }).sum();
    }

    private Set<? extends Named> inputSpecs() {
        var messageSpecs = messageSpecs();

        Set<? extends Named> inputSpecs;
        if (apiSpecMode) {
            inputSpecs = toApiSpecs(messageSpecs);
        }
        else {
            inputSpecs = messageSpecs;
        }

        inputSpecs = applyFilter(inputSpecs);
        return inputSpecs;
    }

    private Set<? extends Named> applyFilter(Set<? extends Named> set) {
        if (this.inputSpecFilter != null) {
            try (Context context = Context.newBuilder("js")
                    .option("engine.WarnInterpreterOnly", "false")
                    .allowAllAccess(true)
                    .build()) {
                // These bindings allow Javascript expressions to be written in terms of the Java classes, without referring to the fully qualified Java name.
                var bindings = context.getBindings("js");
                bindings.putMember("RequestListenerType", RequestListenerType.class);
                bindings.putMember("EntityType", EntityType.class);
                bindings.putMember("Set", Set.class);

                var predicate = context.eval("js", inputSpecFilter);
                return predicate.execute(set.stream()).as(new TypeLiteral<Stream<Named>>() {
                        })
                        .sorted(Comparator.comparing(Named::name))
                        .collect(Collectors.toCollection(LinkedHashSet::new));
            }
        }
        else {
            return set;
        }
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
                        throw new KrpcCodeGenerationException("Exception while processing " + inputPath.toString(), e);
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

    private Set<ApiSpec> toApiSpecs(Set<MessageSpec> messageSpecs) {
        var allRequests = messageSpecs.stream()
                .filter(qms -> MessageSpecType.REQUEST.equals(qms.type()))
                .collect(Collectors.toMap(x -> x.apiKey().orElseThrow(),
                        Function.identity()));
        var allResponses = messageSpecs.stream()
                .filter(sms -> MessageSpecType.RESPONSE.equals(sms.type()))
                .collect(Collectors.toMap(x -> x.apiKey().orElseThrow(),
                        Function.identity()));

        if (allRequests.size() != allResponses.size()) {
            throw new IllegalStateException("Can't pair up requests to responses");
        }

        return allRequests.keySet().stream()
                .map(key -> {
                    var request = Objects.requireNonNull(allRequests.get(key));
                    if (!allResponses.containsKey(key)) {
                        throw new NoSuchElementException("No response found for request with API key: " + key);
                    }

                    var response = allResponses.get(key);
                    var name = request.name().replaceFirst("Request$", "");
                    var listeners = Set.copyOf(new HashSet<>(request.listeners()));
                    return new ApiSpec(name, ApiKeys.forId(key), listeners, request, response);
                })
                .sorted(Comparator.comparing(Named::name))
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    @NonNull
    private StructRegistry buildStructRegistry(MessageSpec messageSpec) {
        var structRegistry = new StructRegistry();
        structRegistry.register(messageSpec);
        return structRegistry;
    }

    private boolean shouldSkip(String finalFileName) {
        if (skipOutputIfSourceExists) {
            var sourceFile = Objects.requireNonNull(sourceDir).toPath().resolve(finalFileName);
            if (sourceFile.toFile().exists()) {
                logger.log(Level.DEBUG, "Skipping generation of {0} as (ungenerated) source already exists in the project source tree.", finalFileName);
                return true;
            }
        }
        return false;
    }

}
