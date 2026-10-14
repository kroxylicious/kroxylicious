/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.migrations.cli;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;

import org.openrewrite.ExecutionContext;
import org.openrewrite.InMemoryExecutionContext;
import org.openrewrite.PrintOutputCapture;
import org.openrewrite.Recipe;
import org.openrewrite.RecipeRun;
import org.openrewrite.Result;
import org.openrewrite.SourceFile;
import org.openrewrite.Tree;
import org.openrewrite.TreeVisitor;
import org.openrewrite.config.Environment;
import org.openrewrite.internal.InMemoryLargeSourceSet;
import org.openrewrite.marker.Markup;
import org.openrewrite.yaml.YamlParser;

import edu.umd.cs.findbugs.annotations.Nullable;
import picocli.CommandLine.Command;
import picocli.CommandLine.ExitCode;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.ParameterException;
import picocli.CommandLine.Parameters;
import picocli.CommandLine.Spec;

/**
 * Converts proxy configuration files to the form used by the current release.
 * <p>
 * The files are parsed as YAML and handed straight to the recipes, so a configuration file is converted wherever it
 * lives and whatever it is called. In particular, unlike the OpenRewrite build plugins, this does not consult
 * {@code .gitignore} and does not require the file to sit inside a project.
 */
@Command(name = "convert-config", mixinStandardHelpOptions = true, description = "Converts proxy configuration files to the form used by the current release")
class ConvertConfigCommand implements Callable<Integer> {

    /**
     * Recipes which do not apply to a YAML configuration, such as those rewriting Java sources or bumping dependency
     * versions, find nothing to do here, so the whole aggregate is run rather than an enumeration of the
     * configuration recipes which would have to be kept in step with it.
     */
    private static final String RECIPE = "io.kroxylicious.migrations.rewrite.MigrateToLatest";

    /**
     * Returned when every given file was processed without error, but at least one still carries a
     * {@link Markup.Warn} marker calling out a virtual cluster that could not be migrated automatically.
     */
    static final int NEEDS_MANUAL_MIGRATION = 3;

    @Spec
    private @Nullable CommandSpec spec;

    @Parameters(arity = "1..*", paramLabel = "FILE", description = "the configuration files to convert")
    private @Nullable List<Path> configFiles;

    @Option(names = { "-n", "--dry-run" }, description = "report the changes which would be made, without writing them")
    private boolean dryRun;

    @Override
    public Integer call() throws IOException {
        Objects.requireNonNull(spec, "spec");
        Objects.requireNonNull(configFiles, "configFiles");
        for (Path configFile : configFiles) {
            if (!Files.isRegularFile(configFile)) {
                throw new ParameterException(spec.commandLine(), String.format("Given configuration file does not exist: %s", configFile.toAbsolutePath()));
            }
        }

        PrintWriter out = spec.commandLine().getOut();
        PrintWriter err = spec.commandLine().getErr();
        ExecutionContext ctx = new InMemoryExecutionContext(error -> {
            throw new IllegalStateException("Conversion failed", error);
        });
        List<SourceFile> sources = YamlParser.builder().build().parse(configFiles, null, ctx).toList();
        Recipe recipe = Environment.builder().scanRuntimeClasspath().build().activateRecipes(RECIPE);

        RecipeRun run = recipe.run(new InMemoryLargeSourceSet(sources), ctx);
        List<Result> results = run.getChangeset().getAllResults();
        boolean needsManualMigration = false;
        for (Result result : results) {
            SourceFile after = Objects.requireNonNull(result.getAfter(), "after");
            if (hasUnresolvedWarning(after)) {
                needsManualMigration = true;
                // CHECKSTYLE:OFF RegexpSinglelineJava - CLI tool legitimately writes to stderr
                err.println(result.diff());
                // CHECKSTYLE:ON RegexpSinglelineJava
            }
            else {
                out.println(result.diff());
            }
            if (!dryRun) {
                PrintOutputCapture<Integer> captureWithoutDiagnosticMarkup = new PrintOutputCapture<>(0, PrintOutputCapture.MarkerPrinter.SANITIZED);
                String sanitizedOutput = after.printAll(captureWithoutDiagnosticMarkup);
                Files.writeString(after.getSourcePath(), sanitizedOutput);
            }
        }

        if (results.isEmpty()) {
            out.println("No changes required.");
        }
        else {
            out.printf("%s %d of %d file(s).%n", dryRun ? "Would convert" : "Converted", results.size(), configFiles.size());
        }
        return needsManualMigration ? NEEDS_MANUAL_MIGRATION : ExitCode.OK;
    }

    /**
     * Detects whether any node in the given tree still carries a {@link Markup.Warn} marker, meaning the
     * recipes declined to migrate part of it and left a warning behind instead.
     */
    private static boolean hasUnresolvedWarning(SourceFile sourceFile) {
        AtomicBoolean found = new AtomicBoolean(false);
        new TreeVisitor<Tree, AtomicBoolean>() {
            @Override
            public Tree preVisit(Tree tree, AtomicBoolean accumulator) {
                if (tree.getMarkers().findFirst(Markup.Warn.class).isPresent()) {
                    accumulator.set(true);
                    stopAfterPreVisit();
                }
                return tree;
            }
        }.visit(sourceFile, found);
        return found.get();
    }
}
