/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.migrations.cli;

import java.util.Objects;
import java.util.concurrent.Callable;

import edu.umd.cs.findbugs.annotations.Nullable;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Spec;

/**
 * Entrypoint for applying migrations to files which are not part of a Maven or Gradle build.
 * <p>
 * Migrations of Java sources and build files are better run by the OpenRewrite build plugins, which supply the
 * classpath a Java recipe needs. This command exists for the files those plugins cannot conveniently reach, such as a
 * proxy configuration file living on its own outside any project.
 */
@Command(name = "kroxylicious-migrations", mixinStandardHelpOptions = true, versionProvider = KroxyliciousMigrations.VersionProvider.class, subcommands = {
        ConvertConfigCommand.class }, description = "Applies Kroxylicious migrations to files which are not part of a build")
public class KroxyliciousMigrations implements Callable<Integer> {

    @Spec
    private @Nullable CommandSpec spec;

    /**
     * Creates the top level command.
     */
    public KroxyliciousMigrations() {
    }

    /**
     * Invoked when no subcommand is given, printing the usage message.
     *
     * @return the exit code
     */
    @Override
    public Integer call() {
        Objects.requireNonNull(spec, "spec");
        spec.commandLine().usage(spec.commandLine().getOut());
        return CommandLine.ExitCode.USAGE;
    }

    /**
     * Migrations entry point.
     *
     * @param args args
     */
    public static void main(String... args) {
        // no SLF4J binding is shipped, since one would compete with the binding of a build running these recipes.
        // Without this the jgit used to render a diff would announce that absence on stderr, which is noise here.
        System.setProperty("slf4j.internal.verbosity", "ERROR");
        int exitCode = new CommandLine(new KroxyliciousMigrations()).execute(args);
        System.exit(exitCode);
    }

    /**
     * Reports the version recorded in the jar manifest.
     */
    static class VersionProvider implements CommandLine.IVersionProvider {
        @Override
        public String[] getVersion() {
            String version = KroxyliciousMigrations.class.getPackage().getImplementationVersion();
            return new String[]{ "kroxylicious-migrations: " + (version == null ? "unknown" : version) };
        }
    }
}
