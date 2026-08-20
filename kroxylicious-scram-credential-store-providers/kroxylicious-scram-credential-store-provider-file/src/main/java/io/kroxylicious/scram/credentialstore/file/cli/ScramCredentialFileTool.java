/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.scram.credentialstore.file.cli;

import java.io.Console;
import java.nio.file.Path;
import java.security.KeyStoreException;
import java.util.List;
import java.util.concurrent.Callable;

import org.apache.kafka.common.security.scram.internals.ScramMechanism;

import io.kroxylicious.scram.credentialstore.file.CredentialValidationException;
import io.kroxylicious.scram.credentialstore.file.ScramCredentialFileManager;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.ParentCommand;

/**
 * Command-line tool for managing SCRAM credentials in proxy SCRAM credential files.
 * <p>
 * Provides commands to create credential files and manage users (add, remove, update password, list).
 * </p>
 * <p>
 * <strong>Usage:</strong>
 * </p>
 * <pre>{@code
 * # Create a new credential file
 * scram-credential-file-tool create -k credentials.p12 -p password
 *
 * # Add a user
 * scram-credential-file-tool add-user -k credentials.p12 -p password -u alice -w alice-secret
 *
 * # List users
 * scram-credential-file-tool list-users -k credentials.p12 -p password
 *
 * # Update password
 * scram-credential-file-tool update-password -k credentials.p12 -p password -u alice -w new-password
 *
 * # Remove user
 * scram-credential-file-tool remove-user -k credentials.p12 -p password -u alice
 * }</pre>
 */
@Command(name = "scram-credential-file-tool", description = "Manage SCRAM credentials in proxy SCRAM credential files", mixinStandardHelpOptions = true, subcommands = {
        ScramCredentialFileTool.CreateCommand.class,
        ScramCredentialFileTool.AddUserCommand.class,
        ScramCredentialFileTool.RemoveUserCommand.class,
        ScramCredentialFileTool.UpdatePasswordCommand.class,
        ScramCredentialFileTool.ListUsersCommand.class,
        CommandLine.HelpCommand.class
})
public class ScramCredentialFileTool implements Callable<Integer> {

    private static final String KEYSTORE_PASSWORD_PROMPT = "KeyStore password";

    /** Creates a new instance; called by picocli. */
    public ScramCredentialFileTool() {
        // required by picocli
    }

    @CommandLine.Spec
    CommandLine.Model.CommandSpec spec;

    @Option(names = { "--unlock-insecure-options" }, description = "Unlock password options (NOT RECOMMENDED: passwords visible in process listings and shell history)")
    boolean unlockInsecureOptions;

    @Override
    public Integer call() {
        spec.commandLine().usage(spec.commandLine().getOut());
        return 0;
    }

    /**
     * Read a password interactively from console.
     *
     * @param prompt the prompt to display
     * @return the password, or null if console not available
     */
    @SuppressWarnings("SystemConsoleNull") // Project targets Java 21 where System.console() can still return null
    static String readPasswordFromConsole(String prompt) {
        Console console = System.console();
        if (console == null) {
            return null;
        }
        char[] passwordChars = console.readPassword("%s (minimum 12 characters): ", prompt);
        if (passwordChars == null) {
            return null;
        }
        return new String(passwordChars);
    }

    /**
     * Get a password, either from option (if unlocked) or from console.
     *
     * @param optionValue the password option value (may be null)
     * @param unlocked whether insecure options are unlocked
     * @param prompt the console prompt
     * @param confirm if true and reading interactively, prompt a second time to confirm
     * @param err the error stream for warnings
     * @return the password
     * @throws IllegalStateException if password option used without unlock, console not available, or confirmation mismatch
     */
    static String getPassword(
                              String optionValue,
                              boolean unlocked,
                              String prompt,
                              boolean confirm,
                              java.io.PrintWriter err) {
        if (optionValue != null) {
            if (!unlocked) {
                throw new IllegalStateException(
                        "Password options are disabled by default for security. " +
                                "Use --unlock-insecure-options to enable them, or omit the password option to be prompted interactively. " +
                                "SECURITY WARNING: Command-line passwords are visible in process listings, shell history, and system logs. " +
                                "Prefer interactive prompts or environment variables.");
            }
            // Warn about insecure usage
            // CHECKSTYLE:OFF RegexpSinglelineJava - CLI tool legitimately writes to stderr
            err.println("SECURITY WARNING: Password provided via command-line option.");
            err.println("This is NOT RECOMMENDED as passwords are visible in:");
            err.println("  - Process listings (ps, top, /proc/<pid>/cmdline)");
            err.println("  - Shell history (.bash_history, .zsh_history, etc.)");
            err.println("  - System audit logs");
            err.println("Prefer:");
            err.println("  - Interactive password prompts (omit -p/-w options)");
            err.println("  - Environment variables");
            err.println("  - Password files with restricted permissions");
            err.println();
            // CHECKSTYLE:ON RegexpSinglelineJava
            return optionValue;
        }

        // Read interactively
        String password = readPasswordFromConsole(prompt);
        if (password == null) {
            throw new IllegalStateException(
                    "Cannot read password interactively (no console available). " +
                            "Either run from an interactive terminal, or use --unlock-insecure-options with password options.");
        }

        if (confirm) {
            String confirmation = readPasswordFromConsole("Confirm " + prompt.substring(0, 1).toLowerCase(java.util.Locale.ROOT) + prompt.substring(1));
            if (!password.equals(confirmation)) {
                throw new IllegalStateException("Passwords do not match.");
            }
        }

        return password;
    }

    /**
     * Create a new KeyStore file.
     */
    @Command(name = "create", description = "Create a new KeyStore file", sortOptions = false, sortSynopsis = false)
    static class CreateCommand implements Callable<Integer> {

        @ParentCommand
        ScramCredentialFileTool parent;

        @CommandLine.Spec
        CommandLine.Model.CommandSpec spec;

        @Option(names = { "-k", "--keystore" }, description = "Path to the KeyStore file", required = true)
        Path keystorePath;

        @Option(names = { "-p", "--password" }, description = "Optional store password; requires --unlock-insecure-options. "
                + "When omitted the store password will be prompted for interactively.")
        String password;

        @Override
        public Integer call() {
            return executeCommand(() -> {
                String keystorePassword = getPassword(
                        password,
                        parent.unlockInsecureOptions,
                        KEYSTORE_PASSWORD_PROMPT,
                        true,
                        spec.commandLine().getErr());

                ScramCredentialFileManager manager = new ScramCredentialFileManager();
                manager.createKeyStore(keystorePath, keystorePassword);
                spec.commandLine().getOut().println("KeyStore created successfully: " + keystorePath);
            }, "Failed to create KeyStore", spec.commandLine().getErr());
        }
    }

    /**
     * Add a user to a KeyStore.
     */
    @Command(name = "add-user", description = "Add a user to the KeyStore", sortOptions = false, sortSynopsis = false)
    static class AddUserCommand implements Callable<Integer> {

        @ParentCommand
        ScramCredentialFileTool parent;

        @CommandLine.Spec
        CommandLine.Model.CommandSpec spec;

        @Option(names = { "-k", "--keystore" }, description = "Path to the KeyStore file", required = true)
        Path keystorePath;

        @Option(names = { "-u", "--username" }, description = "Username to add", required = true)
        String username;

        @Option(names = { "-m",
                "--mechanism" }, description = "Optional SCRAM mechanism: ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE})", defaultValue = "SCRAM-SHA-256", converter = ScramMechanismType.Converter.class)
        ScramMechanismType mechanism;

        @Option(names = { "-i",
                "--iterations" }, description = "Optional PBKDF2 iteration count (default: ${DEFAULT-VALUE}, minimum: 4096)", defaultValue = "10000")
        int iterations;

        @Option(names = { "-p", "--password" }, description = "Optional store password; requires --unlock-insecure-options. "
                + "When omitted the store password will be prompted for interactively.")
        String storePassword;

        @Option(names = { "-w", "--user-password" }, description = "Optional user's password; requires --unlock-insecure-options. "
                + "When omitted the user password will be prompted for interactively.")
        String userPassword;

        @Override
        public Integer call() {
            return executeCommand(() -> {
                String keystorePassword = getPassword(
                        storePassword,
                        parent.unlockInsecureOptions,
                        KEYSTORE_PASSWORD_PROMPT,
                        false,
                        spec.commandLine().getErr());

                String password = getPassword(
                        userPassword,
                        parent.unlockInsecureOptions,
                        "Password for user '" + username + "'",
                        true,
                        spec.commandLine().getErr());

                ScramCredentialFileManager manager = new ScramCredentialFileManager();
                manager.addUser(keystorePath, keystorePassword, username, password, mechanism.toScramMechanism(), iterations);
                spec.commandLine().getOut().println("User '" + username + "' added successfully");
            }, "Failed to add user", spec.commandLine().getErr());
        }
    }

    /**
     * Remove a user from a KeyStore.
     */
    @Command(name = "remove-user", description = "Remove a user from the KeyStore", sortOptions = false, sortSynopsis = false)
    static class RemoveUserCommand implements Callable<Integer> {

        @ParentCommand
        ScramCredentialFileTool parent;

        @CommandLine.Spec
        CommandLine.Model.CommandSpec spec;

        @Option(names = { "-k", "--keystore" }, description = "Path to the KeyStore file", required = true)
        Path keystorePath;

        @Option(names = { "-u", "--username" }, description = "Username to remove", required = true)
        String username;

        @Option(names = { "-p", "--password" }, description = "Optional store password; requires --unlock-insecure-options. "
                + "When omitted the store password will be prompted for interactively.")
        String password;

        @Override
        public Integer call() {
            return executeCommand(() -> {
                String keystorePassword = getPassword(
                        password,
                        parent.unlockInsecureOptions,
                        KEYSTORE_PASSWORD_PROMPT,
                        false,
                        spec.commandLine().getErr());

                ScramCredentialFileManager manager = new ScramCredentialFileManager();
                manager.removeUser(keystorePath, keystorePassword, username);
                spec.commandLine().getOut().println("User '" + username + "' removed successfully");
            }, "Failed to remove user", spec.commandLine().getErr());
        }
    }

    /**
     * Update a user's password.
     */
    @Command(name = "update-password", description = "Update a user's password", sortOptions = false, sortSynopsis = false)
    static class UpdatePasswordCommand implements Callable<Integer> {

        @ParentCommand
        ScramCredentialFileTool parent;

        @CommandLine.Spec
        CommandLine.Model.CommandSpec spec;

        @Option(names = { "-k", "--keystore" }, description = "Path to the KeyStore file", required = true)
        Path keystorePath;

        @Option(names = { "-u", "--username" }, description = "Username", required = true)
        String username;

        @Option(names = { "-m",
                "--mechanism" }, description = "Optional SCRAM mechanism: ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE})", defaultValue = "SCRAM-SHA-256", converter = ScramMechanismType.Converter.class)
        ScramMechanismType mechanism;

        @Option(names = { "-i",
                "--iterations" }, description = "Optional PBKDF2 iteration count (default: ${DEFAULT-VALUE}, minimum: 4096)", defaultValue = "10000")
        int iterations;

        @Option(names = { "-p", "--password" }, description = "Optional store password; requires --unlock-insecure-options. "
                + "When omitted the store password will be prompted for interactively.")
        String storePassword;

        @Option(names = { "-w", "--new-password" }, description = "Optional new password for the user; requires --unlock-insecure-options. "
                + "When omitted the new user password will be prompted for interactively.")
        String newPassword;

        @Override
        public Integer call() {
            return executeCommand(() -> {
                String keystorePassword = getPassword(
                        storePassword,
                        parent.unlockInsecureOptions,
                        KEYSTORE_PASSWORD_PROMPT,
                        false,
                        spec.commandLine().getErr());

                String password = getPassword(
                        newPassword,
                        parent.unlockInsecureOptions,
                        "New password for user '" + username + "'",
                        true,
                        spec.commandLine().getErr());

                ScramCredentialFileManager manager = new ScramCredentialFileManager();
                manager.updatePassword(keystorePath, keystorePassword, username, password, mechanism.toScramMechanism(), iterations);
                spec.commandLine().getOut().println("Password for user '" + username + "' updated successfully");
            }, "Failed to update password", spec.commandLine().getErr());
        }
    }

    /**
     * List all users in a KeyStore.
     */
    @Command(name = "list-users", description = "List all users in the KeyStore", sortOptions = false, sortSynopsis = false)
    static class ListUsersCommand implements Callable<Integer> {

        @ParentCommand
        ScramCredentialFileTool parent;

        @CommandLine.Spec
        CommandLine.Model.CommandSpec spec;

        @Option(names = { "-k", "--keystore" }, description = "Path to the KeyStore file", required = true)
        Path keystorePath;

        @Option(names = { "-p", "--password" }, description = "KeyStore password; requires --unlock-insecure-options (omit to be prompted interactively)")
        String password;

        @Override
        public Integer call() {
            return executeCommand(() -> {
                String keystorePassword = getPassword(
                        password,
                        parent.unlockInsecureOptions,
                        KEYSTORE_PASSWORD_PROMPT,
                        false,
                        spec.commandLine().getErr());

                ScramCredentialFileManager manager = new ScramCredentialFileManager();
                List<ScramCredentialFileManager.UserCredentialInfo> credentials = manager.listCredentials(keystorePath, keystorePassword);

                if (credentials.isEmpty()) {
                    spec.commandLine().getOut().println("No users found in KeyStore");
                }
                else {
                    spec.commandLine().getOut().println("Users in KeyStore (" + credentials.size() + "):");
                    for (var info : credentials) {
                        spec.commandLine().getOut().println("  " + info.username() + "  " + info.mechanism() + "  iterations=" + info.iterations());
                    }
                }
            }, "Failed to list users", spec.commandLine().getErr());
        }
    }

    /**
     * Enum wrapper for SCRAM mechanism types.
     */
    enum ScramMechanismType {
        SCRAM_SHA_256,
        SCRAM_SHA_512;

        ScramMechanism toScramMechanism() {
            return switch (this) {
                case SCRAM_SHA_256 -> ScramMechanism.SCRAM_SHA_256;
                case SCRAM_SHA_512 -> ScramMechanism.SCRAM_SHA_512;
            };
        }

        @Override
        public String toString() {
            return toScramMechanism().mechanismName();
        }

        /** Picocli type converter that accepts the hyphenated IANA mechanism names. */
        static class Converter implements CommandLine.ITypeConverter<ScramMechanismType> {
            @Override
            public ScramMechanismType convert(String value) {
                ScramMechanism mechanism = ScramMechanism.forMechanismName(value);
                if (mechanism == null) {
                    throw new CommandLine.TypeConversionException("expected one of " +
                            java.util.Arrays.toString(values()) + " but was '" + value + "'");
                }
                return valueOf(mechanism.name());
            }
        }
    }

    /**
     * Action that may throw {@link KeyStoreException}.
     */
    @FunctionalInterface
    interface CommandAction {
        void execute() throws KeyStoreException;
    }

    /**
     * Execute a command action with standardised error handling.
     *
     * @param action the action to execute
     * @param errorContext context string for KeyStoreException messages (e.g. "Failed to create KeyStore")
     * @param err the error stream
     * @return exit code: 0 for success, 1 for validation/keystore errors, 2 for input errors
     */
    static int executeCommand(CommandAction action, String errorContext, java.io.PrintWriter err) {
        try {
            action.execute();
            return 0;
        }
        catch (CredentialValidationException e) {
            printError(err, e.getMessage());
            return 1;
        }
        catch (IllegalStateException e) {
            printError(err, e.getMessage());
            return 2;
        }
        catch (KeyStoreException e) {
            printError(err, formatError(errorContext, e));
            return 1;
        }
    }

    // CHECKSTYLE:OFF RegexpSinglelineJava - CLI tool legitimately writes to stderr
    private static void printError(java.io.PrintWriter err, String message) {
        err.println(CommandLine.Help.Ansi.AUTO.string("@|bold,red ERROR:|@ " + message));
    }
    // CHECKSTYLE:ON RegexpSinglelineJava

    private static String formatError(String message, Exception exception) {
        StringBuilder sb = new StringBuilder(message);
        if (exception.getMessage() != null) {
            sb.append(": ").append(exception.getMessage());
        }
        Throwable cause = exception.getCause();
        while (cause != null && cause.getMessage() != null) {
            sb.append(": ").append(cause.getMessage());
            cause = cause.getCause();
        }
        return sb.toString();
    }

    /**
     * Main entry point.
     *
     * @param args command-line arguments
     */
    public static void main(String[] args) {
        int exitCode = new CommandLine(new ScramCredentialFileTool()).execute(args);
        System.exit(exitCode);
    }
}
