/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.executor;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.systemtests.k8s.exception.KubeClusterException;

import static java.lang.String.join;

/**
 * Class provide execution of external command
 */
@SuppressWarnings({ "checkstyle:ClassDataAbstractionCoupling", "checkstyle:CyclomaticComplexity", "checkstyle:NPathComplexity" })
public class Exec {
    private static final Logger LOGGER = LoggerFactory.getLogger(Exec.class);
    private static final Pattern ERROR_PATTERN = Pattern.compile("Error from server \\(([a-zA-Z0-9]+)\\):");
    private static final Pattern INVALID_PATTERN = Pattern.compile("The ([a-zA-Z0-9]+) \"([a-z0-9.-]+)\" is invalid:");
    private static final Pattern PATH_SPLITTER = Pattern.compile(System.getProperty("path.separator"));
    private static final int MAXIMUM_EXEC_LOG_CHARACTER_SIZE = Integer.parseInt(System.getenv().getOrDefault("STRIMZI_EXEC_MAX_LOG_OUTPUT_CHARACTERS", "20000"));
    private static final Object LOCK = new Object();

    /**
     * The Process.
     */
    private Process process;
    private String stdOut;
    private String stdErr;
    private StreamGobbler stdOutReader;
    private StreamGobbler stdErrReader;
    private Path logPath;

    /**
     * Instantiates a new Exec.
     */
    private Exec() {
    }

    /**
     * Getter for stdOutput
     *
     * @return string stdOut
     */
    public String out() {
        return stdOut;
    }

    /**
     * Getter for stdErrorOutput
     *
     * @return string stdErr
     */
    public String err() {
        return stdErr;
    }

    /**
     * Is running boolean.
     *
     * @return the boolean
     */
    public boolean isRunning() {
        return process.isAlive();
    }

    /**
     * Gets ret code.
     *
     * @return the ret code
     */
    public int getRetCode() {
        LOGGER.info("Process: {}", process);
        if (isRunning()) {
            return -1;
        }
        else {
            return process.exitValue();
        }
    }

    /**
     * Method executes external command
     *
     * @param dir the dir
     * @param command arguments for command
     * @return execution results
     */
    public static ExecResult exec(File dir, String... command) {
        return exec(Arrays.asList(command), dir);
    }

    /**
     * Exec exec result.
     *
     * @param command the command
     * @return the exec result
     */
    public static ExecResult exec(String... command) {
        return exec(Arrays.asList(command), null);
    }

    /**
     * Exec without wait.
     *
     * @param command the command
     * @return the pid
     */
    public static long execWithoutWait(String... command) {
        return execWithoutWait(Arrays.asList(command));
    }

    /**
     * Method executes external command
     *
     * @param command arguments for command
     * @param dir the dir
     * @return execution results
     */
    public static ExecResult exec(List<String> command, File dir) {
        return exec(null, command, Duration.ZERO, false, dir);
    }

    /**
     * Exec exec result.
     *
     * @param command the command
     * @return the exec result
     */
    public static ExecResult exec(List<String> command) {
        return exec(null, command, Duration.ZERO, false);
    }

    /**
     * Method executes external command
     *
     * @param input the input
     * @param command arguments for command
     * @return execution results
     */
    public static ExecResult exec(String input, List<String> command) {
        return exec(input, command, Duration.ZERO, false);
    }

    /**
     * Method executes external command
     * @param input the input
     * @param command arguments for command
     * @param timeout timeout for execution
     * @param logToOutput log output or not
     * @param dir the dir
     * @return execution results
     */
    public static ExecResult exec(String input, List<String> command, Duration timeout, boolean logToOutput, File dir) {
        return exec(input, command, timeout, logToOutput, true, dir);
    }

    /**
     * Exec without wait exec result.
     *
     * @param command the command
     * @return the pid
     */
    public static long execWithoutWait(List<String> command) {
        Exec executor = new Exec();
        return executor.executeWithoutWait(command, null);
    }

    /**
     * Exec exec result.
     *
     * @param input the input
     * @param command the command
     * @param timeout the timeout
     * @param logToOutput the log to output
     * @return the exec result
     */
    public static ExecResult exec(String input, List<String> command, Duration timeout, boolean logToOutput) {
        return exec(input, command, timeout, logToOutput, true, null);
    }

    /**
     * Method executes external command
     * @param input the input
     * @param command arguments for command
     * @param timeout timeout for execution
     * @param logToOutput log output or not
     * @param throwErrors look for errors in output and throws exception if true
     * @param dir the dir
     * @return execution results
     */
    public static ExecResult exec(String input, List<String> command, Duration timeout, boolean logToOutput, boolean throwErrors, File dir) {
        int ret;
        ExecResult execResult;
        try {
            Exec executor = new Exec();
            ret = executor.execute(input, command, timeout, dir);
            synchronized (LOCK) {
                if (logToOutput || ret != 0) {
                    logExecutor(ret, input, command, executor.out(), executor.err());
                }
            }

            execResult = new ExecResult(ret, executor.out(), executor.err());

            if (throwErrors && ret != 0) {
                String msg = "`" + join(" ", command) + "` got status code " + ret + " and stderr:\n------\n" + executor.stdErr + "\n------\nand stdout:\n------\n"
                        + executor.stdOut + "\n------";

                throwExceptionForErrorPattern(msg, executor.err(), execResult);
            }
            return execResult;

        }
        catch (IOException | ExecutionException e) {
            throw new KubeClusterException(e);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new KubeClusterException(e);
        }
    }

    private static void throwExceptionForErrorPattern(String msg, String err, ExecResult execResult) {
        Matcher matcher = ERROR_PATTERN.matcher(err);
        KubeClusterException kubeClusterException = new KubeClusterException(execResult, msg);

        if (matcher.find()) {
            switch (matcher.group(1)) {
                case "NotFound":
                    kubeClusterException = new KubeClusterException.NotFound(execResult, msg);
                    break;
                case "AlreadyExists":
                    kubeClusterException = new KubeClusterException.AlreadyExists(execResult, msg);
                    break;
                default:
                    break;
            }
        }
        matcher = INVALID_PATTERN.matcher(err);
        if (matcher.find()) {
            kubeClusterException = new KubeClusterException.InvalidResource(execResult, msg);
        }
        throw kubeClusterException;
    }

    private static void logExecutor(int ret, String input, List<String> command, String execOut, String execErr) {
        String log = ret != 0 ? "Failed to exec command" : "Command";
        String commandLine = command == null || command.isEmpty() ? "" : String.join(" ", command);
        LOGGER.info("{}: {}", log, commandLine);
        if (input != null && !input.contains("CustomResourceDefinition")) {
            LOGGER.info("Input: {}", input);
        }
        LOGGER.info("RETURN code: {}", ret);
        if (!execOut.isEmpty()) {
            LOGGER.debug("======STDOUT START=======");
            LOGGER.debug("{}", cutExecutorLog(execOut));
            LOGGER.debug("======STDOUT END======");
        }
        if (!execErr.isEmpty()) {
            LOGGER.debug("======STDERR START=======");
            LOGGER.debug("{}", cutExecutorLog(execErr));
            LOGGER.debug("======STDERR END======");
        }
    }

    /**
     * Method executes external command
     *
     * @param input the input
     * @param commands arguments for command
     * @param timeout timeout allowed for the process's execution. If the timeout is exceed the process will be killed.
     * @param dir the dir
     * @return returns ecode of execution
     * @throws IOException the io exception
     * @throws InterruptedException the interrupted exception
     * @throws ExecutionException the execution exception
     */
    public int execute(String input, List<String> commands, Duration timeout, File dir) throws IOException, InterruptedException, ExecutionException {
        LOGGER.debug("Running command - {}", String.join(" ", commands.toArray(new String[0])));
        ProcessBuilder builder = new ProcessBuilder();
        builder.command(commands);
        dir = dir == null ? new File(System.getProperty("user.dir")) : dir;
        builder.directory(dir);
        process = builder.start();
        OutputStream outputStream = process.getOutputStream();
        if (input != null) {
            LOGGER.debug("With stdin {}", input);
            outputStream.write(input.getBytes(Charset.defaultCharset()));
        }
        // Close subprocess' stdin
        outputStream.close();

        Future<String> output = readStdOutput();
        Future<String> error = readStdError();

        int retCode = 1;
        if (timeout.toMillis() > 0) {
            if (process.waitFor(timeout.toMillis(), TimeUnit.MILLISECONDS)) {
                retCode = process.exitValue();
            }
            else {
                process.destroyForcibly();
            }
        }
        else {
            retCode = process.waitFor();
        }

        try {
            stdOut = output.get(500, TimeUnit.MILLISECONDS);
        }
        catch (TimeoutException ex) {
            output.cancel(true);
            stdOut = stdOutReader.getData();
        }

        try {
            stdErr = error.get(500, TimeUnit.MILLISECONDS);
        }
        catch (TimeoutException ex) {
            error.cancel(true);
            stdErr = stdErrReader.getData();
        }
        storeOutputsToFile();

        return retCode;
    }

    /**
     * Execute without waiting for response.
     *
     * @param commands the commands
     * @param dir the dir
     * @return the pid
     */
    public long executeWithoutWait(List<String> commands, File dir) {
        LOGGER.debug("Running command - {}", String.join(" ", commands.toArray(new String[0])));
        ProcessBuilder builder = new ProcessBuilder();
        builder.command(commands);
        dir = dir == null ? new File(System.getProperty("user.dir")) : dir;
        builder.directory(dir);
        try {
            process = builder.start();
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
        return process.pid();
    }

    /**
     * Get standard output of execution
     *
     * @return future string output
     */
    private Future<String> readStdOutput() {
        stdOutReader = new StreamGobbler(process.getInputStream());
        return stdOutReader.read();
    }

    /**
     * Get standard error output of execution
     *
     * @return future string error output
     */
    private Future<String> readStdError() {
        stdErrReader = new StreamGobbler(process.getErrorStream());
        return stdErrReader.read();
    }

    /**
     * Get stdOut and stdErr and store it into files
     */
    private void storeOutputsToFile() {
        if (logPath != null) {
            try {
                Files.createDirectories(logPath);
                Files.writeString(Paths.get(logPath.toString(), "stdOutput.log"), stdOut, Charset.defaultCharset());
                Files.writeString(Paths.get(logPath.toString(), "stdError.log"), stdErr, Charset.defaultCharset());
            }
            catch (Exception ex) {
                LOGGER.warn("Cannot save output of execution: {}", ex.getMessage());
            }
        }
    }

    /**
     * Check if command is executable
     * @param cmd command
     * @return true.false boolean
     */
    public static boolean isExecutableOnPath(String cmd) {
        for (String dir : PATH_SPLITTER.split(System.getenv("PATH"))) {
            if (new File(dir, cmd).canExecute()) {
                return true;
            }
        }
        return false;
    }

    /**
     * This method check the size of executor output log and cut it if it's too long.
     * @param log executor log
     * @return updated log if size is too big
     */
    public static String cutExecutorLog(String log) {
        if (log.trim().length() > MAXIMUM_EXEC_LOG_CHARACTER_SIZE) {
            LOGGER.warn("Executor log is too long. Going to strip it and print only first {} characters", MAXIMUM_EXEC_LOG_CHARACTER_SIZE);
            return log.trim().substring(0, MAXIMUM_EXEC_LOG_CHARACTER_SIZE);
        }
        return log.trim();
    }

    /**
     * Class represent async reader
     */
    class StreamGobbler {
        private final InputStream is;
        private final StringBuilder data = new StringBuilder();

        /**
         * Constructor of StreamGobbler
         *
         * @param is input stream for reading
         */
        StreamGobbler(InputStream is) {
            this.is = is;
        }

        /**
         * Return data from stream sync
         *
         * @return string of data
         */
        public String getData() {
            return data.toString();
        }

        /**
         * read method
         *
         * @return return future string of output
         */
        public Future<String> read() {
            return CompletableFuture.supplyAsync(() -> {
                try {
                    return new String(is.readAllBytes(), StandardCharsets.UTF_8);
                }
                catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }, runnable -> new Thread(runnable).start());
        }
    }
}