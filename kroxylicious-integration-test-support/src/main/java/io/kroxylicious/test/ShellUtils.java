/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Stream;

import org.assertj.core.util.Sets;
import org.opentest4j.TestAbortedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThatCode;

public final class ShellUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(ShellUtils.class);

    private static final Function<Stream<String>, Boolean> ALWAYS_VALID = lines -> true;

    private ShellUtils() {
        throw new UnsupportedOperationException();
    }

    public static void exec(String... args) {
        execValidate(ALWAYS_VALID, ALWAYS_VALID, args);
    }

    public static boolean execValidate(Function<Stream<String>, Boolean> stdOutValidator, Function<Stream<String>, Boolean> stdErrValidator, String... args) {
        Process process = null;
        try {
            List<String> argList = List.of(args);
            LOGGER.info("Executing '{}'", String.join(" ", argList));
            var out = Files.createTempFile(ShellUtils.class.getSimpleName(), ".out");
            var err = Files.createTempFile(ShellUtils.class.getSimpleName(), ".err");
            process = new ProcessBuilder()
                    .command(argList)
                    .redirectOutput(out.toFile())
                    .redirectError(err.toFile())
                    .start();
            boolean exited = process.waitFor(5, TimeUnit.MINUTES);
            if (exited) {
                if (process.exitValue() != 0 || LOGGER.isDebugEnabled()) {
                    String description = ("'%s' exited with value: %d%n"
                            + "standard error output:%n"
                            + "---%n"
                            + "%s---%n"
                            + "standard output:%n"
                            + "---%n"
                            + "%s---").formatted(
                                    argList,
                                    process.exitValue(),
                                    Files.readString(err),
                                    Files.readString(out));
                    LOGGER.info(description);
                }
                assertThat(process.exitValue()).describedAs(argList + " should have 0 exit code").isZero();
                var rtnValue = stdOutValidator.apply(Files.lines(out));
                rtnValue &= stdErrValidator.apply(Files.lines(err));
                return rtnValue;
            }
            else {
                process.destroy();
                boolean killed = process.waitFor(5, TimeUnit.SECONDS);
                if (!killed) {
                    process.destroyForcibly();
                    process.waitFor(5, TimeUnit.SECONDS);
                }
                throw new AssertionError("Process " + argList + " did not complete within timeout");
            }

        }

        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted whilst awaiting process" + process, e);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static boolean isToolOnPath(String tool) {
        LOGGER.info("Checking whether {} is available", tool);
        try {
            assumeThatCode(() -> exec(tool))
                    .describedAs(tool + " must be available on the path")
                    .doesNotThrowAnyException();
            return true;
        }
        catch (TestAbortedException abort) {
            LOGGER.warn("{}", abort.getMessage());
            return false;
        }
    }

    public static boolean validateToolsOnPath(String... additionalTools) {
        Set<String> tools = Sets.newLinkedHashSet(additionalTools);
        try {
            return tools.stream().allMatch(ShellUtils::isToolOnPath);
        }
        catch (TestAbortedException abort) {
            LOGGER.warn("{}", abort.getMessage());
        }
        return false;
    }

    public static boolean validateKubeContext(String expectedContext) {
        return validateToolsOnPath("kubectl")
                && execValidate(lines -> lines.anyMatch(line -> line.contains(expectedContext)), ALWAYS_VALID, "kubectl", "config", "current-context");
    }

}
