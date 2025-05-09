/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.doxylicious.exec;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.doxylicious.Base;
import io.kroxylicious.doxylicious.model.ExecDecl;
import io.kroxylicious.doxylicious.model.ProcDecl;
import io.kroxylicious.doxylicious.model.StepDecl;

public class ProcExecutor {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProcExecutor.class);

    private final Base base;
    private final Duration timeout;
    private final Duration destroyTimeout;

    public ProcExecutor(Base base) {
        this(base, Duration.ofSeconds(30), Duration.ofSeconds(10));
    }

    public ProcExecutor(Base base, Duration timeout, Duration destroyTimeout) {
        this.base = base;
        this.timeout = timeout;
        this.destroyTimeout = destroyTimeout;
    }

    public void executeProcedure(ProcDecl procDecl) throws ExecException {
        LOGGER.atInfo().log("Executing procedure '{}'", procDecl.id());
        execList(procDecl.id() + "/procedure", procDecl.procedure());
        LOGGER.atInfo().log("Executed procedure '{}'", procDecl.id());
    }

    public void executeVerification(ProcDecl procDecl) throws ExecException {
        LOGGER.atInfo().log("Executing verification '{}'", procDecl.id());
        execList(procDecl.id() + "/verification", procDecl.verification());
        LOGGER.atInfo().log("Executed verification '{}'", procDecl.id());
    }

    public void executeTearDown(ProcDecl procDecl) throws ExecException {
        LOGGER.atInfo().log("Executing tearDown '{}'", procDecl.id());
        execList(procDecl.id() + "/tearDown", procDecl.tearDown());
        LOGGER.atInfo().log("Executed tearDown '{}'", procDecl.id());
    }

    private void execList(String path, List<StepDecl> steps) throws ExecException {
        if (steps.isEmpty()) {
            LOGGER.atInfo().log("Nothing to execute for {}", path);
        }
        else {
            for (int i = 0; i < steps.size(); i++) {
                StepDecl step = steps.get(i);
                execStep(path + "/" + i, step);
            }
        }
    }

    private void execStep(String path, StepDecl step) throws ExecException {
        if (step.substeps() != null) {
            execList(path + "/steps", step.substeps());
        }
        else if (step.exec() != null) {
            String arg = path + "/exec";
            LOGGER.atInfo().log("Executing {}", arg);
            exec(arg, step.exec());
        }
        else if (step.adoc() != null) {
            LOGGER.atInfo().log("Skipping non-executable step {}/adoc: {}", path, step.adoc());
        }
    }

    private static String argsAsString(List<String> args) {
        return args.stream().map(arg -> {
            if (!arg.contains("'")) {
                return '\'' + arg + '\'';
            }
            else if (!arg.contains("\"")) {
                return '"' + arg + '"';
            }
            else {
                // Not unambigous!
                return '\'' + arg.replace("'", "\\'") + '\'';
            }
        }).collect(Collectors.joining(", ", "[", "]"));
    }

    public ExecResult exec(String path, ExecDecl exec) throws ExecException {
        List<String> args;
        if (exec.args() != null) {
            args = exec.args();
        }
        else {
            args = Arrays.asList(exec.command().split("\\s+"));
        }
        LOGGER.atInfo().log("Executing {}", argsAsString(args));
        try {
            var dir = Files.createTempDirectory("");
            Path outPath = dir.resolve("out");
            File out = outPath.toFile();
            out.deleteOnExit();
            Path errPath = dir.resolve("err");
            File err = errPath.toFile();
            err.deleteOnExit();
            ProcessBuilder processBuilder = new ProcessBuilder();
            if (exec.env() != null) {
                Map<String, String> environment = processBuilder.environment();
                // environment.clear();
                environment.putAll(exec.env());
            }
            if (exec.dir() != null) {
                processBuilder.directory(new File(".."));
            }
            var process = processBuilder
                    .command(args)
                    .redirectOutput(out)
                    .redirectError(err)
                    .start();
            var exited = process.waitFor(Optional.ofNullable(exec.timeout()).orElse(timeout).toMillis(), TimeUnit.MILLISECONDS);
            if (exited) {
                if (Optional.ofNullable(exec.exitCodes()).orElse(Set.of(0)).contains(process.exitValue())) {
                    return handleExpectedErrorCode(exec, args, outPath, errPath, process.exitValue());
                }
                else {
                    throw handleUnexpectedErrorCode(path, process, args, errPath);
                }
            }
            else {
                throw handleTimeout(path, exec, process, args);
            }
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        }
        catch (IOException e) {
            throw new ExecException(String.format("'%s'", path), e);
        }
    }

    record ExecResult(int exitCode, String stdout, String stderr) {}

    private static ExecResult handleExpectedErrorCode(ExecDecl exec, List<String> args, Path outPath, Path errPath, int exitCode) throws IOException {
        LOGGER.atInfo().log("Command terminated normally: {}", argsAsString(args));
        String standardOutput = Files.readString(outPath);
        if (Files.size(outPath) == 0) {
            LOGGER.atInfo().log("No command output");
        }
        else {
            LOGGER.atInfo().log("Command output:\n{}", standardOutput);
        }
        String standardError = Files.readString(errPath);
        if (Files.size(errPath) == 0) {
            LOGGER.atInfo().log("No command error");
        }
        else {
            LOGGER.atInfo().log("Command error:\n{}", standardError);
        }
        if (exec.standardOutput() != null) {
            exec.standardOutput().makeAssertion("standard output from " + argsAsString(args), standardOutput);
        }
        if (exec.standardError() != null) {
            exec.standardError().makeAssertion("standard error from " + argsAsString(args), standardError);
        }
        return new ExecResult(exitCode, standardOutput, standardError);
    }

    private ExecException handleTimeout(String path, ExecDecl exec, Process process, List<String> args) throws InterruptedException, ExecException {
        boolean forciblyDestroyed = false;
        if (process.toHandle().isAlive()) {
            LOGGER.atInfo().log("Command {} has not terminated within timeout {}; destroying process", argsAsString(args), timeout);
            process.destroy();
            if (!process.waitFor(Optional.ofNullable(exec.destroyTimeout()).orElse(destroyTimeout).toMillis(), TimeUnit.MILLISECONDS)) {
                LOGGER.atInfo().log("Forcibly destroying process for command {} that has not terminated within destroy timeout {}", argsAsString(args),
                        destroyTimeout);
                process.destroyForcibly();
                forciblyDestroyed = true;
            }
        }
        return new ExecException(String.format(
                "'%s': Timed out waiting for command %s to exit (process %sdestroyed)",
                path,
                argsAsString(args),
                forciblyDestroyed ? "forcibly " : ""));
    }

    private static ExecException handleUnexpectedErrorCode(String path, Process process, List<String> args, Path errPath) throws IOException, ExecException {
        LOGGER.atInfo().log("Command terminated with exit code {}: {}", process.exitValue(), argsAsString(args));
        if (Files.size(errPath) == 0) {
            return new ExecException(String.format("'%s': Command %s failed with code %s and no error output",
                    path,
                    args,
                    process.exitValue()));
        }
        String errorOutput = Files.readString(errPath);
        return new ExecException(String.format("'%s': Command %s failed with code %s, error output %s",
                path,
                args,
                process.exitValue(),
                errorOutput));
    }

    public Suite suite(String name) {
        return suite(name, Set.of());
    }

    public Suite suite(String name, Set<String> assume) {
        return suite(name, assume, Map.of());
    }

    public Suite suite(String name, Set<String> assume, Map<String, String> fix) {
        assume.forEach(key -> {
            var stopProc = base.procDecl(key);
            if (stopProc == null) {
                throw new IllegalArgumentException(String.format("assume contains unknown proc '%s'", key));
            }
        });

        Map<String, Iterator<String>> remaining = new HashMap<>();
        LinkedHashSet<List<ProcDecl>> cases = new LinkedHashSet<>();
        do {
            LinkedHashSet<ProcDecl> procDecls = suiteRecursive(name, new HashSet<>(assume), new HashMap<>(fix), remaining);
            cases.add(procDecls.stream().toList());
        } while (!remaining.isEmpty());
        return new Suite(name, cases.stream().toList());
    }

    private LinkedHashSet<ProcDecl> suiteRecursive(String procName,
                                                   Set<String> assume,
                                                   Map<String, String> fix,
                                                   Map<String, Iterator<String>> remaining) {
        ProcDecl proc = base.procDecl(procName);
        if (proc == null) {
            throw new IllegalArgumentException(String.format("Unknown proc '%s'", procName));
        }
        if (assume.contains(procName)) {
            return new LinkedHashSet<>(0);
        }
        LinkedHashSet<ProcDecl> result = new LinkedHashSet<>();
        for (var pr : proc.prereqs()) {
            result.addAll(suiteRecursive(pr.ref(), assume, fix, remaining)); // recurse
        }
        if (proc.notional()) {
            var it = remaining.get(procName);
            if (it == null) {
                Set<String> sat = base.transitiveSatisfiers(procName);
                // narrow by fixed
                String fixed = fix.get(procName);
                if (fixed != null) {
                    sat.retainAll(Set.of(fixed));
                }
                it = sat.iterator();
                if (!it.hasNext()) {
                    throw new IllegalStateException("Notional proc '%s' has not satisfiers".formatted(procName));
                }
                remaining.put(procName, it);
            }
            if (!it.hasNext()) {
                throw new IllegalStateException("Found an empty iterator for " + procName);
            }
            String picked = it.next();
            fix.put(procName, picked);
            result.addAll(suiteRecursive(picked, assume, fix, remaining));
            if (!it.hasNext()) {
                remaining.remove(procName);
            }
        }
        else {
            result.add(proc);
        }
        assume.add(procName);
        return result;
    }

}
