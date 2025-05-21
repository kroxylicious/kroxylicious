/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.doxylicious;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nullable;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;

import io.kroxylicious.doxylicious.model.ProcDecl;
import io.kroxylicious.doxylicious.model.Unit;

public class Base {

    private static final YAMLMapper YAML_MAPPER = new YAMLMapper();

    private final Map<String, ProcDecl> byName;
    private final Map<String, Unit> unitByName;
    private final Map<String, Set<String>> satisfiersByName;

    private Base(Map<String, ProcDecl> byName, Map<String, Unit> unitByName, Map<String, Set<String>> satisfiersByName) {
        this.byName = byName;
        this.unitByName = unitByName;
        this.satisfiersByName = satisfiersByName;
    }

    public static Base fromDirectories(List<Path> dirs) {
        var units = dirs.stream()
                .filter(Files::exists)
                .flatMap(dir -> {
                    try {
                        return Files.walk(dir);
                    }
                    catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                })
                .filter(Files::isRegularFile)
                .filter(path -> path.getFileName().toString().endsWith(".yaml"))
                .map(path -> {
                    try {
                        JsonParser parser = YAML_MAPPER.createParser(path.toFile());
                        var procList = YAML_MAPPER.readValues(parser, new TypeReference<ProcDecl>() {
                        }).readAll();
                        return new Unit(path.toString(), procList);
                    }
                    catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                }).toList();

        return Base.fromUnits(units);
    }

    public static Base from(List<ProcDecl> procs) {
        return from("UNKNOWN", procs);
    }

    public static Base from(String source, List<ProcDecl> procs) {
        return fromUnits(List.of(new Unit(source, procs)));
    }

    public static Base fromUnits(List<Unit> units) {
        Map<String, String> procNameToPath = new HashMap<>();
        var byName = units.stream()
                .flatMap(unit -> {
                    unit.procs().forEach(proc -> {
                        var old = procNameToPath.put(proc.id(), unit.sourceName());
                        if (old != null) {
                            throw new ProcCheckException("proc '" + proc.id() + "' is declared in " + old + " and also " + unit.sourceName());
                        }
                    });
                    return unit.procs().stream();
                })
                .collect(Collectors.toMap(ProcDecl::id, Function.identity()));

        var unitByName = units.stream()
                .flatMap(unit -> unit.procs().stream().map(proc -> Map.entry(proc.id(), unit)))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        byName.forEach((k, v) -> {
            // Each named reference (satisfied, prereqs) is defined
            v.prereqs().forEach(prereq -> {
                if (!byName.containsKey(prereq.ref())) {
                    throw new ProcCheckException(String.format("proc '%s' has unknown proc '%s' in its prereqs", k, prereq.ref()));
                }
            });
            if (v.satisfies() != null) {
                if (!byName.containsKey(v.satisfies())) {
                    throw new ProcCheckException(String.format("proc '%s' has unknown proc '%s' in its satisfies", k, v.satisfies()));
                }
            }

            // notional mutex with satisfies?
            if (v.notional() && !v.procedure().isEmpty()) {
                throw new ProcCheckException(String.format("proc '%s' is notional, but has steps", k));
            }
        });

        // no cycles
        byName.forEach((k, v) -> {
            Set<Rel> seen = new LinkedHashSet<>();
            checkAcyclic(new Rel("", v.id()), seen, byName);
        });

        var satisfiers = new HashMap<String, Set<String>>();
        byName.values().forEach(v -> computeSatisfiersRecursive(v, byName, satisfiers));

        return new Base(byName, unitByName, satisfiers);
    }

    private static void computeSatisfiersRecursive(ProcDecl v, Map<String, ProcDecl> byName, HashMap<String, Set<String>> satisfiers) {
        if (v.satisfies() != null) {
            var s = satisfiers.computeIfAbsent(v.satisfies(), k -> new HashSet<>());
            if (!satisfiers.containsKey(v.satisfies())) {
                computeSatisfiersRecursive(byName.get(v.id()), byName, satisfiers);
                s.addAll(satisfiers.get(v.satisfies()));
            }
            s.add(v.id());
        }
    }

    private static void checkAcyclic(Rel relation, Set<Rel> seen, Map<String, ProcDecl> byName) {
        seen.add(relation);
        var procDecl = byName.get(relation.proc);
        if (procDecl.satisfies() != null) {
            var satisfiedDecl = byName.get(procDecl.satisfies());
            var newRelation = new Rel("satisfies", satisfiedDecl.id());
            if (seen.contains(newRelation)) {
                throwCyclicDefinition(procDecl, seen, newRelation, satisfiedDecl);
            }
            checkAcyclic(newRelation, seen, byName);
        }
        for (var prepeq : procDecl.prereqs()) {
            var prereqDecl = byName.get(prepeq.ref());
            var newRelation = new Rel("prereq", prereqDecl.id());
            if (seen.contains(newRelation)) {
                throwCyclicDefinition(procDecl, seen, newRelation, prereqDecl);
            }
            checkAcyclic(newRelation, seen, byName);
        }
        seen.remove(relation);
    }

    private static void throwCyclicDefinition(ProcDecl v, Set<Rel> s, Rel rel, ProcDecl finalX) {
        var via = Stream.concat(s.stream(), Stream.of(rel))
                .dropWhile(f -> !f.proc().equals(finalX.id()))
                .flatMap(r -> r.relation.isEmpty() ? Stream.of(r.proc) : Stream.of(r.relation, r.proc))
                .collect(Collectors.joining("->"));
        throw new ProcCheckException(String.format("proc '%s' has a cyclic definition: %s", v.id(), via));
    }

    /**
     * @param procName
     * @return the set of proc which are declared to directly satisfy the given proc.
     */
    public Set<String> directSatisfiers(String procName) {
        return satisfiersByName.getOrDefault(procName, Set.of());
    }

    /**
     * @param proc
     * @return the set of concrete procs which transitively satisfy the given proc.
     */
    public Set<String> transitiveSatisfiers(String proc) {
        Set<String> result = new HashSet<>();
        for (var s : directSatisfiers(proc)) {
            if (byName.get(s).notional()) {
                result.addAll(transitiveSatisfiers(s));
            }
            else {
                result.add(s);
            }
        }
        return result;
    }

    public @Nullable Unit unit(String name) {
        return unitByName.get(name);
    }

    public Collection<Unit> allUnits() {
        return new HashSet<>(unitByName.values());
    }

    public @Nullable ProcDecl procDecl(String name) {
        return byName.get(name);
    }

    public List<ProcDecl> allProcDecls() {
        return byName.values().stream().toList();
    }

    record Rel(String relation, String proc) {
        @Override
        public boolean equals(Object o) {
            if (!(o instanceof Rel rel)) {
                return false;
            }
            return Objects.equals(proc, rel.proc);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(proc);
        }
    }

}
