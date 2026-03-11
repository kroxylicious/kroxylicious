/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.audit.emitter.slf4j;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeSet;
import java.util.stream.Collectors;

import io.kroxylicious.proxy.audit.AuditEmitter;
import io.kroxylicious.proxy.audit.AuditEmitterFactory;
import io.kroxylicious.proxy.plugin.Plugin;

import edu.umd.cs.findbugs.annotations.Nullable;

@Plugin(configType = SlfEmitterConfig.class)
public class Slf4jEmitterFactory implements AuditEmitterFactory<SlfEmitterConfig> {

    @Override
    public AuditEmitter create(@Nullable SlfEmitterConfig configuration) {
        if (configuration == null) {
            configuration = new SlfEmitterConfig(Level.INFO, null);
        }
        var grouped = configuration.except().stream().collect(Collectors.groupingBy(LevelExceptionConfig::action));
        var duplicateActions = grouped.entrySet().stream().filter(e -> e.getValue().size() > 1).map(Map.Entry::getKey).collect(Collectors.toCollection(TreeSet::new));
        if (!duplicateActions.isEmpty()) {
            throw new IllegalArgumentException("Duplicate actions found in the `except` property of the Slf4JEmitter: " + duplicateActions);
        }
        var map = grouped.entrySet().stream()
                .flatMap(e -> {
                    String action = e.getKey();
                    LevelExceptionConfig exceptionConfig = e.getValue().get(0);

                    Map<ActionMatch, Level> map1 = new HashMap<>();
                    if (exceptionConfig.logAt() != null) {
                        map1.put(new ActionMatch(action, true), exceptionConfig.logAt());
                        map1.put(new ActionMatch(action, false), exceptionConfig.logAt());
                    }
                    if (exceptionConfig.logSuccessAt() != null) {
                        map1.put(new ActionMatch(action, true), exceptionConfig.logSuccessAt());
                    }
                    if (exceptionConfig.logFailureAt() != null) {
                        map1.put(new ActionMatch(action, false), exceptionConfig.logFailureAt());
                    }
                    return map1.entrySet().stream();
                })
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        return new Slf4jEmitter(configuration.logAt(), map);
    }
}
