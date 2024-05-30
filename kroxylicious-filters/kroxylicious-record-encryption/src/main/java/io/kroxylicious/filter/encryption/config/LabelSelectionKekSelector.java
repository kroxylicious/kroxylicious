/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption.config;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;

import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.metadata.ListTopicsRequest;
import io.kroxylicious.proxy.metadata.selector.Selector;
import io.kroxylicious.proxy.plugin.PluginConfigurationException;

import edu.umd.cs.findbugs.annotations.NonNull;

public class LabelSelectionKekSelector<K> extends TopicNameBasedKekSelector<K> {

    private final Map<Selector, K> selectorToKeyId;

    record SelectorKeyReferencePair<K>(Selector selector, K keyId) {}

    LabelSelectionKekSelector(List<SelectorKeyReferencePair<K>> keys) {
        for (int i = 0; i < keys.size(); i++) {
            var s1 = keys.get(i).selector();
            for (int j = 0; j < i; j++) {
                var s2 = keys.get(j).selector();
                if (!s1.disjoint(s2)) {
                    throw new PluginConfigurationException("Selectors are not disjoint: " + s1 + " and " + s2);
                }
            }
        }
        this.selectorToKeyId = keys.stream().collect(Collectors.toMap(SelectorKeyReferencePair::selector, SelectorKeyReferencePair::keyId));
    }

    @NonNull
    @Override
    public CompletionStage<Map<String, K>> selectKek(@NonNull Set<String> topicNames, FilterContext context) {
        return context.sendMetadataRequest(new ListTopicsRequest(topicNames, selectorToKeyId.keySet())).thenApply(response -> {
            var result = new HashMap<String, K>(topicNames.size());
            response.forEach((selector, topicNames2) -> {
                K k = selectorToKeyId.get(selector);
                for (var name : topicNames2) {
                    result.put(name, k);
                }
            });
            return result;
        });
    }
}
