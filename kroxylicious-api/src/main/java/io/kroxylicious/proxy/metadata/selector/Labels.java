/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.metadata.selector;

import java.util.Map;
import java.util.Objects;

public class Labels {
    private Labels() {

    }

    /**
     * Validate that keys and values in the given {@code labels} conform to the syntax Kubernetes would expect.
     * @param labels The labels to validate
     * @return The given labels.
     */
    public static Map<String, String> validate(Map<String, String> labels) {
        labels.forEach((key, value) -> {
            validateLabelKey(key);
            validateLabelValue(value);
        });
        return labels;
    }

    static void validateLabelKey(String label) {
        Selector.parserFor(Objects.requireNonNull(label)).justLabel();
        int index = label.indexOf('/');
        if (index >= 0) {
            String prefix = label.substring(0, index);
            validateLabelPrefix(prefix);
            String name = label.substring(index + 1);
            validateLabelName(name);
        }
        else {
            validateLabelName(label);
        }
    }

    static void validateLabelPrefix(String dnsSubdomain) {
        if (dnsSubdomain.length() > 253) {
            throw new LabelException("DNS subdomain in prefix is too long: must be 253 characters or fewer");
        }
    }

    static void validateLabelName(String name) {
        if (name.length() > 63) {
            throw new LabelException("name in prefix is too long: must be 63 characters or fewer");
        }
    }

    static void validateLabelValue(String value) {
        Selector.parserFor(value).justValue();
        if (value.length() > 63) {
            throw new LabelException("value is too long: must be 63 characters or fewer");
        }
    }
}
