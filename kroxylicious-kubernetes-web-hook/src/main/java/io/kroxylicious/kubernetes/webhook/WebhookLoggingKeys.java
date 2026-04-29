/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.webhook;

public class WebhookLoggingKeys {

    static final String NAMESPACE = "namespace";

    static final String NAME = "name";

    static final String POD = "pod";

    static final String ANNOTATION = "annotation";

    static final String ANNOTATION_VALUE = "annotationValue";

    private WebhookLoggingKeys() {
    }

}
