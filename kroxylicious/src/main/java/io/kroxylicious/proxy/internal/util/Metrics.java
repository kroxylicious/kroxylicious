/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.util;

import io.micrometer.core.instrument.Tag;

public class Metrics {

    // creating a constant for all Metrics in the one place so we can easily see what metrics there are

    public static final String KROXYLICIOUS_INBOUND_DOWNSTREAM_MESSAGES = "kroxylicious_inbound_downstream_messages";

    public static final String KROXYLICIOUS_INBOUND_DOWNSTREAM_DECODED_MESSAGES = "kroxylicious_inbound_downstream_decoded_messages";

    public static final String KROXYLICIOUS_REQUEST_SIZE_BYTES = "kroxylicious_request_size_bytes";

    public static final String FLOWING_TAG = "flowing";

    public static final Tag FLOWING_UPSTREAM = Tag.of(FLOWING_TAG, "upstream");

    public static final Tag FLOWING_DOWNSTREAM = Tag.of(FLOWING_TAG, "downstream");

}
