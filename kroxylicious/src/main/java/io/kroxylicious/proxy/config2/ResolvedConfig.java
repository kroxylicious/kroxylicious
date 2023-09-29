/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config2;

import java.util.List;

public record ResolvedConfig(List<BoundFilter<?, ?>> filters, boolean useIoUring) {
}
