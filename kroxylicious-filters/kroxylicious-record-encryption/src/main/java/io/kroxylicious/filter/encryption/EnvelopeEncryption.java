/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption;

import io.kroxylicious.filter.encryption.config.RecordEncryptionConfig;
import io.kroxylicious.proxy.filter.FilterFactory;
import io.kroxylicious.proxy.plugin.Plugin;

/**
 * @deprecated Replaced with {@link io.kroxylicious.filter.encryption.RecordEncryption}
 * A {@link FilterFactory} for {@link RecordEncryptionFilter}.
 * @param <K> The key reference
 * @param <E> The type of encrypted DEK
 */
@Deprecated(since = "0.5.0", forRemoval = true)
@Plugin(configType = RecordEncryptionConfig.class)
public class EnvelopeEncryption<K, E> extends RecordEncryption<K, E> implements FilterFactory<RecordEncryptionConfig, SharedEncryptionContext<K, E>> {

}
