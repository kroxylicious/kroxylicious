/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption.config;

/**
 * The encryption version corresponds directly to a configurable under the proxy administrator's control.
 * The proxy administrator uses this configurable to manage the proxy/filter upgrade process
 * The encryption version directly determines what information is persisted within the encrypted record, and how it is persisted,
 * though the versions of each binary blob used is determined separates (see {@link ParcelVersion}, {@link WrapperVersion}).
 * Other aspects of encryption (such as choice of cipher, or cryptograpihc algorithm provider) correspond to their own
 * admin-visible options.
 */
public enum EncryptionVersion {

    V1_UNSUPPORTED,
    V2;

}
