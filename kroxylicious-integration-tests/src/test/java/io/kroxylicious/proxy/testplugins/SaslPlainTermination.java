/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.testplugins;

import java.util.List;
import java.util.Map;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.AppConfigurationEntry;

import org.apache.kafka.common.security.auth.AuthenticateCallbackHandler;
import org.apache.kafka.common.security.plain.PlainAuthenticateCallback;
import org.apache.kafka.common.utils.Utils;

import io.kroxylicious.proxy.filter.Filter;
import io.kroxylicious.proxy.filter.FilterFactory;
import io.kroxylicious.proxy.filter.FilterFactoryContext;
import io.kroxylicious.proxy.plugin.Plugin;
import io.kroxylicious.proxy.plugin.PluginConfigurationException;
import io.kroxylicious.proxy.plugin.Plugins;

import edu.umd.cs.findbugs.annotations.Nullable;

@Plugin(configType = SaslPlainTerminationConfig.class)
public class SaslPlainTermination
        implements FilterFactory<SaslPlainTerminationConfig, SaslPlainTermination.PasswordVerifier> {

    interface PasswordVerifier extends AuthenticateCallbackHandler {
    }

    /**
     * This password verifier is insecure in the sense that the user credentials are stored (and configured!) in plaintext.
     */
    static class InsecurePasswordVerifier implements PasswordVerifier {
        private final Map<String, String> userNameToPassword;

        InsecurePasswordVerifier(Map<String, String> userNameToPassword) {
            this.userNameToPassword = userNameToPassword;
        }

        @Override
        public void configure(Map<String, ?> configs, String saslMechanism, List<AppConfigurationEntry> jaasConfigEntries) {
            if (!"PLAIN".equals(saslMechanism)) {
                throw new IllegalStateException("This verifier only supports PLAIN authentication");
            }
        }

        @Override
        public void close() {

        }

        @Override
        public void handle(Callback[] callbacks) throws UnsupportedCallbackException {
            String username = null;
            for (Callback callback : callbacks) {
                if (callback instanceof NameCallback) {
                    username = ((NameCallback) callback).getDefaultName();
                }
                else if (callback instanceof PlainAuthenticateCallback) {
                    PlainAuthenticateCallback plainCallback = (PlainAuthenticateCallback) callback;
                    boolean authenticated = authenticate(username, plainCallback.password());
                    plainCallback.authenticated(authenticated);
                }
                else {
                    throw new UnsupportedCallbackException(callback);
                }
            }
        }

        protected boolean authenticate(@Nullable String username, char[] password) {
            if (username == null) {
                return false;
            }
            else {
                String expectedPassword = userNameToPassword.get(username);
                return expectedPassword != null && Utils.isEqualConstantTime(password, expectedPassword.toCharArray());
            }
        }
    }

    @Override
    public PasswordVerifier initialize(FilterFactoryContext context, SaslPlainTerminationConfig config) throws PluginConfigurationException {
        Plugins.requireConfig(this, config);
        return new InsecurePasswordVerifier(config.userNameToPassword());
    }

    @Override
    public Filter createFilter(FilterFactoryContext context, PasswordVerifier passwordVerifier) {
        return new SaslPlainTerminationFilter(passwordVerifier);
    }
}
