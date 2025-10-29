/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.enforcer;

import org.apache.maven.enforcer.rule.api.AbstractEnforcerRule;
import org.apache.maven.enforcer.rule.api.EnforcerRuleException;
import org.apache.maven.model.Model;
import org.apache.maven.project.MavenProject;

import javax.inject.Inject;
import javax.inject.Named;

@Named("requireSonatypeCentralMetadata")
public class RequireSonatypeCentralMetadata extends AbstractEnforcerRule {

    @Inject
    private MavenProject project;

    public void execute() throws EnforcerRuleException {
        Model originalModel = project.getOriginalModel();
        if (originalModel.getName() == null) {
            throw missingPropertyException("name");
        }
        if (originalModel.getDescription() == null) {
            throw missingPropertyException("description");
        }
    }

    private static EnforcerRuleException missingPropertyException(String property) {
        return new EnforcerRuleException("Project " + property + " is not explicitly specified, please add a <"
                + property + "> element to the pom.xml.");
    }

}
