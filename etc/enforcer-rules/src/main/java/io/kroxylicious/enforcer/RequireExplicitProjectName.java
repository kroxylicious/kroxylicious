/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.enforcer;

import javax.inject.Inject;
import javax.inject.Named;

import org.apache.maven.enforcer.rule.api.AbstractEnforcerRule;
import org.apache.maven.enforcer.rule.api.EnforcerRuleException;
import org.apache.maven.model.Model;
import org.apache.maven.project.MavenProject;

@Named("requireExplicitProjectName")
public class RequireExplicitProjectName extends AbstractEnforcerRule {

    @Inject
    private MavenProject project;

    public void execute() throws EnforcerRuleException {
        Model originalModel = project.getOriginalModel();
        if (originalModel.getName() == null) {
            throw new EnforcerRuleException("Project name is not explicitly specified, please add a <name> element to the pom.xml.");
        }
    }

}
