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

@Named("requireExplicitProjectDescription")
public class RequireExplicitProjectDescription extends AbstractEnforcerRule {

    @Inject
    private MavenProject project;

    public void execute() throws EnforcerRuleException {
        Model originalModel = project.getOriginalModel();
        if (originalModel.getDescription() == null) {
            throw new EnforcerRuleException("Project description is not explicitly specified, please add a <description> element to the pom.xml.");
        }
    }

}
