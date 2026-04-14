---
paths:
  - "**/pom.xml"
---

# Java Dependency Change Requirements

## Open Source only

This is a strictly open source project.
This extends to our direct and transitive dependencies. 
We can only add a new dependency (or upgrade an existing dependency) when the dependency, and all its transitive dependencies, have an open source license. 
Open source license means a license that's approved but the OSI. 
We prefer non-copyleft open source licenses. 

## Minimise the number of dependencies

We want to avoid having a larger set of dependencies than is strictly necessary, because:
* Dependencies are a liability as much as an asset. 
* They can increase our support burden by requiring dependency upgrades.
* The can also increase our users' exposure to security threats.
* Even when a dependency's CVE is not expoitable in the proxy we have to fix it in practice because of false positives from security scanning software.

In practice the standard is not quite as high for dependencies which will not made it into the production code.

When there's a choice, our ordered preferences for production dependencies are:

1. Implement simple functionality in our own codebase, rather than using a dependency to provide it.
2. Prefer libraries that require no dependencies of their own.
3. Prefer libraries that are well-known and/or well-maintained projects. Regular releases are one signal of "well-maintained".
4. Prefer libraries that require few dependencies of their own.

Please consider these requirements if you're suggesting to add or change a dependency.
