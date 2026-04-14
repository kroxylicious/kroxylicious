---
paths:
  - "kroxylicious-docs/docs/**/*.adoc"
---

# Documentation Requirements

## When Documentation is Required

**New filter plugins:**
- Must include a complete Guide in `kroxylicious-docs`
- Must follow Red Hat modular documentation structure
- Must follow IBM documentation style guide

**Changes to core proxy:**
- User-visible changes require updates to "Proxy guide"
- Configuration changes require YAML examples

**Kubernetes changes:**
- CRD changes require updates to "Kroxylicious Operator for Kubernetes" guide
- New CRDs require complete documentation

**Plugin changes:**
- API changes require Javadoc updates
- Breaking changes require migration guide

## Filter Guide Requirements

Each filter guide must cover:

**What:** What the plugin does (what end-user problem does it solve?)

**When:** When the plugin should be used (and when not to use it)

**How:** Configuration examples covering:
- Configuring the proxy run directly on Linux
- Configuring the filter using the Kubernetes operator

**Gotchas:** Sharp edges or limitations

**Security:**
- If the plugin makes network connections, document TLS configuration
- Include op-sec best practices
- **IMPORTANT:** Show generating secure passwords, not hardcoded dummy values

    **Example:**
    ```yaml
    # ✅ Good - shows secure password generation
    password: $(openssl rand -base64 32)
    
    # ❌ Bad - hardcoded insecure value
    password: "changeme"
    ```

**Operations:**
- Document any metrics emitted
- Explain operational interpretation
- Troubleshooting guidance

## Documentation Structure

**Format:** AsciiDoc

**Structure:** Red Hat's modular documentation system

**Content types:**
- `ASSEMBLY` - Chapters grouping related content
- `PROCEDURE` - Step-by-step instructions
- `CONCEPT` - Explanatory content
- `REFERENCE` - Reference material
- `SNIPPET` - Reusable fragments

**Required elements:**
- Content type declaration at top
- Anchor ID: `[id='name-of-file_{context}']`
- Abstract paragraph with `[role="_abstract"]` tag

## Style

- Use US (global) English
- Follow IBM documentation style guide
- Be comprehensive

## Compiling the documentation

If you change the documentation you should always check that the documentation sources can still be 
converted to HTML and PDF, using `mvn -P dist package --pl kroxylicious-docs,kroxylicious-docs-tests`.
