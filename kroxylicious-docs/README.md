[KroxyliciousDoc]: https://kroxylicious.io/ "Kroxylicious documentation"
[AsciiDoc]: https://docs.asciidoctor.org/asciidoc/latest/syntax-quick-reference/  "AsciiDoc reference"

<!-- omit from toc -->
# Kroxylicious documentation

This folder contains the source files for managing the Kroxylicious documentation. 
The documentation is written in [AsciiDoc][AsciiDoc] and provides content to help you understand and set up Kroxylicious.
US (global) English is used throughout.

<!-- omit from toc -->
## Table of Contents
- [Kroxylicious guides](#kroxylicious-guides)
- [Documentation folder structure](#documentation-folder-structure)
- [Generating the guides](#generating-the-guides)
- [Contributing to the documentation](#contributing-to-the-documentation)

## Kroxylicious guides

The Kroxylicious documentation is organized into specific **Kroxylicious** guides.

The content for the guides are each encapsulated in the index files:

- [kroxylicious-proxy/index.adoc](docs/kroxylicious-proxy/index.adoc)
- [developer-guide/index.adoc](docs/kroxylicious-proxy/index.adoc)
- [operator-guide/index.adoc](operator-guide/index.adoc)
- [record-encryption-guide/index.adoc](docs/record-encryption-guide/index.adoc)

## Documentation folder structure

The index files are used to build each guide.
Documentation folders contain the content that's incorporated into the main source files.
An assembly is like a sub-section or chapter in a book.
A module contains a procedure (`proc-`), concepts (`con-`), or reference (`ref-`) content.

**Documentation folders**

| Folder                   | Description                                 |
|--------------------------|---------------------------------------------|
| `assemblies/`            | Assemblies (chapters) group related content |
| `modules/`               | Modules provide content for assemblies      |
| `_assets/`               | Content common to all doc files             |
| `shared/attributes.adoc` | Global book attributes                      |
| `kroxylicious-proxy/`    | The Kroxylicious Proxy guide                |

Each documentation file requires the following:

 - Content type
 - Anchor ID
 - Abstract tag

**Content Type** 

Defined at the top of the file:
- :_mod-docs-content-type: ASSEMBLY 		
- :_mod-docs-content-type: PROCEDURE 
- :_mod-docs-content-type: CONCEPT
- :_mod-docs-content-type: REFERENCE 
- :_mod-docs-content-type: SNIPPET

**Anchor ID**

Use the same name as the file (with dashes) plus a `_{context}` variable: `[id='name-of-file_{context}']`

The context variable is defined in the assembly of a guide, such as `:context: operator`. Context variables allow reuse of the same content. Anchor IDs allow cross-referencing. You can also add anchors to subheadings.

**Abstract**

Start each file with an introductory paragraph. Mark it by adding a `[role="_abstract"]` tag above it.

## Generating the guide

To generate the guides in HTML, run the following Maven command from the project root directory (the parent directory of the `kroxylicious-docs` directory).

```shell
mvn -P dist package --pl kroxylicious-docs 
```

The HTML for each guide is output to a subdirectory of `target/docs`. 

## Contributing to the documentation

If there's something that you want to add or change in the documentation, do the following:

1. Fork the repository
1. Set up a local Git repository by cloning the forked repository
2. Create a branch for your changes
3. Add the changes through a pull request

The pull request will be reviewed and the changes merged when the review is complete.
The guide is then rebuilt and the updated content is published on the Kroxylicious website.
Published documentation for the current _main_ branch as well as all releases can be found on our [website][KroxyliciousDoc].

## Documentation Standards

### What Each Guide Should Contain

We aim to have a separate "Guide" for each of the Filters the project provides. If a new plugin gets added, it should come with its own Guide. Such guides are aimed at the end user persona and should explain, at a minimum:

- What the plugin does (what end-user problem does it solve?)
- When the plugin should be used (and when it should not be used, if there are such circumstances)
- How it should be configured, including examples which cover both:
  - Configuring the proxy when run directly on Linux
  - Configuring the filter when run on Kubernetes and its derivatives, using the operator
- Any gotchas/sharp edges in using the plugin
- Any security aspects of using the plugin. For example, if the plugin makes network connections to other systems, the guide must cover configuring TLS for those connections. It should also include appropriate best-practices around op-sec. For example, if a password is used, the example should demonstrate generating a random password of suitable complexity, rather than show a hard-coded dummy value.
- Operational aspects of using the plugin. For example, if the plugin emits any metrics, these should be documented along with a description of what they represent, and their operational interpretation.

### When Documentation Needs Updates

- User-visible changes to the "core" of the proxy (e.g., the configuration of anything that is not a plugin) require changes in the "Proxy guide" and/or the "Kroxylicious Operator for Kubernetes"
- User-visible changes to Kubernetes-specific functionality (like changes to the CRDs) should be documented in "Kroxylicious Operator for Kubernetes"
- The documentation should be comprehensive

### Style Guide

The documentation is structured according to [Red Hat's modular documentation](https://redhat-documentation.github.io/modular-docs/) system. The documentation should follow the IBM documentation style guide.