[KroxyliciousDoc]: https://kroxylicious.io/ "Kroxylicious documentation"
[AsciiDoc]: https://docs.asciidoctor.org/asciidoc/latest/syntax-quick-reference/  "AsciiDoc reference"

<!-- omit from toc -->
# Kroxylicious documentation

Welcome to the Kroxylicious documentation! 

This folder contains the source files for managing the Kroxylicious documentation. 
The documentation is written in [AsciiDoc][AsciiDoc] and provides content to help you understand and set up Kroxylicious.
US (global) English is used throughout.

<!-- omit from toc -->
## Table of Contents
- [Kroxylicious guide](#kroxylicious-guide)
- [Documentation folder structure](#documentation-folder-structure)
- [Generating the guide](#generating-the-guide)
- [Contributing to the documentation](#contributing-to-the-documentation)

## Kroxylicious guide

The Kroxylicious documentation is organized into specific **Kroxylicious** guides.

The content for the guide to the proxy is encapsulated in the index file:

- [kroxylicious-proxy/index.adoc](kroxylicious-proxy/index.adoc)

The content for the developer's guide is encapsulated in the index file:

- [developers-guide/index.adoc](kroxylicious-proxy/index.adoc) 

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

## Generating the guide

To generate the guide in HTML, run the following Maven command from the project root directory (the parent directory of the `docs` directory).

```shell
mvn org.asciidoctor:asciidoctor-maven-plugin:process-asciidoc@convert-kroxylicious-proxy-to-html
```

The HTML is output to `target/kroxylicious-proxy/html/index.html`. 

## Contributing to the documentation

If there's something that you want to add or change in the documentation, do the following:

1. Fork the repository
1. Set up a local Git repository by cloning the forked repository
2. Create a branch for your changes
3. Add the changes through a pull request

The pull request will be reviewed and the changes merged when the review is complete.
The guide is then rebuilt and the updated content is published on the Kroxylicious website.
Published documentation for the current _main_ branch as well as all releases can be found on our [website][KroxyliciousDoc].