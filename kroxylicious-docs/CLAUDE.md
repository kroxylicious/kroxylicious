
This is the end-user documentation and plugin developer documentation for Kroxylicious.

It is written using the `asciidoc` format.
The documentation is structured according to [Red Hat's modular documentation](https://redhat-documentation.github.io/modular-docs/) system.
The documentation should follow the IBM documentation style guide.

We aim to have a separate "Guide" for each of the `Filters` the project provides.
If a new plugin gets added it should come with its own Guide.
such guides are aimed at the end user persona and should explain, at a minimum:

* what the plugins does (what end-user problem does it solve?)
* when the plugin should be used (and when it should not be used, of there are such circumstances).
* how it should be configured, including examples which cover both:
    - configuring the proxy both when run directly on Linux
    - configuring the filter when run on Kubernetes and its derivatives, using the operator.
* any gotchas/sharp edges in using the plugin
* any security aspects of using the plugin. 
  For example if the plugin makes network connections to other systems the guide must cover configuring TLS for those connections. 
  It should also include approriate best-practices around op-sec. 
  For example, if a password is used the example should demonstrate generating a random password of suitable complexity, rather than show a hard-coded dummy value.
* operational aspects of using the plugin. 
  For example if the plugin emits any metrics these should be documented along with a description of what the represent, and their operational interpretation. 

User-visible changes to the "core" of the proxy (e.g. the configuration of anything that is not a plugin) require changes in the "Proxy guide" and/or the "Kroxylicious Operator for Kubernetes".

User-visible changes to Kubernetes-specific functionality (like changes to the CRDs) should be documented in "Kroxylicious Operator for Kubernetes".

The documentation should be comprehensive.
