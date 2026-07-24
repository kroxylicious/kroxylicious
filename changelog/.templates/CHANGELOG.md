# CHANGELOG

This changelog enumerates **all user-facing** changes made to Kroxylicious.
For changes that effect a public API, the [deprecation policy](./DEV_GUIDE.md#deprecation-policy) is followed.

Format `<github issue/pr number>: <short description>`.

{# repo_raw is injected by Maven resource filtering via the changelog.link.prefix pom.xml property.
   Falls back to the upstream URL if filtering has not been run (placeholder left unresolved). #}
{% set repo_raw = "${changelog.link.prefix}" %}
{% set repo = "https://github.com/kroxylicious/kroxylicious" if repo_raw.startsWith("${") else repo_raw %}
{% for v in changelog.versions %}
{% set ns_has_entries = namespace(value=false) %}
{% for group in v.entriesGroups %}{% if group.notEmpty %}{% set ns_has_entries.value = true %}{% endif %}{% endfor %}
{% if ns_has_entries.value %}
## {{ "SNAPSHOT" if v.version.unreleased else v.version.value }}

{% for group in v.entriesGroups %}
{% if group.notEmpty %}
{% for entry in group.entries %}
{% if entry.mergeRequests %}{% set link_prefix = "[#" ~ entry.mergeRequests[0].value ~ "](" ~ repo ~ "/pull/" ~ entry.mergeRequests[0].value ~ "): " %}{% elif entry.issues %}{% set link_prefix = "[#" ~ entry.issues[0] ~ "](" ~ repo ~ "/issues/" ~ entry.issues[0] ~ "): " %}{% else %}{% set link_prefix = "" %}{% endif %}
* {{ link_prefix }}{{ entry.title.value | trim }}
{% endfor %}
{% endif %}
{% endfor %}
{% set ns = namespace(has_notes=false) %}
{% for group in v.entriesGroups %}
{% if group.notEmpty %}
{% for entry in group.entries %}
{% if entry.importantNotes %}
{% set ns.has_notes = true %}
{% endif %}
{% endfor %}
{% endif %}
{% endfor %}
{% if ns.has_notes %}

### Changes, deprecations and removals

{% for group in v.entriesGroups %}
{% if group.notEmpty %}
{% for entry in group.entries %}
{% if entry.importantNotes %}
{% if entry.mergeRequests %}{% set link_prefix = "[#" ~ entry.mergeRequests[0].value ~ "](" ~ repo ~ "/pull/" ~ entry.mergeRequests[0].value ~ "): " %}{% elif entry.issues %}{% set link_prefix = "[#" ~ entry.issues[0] ~ "](" ~ repo ~ "/issues/" ~ entry.issues[0] ~ "): " %}{% else %}{% set link_prefix = "" %}{% endif %}
* {{ link_prefix }}{{ entry.importantNotes[0].value | trim | replace('\n', '\n  ') }}
{% for note in entry.importantNotes[1:] %}
  * {{ note.value | trim | replace('\n', '\n    ') }}
{% endfor %}
{% endif %}
{% endfor %}
{% endif %}
{% endfor %}
{% endif %}

{% endif %}
{% endfor %}
{% for archive in changelog.archives.archives %}{{ archive.lines | join("\n") }}{% endfor %}