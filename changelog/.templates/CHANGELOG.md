# CHANGELOG

This changelog enumerates **all user-facing** changes made to Kroxylicious, in reverse chronological order.
For changes that effect a public API, the [deprecation policy](./DEV_GUIDE.md#deprecation-policy) is followed.

Format `<github issue/pr number>: <short description>`.

{% set repo = "https://github.com/kroxylicious/kroxylicious" %}
{% set change_types = ["changed", "deprecated", "removed"] %}
{% for v in changelog.versions %}
## {{ "SNAPSHOT" if v.version.unreleased else v.version.value }}

{% for group in v.entriesGroups %}
{% if group.notEmpty and group.type.key not in change_types %}
{% for entry in group.entries %}
{% if entry.mergeRequests %}{% set link_prefix = "[#" ~ entry.mergeRequests[0].value ~ "](" ~ repo ~ "/pull/" ~ entry.mergeRequests[0].value ~ "): " %}{% elif entry.issues %}{% set link_prefix = "[#" ~ entry.issues[0] ~ "](" ~ repo ~ "/issues/" ~ entry.issues[0] ~ "): " %}{% else %}{% set link_prefix = "" %}{% endif %}
* {{ link_prefix }}{{ entry.title.value | trim }}
{% endfor %}
{% endif %}
{% endfor %}
{% set ns = namespace(has_changes=false) %}
{% for group in v.entriesGroups %}
{% if group.notEmpty and group.type.key in change_types %}
{% set ns.has_changes = true %}
{% endif %}
{% endfor %}
{% if ns.has_changes %}

### Changes, deprecations and removals

{% for group in v.entriesGroups %}
{% if group.notEmpty and group.type.key in change_types %}
{% for entry in group.entries %}
{% if entry.mergeRequests %}{% set link_prefix = "[#" ~ entry.mergeRequests[0].value ~ "](" ~ repo ~ "/pull/" ~ entry.mergeRequests[0].value ~ "): " %}{% elif entry.issues %}{% set link_prefix = "[#" ~ entry.issues[0] ~ "](" ~ repo ~ "/issues/" ~ entry.issues[0] ~ "): " %}{% else %}{% set link_prefix = "" %}{% endif %}
* {{ link_prefix }}{{ entry.title.value | trim }}
{% endfor %}
{% endif %}
{% endfor %}
{% endif %}

{% endfor %}
{% for archive in changelog.archives.archives %}{{ archive.lines | join("\n") }}{% endfor %}