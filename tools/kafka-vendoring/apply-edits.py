#!/usr/bin/env python3
#
# Copyright Kroxylicious Authors.
#
# Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#

"""Apply declarative content-based edits (edits.yaml) to freshly copied Kafka files.

Generic engine: all file-specific knowledge lives in edits.yaml, not here. See edits.yaml for the
schema and the "what is cut and why" rationale.

Usage: apply-edits.py <copy-root> <edits.yaml>
"""
import sys, re, os
import yaml
from pathlib import Path

def strip_imports(text, patterns):
    out=[]
    for line in text.splitlines(keepends=True):
        if any(re.search(p, line) for p in patterns) and line.lstrip().startswith('import'):
            continue
        out.append(line)
    return ''.join(out)

def _matching_brace(text, open_idx):
    depth=0
    i=open_idx
    while i < len(text):
        c=text[i]
        if c=='{': depth+=1
        elif c=='}':
            depth-=1
            if depth==0: return i
        i+=1
    raise SystemExit("unbalanced braces from %d" % open_idx)

def _member_end(text, match_start, match_end):
    """From the end of a matched signature/statement start, find where it ends: a ';'
    or '}' once both paren and brace nesting return to the level they were at when the
    match ended. Tracking parens as well as braces matters for statements like
    `foo(bar, () -> { ... });` — the real terminator is the ';' *after* the call's
    closing ')', not the '}' closing the lambda body nested inside it."""
    paren=text.count('(', match_start, match_end)-text.count(')', match_start, match_end)
    brace=0
    i=match_end
    while i < len(text):
        c=text[i]
        if c=='(': paren+=1
        elif c==')': paren-=1
        elif c=='{': brace+=1
        elif c=='}':
            brace-=1
            if brace==0 and paren==0: return i+1
        elif c==';' and paren==0 and brace==0: return i+1
        i+=1
    raise SystemExit("unterminated member from %d" % match_end)

def _member_bounds(text, sig_regex, label):
    """Locate a class member or statement (field, method, nested type, or a single
    statement within a method body) by its signature, returning (cut, end): cut is the
    start of its leading javadoc/@annotation lines (or the member itself if neither),
    end is just past its terminating ';' or '}'."""
    m=re.search(sig_regex, text)
    if not m:
        raise SystemExit("SURGERY MISS: %s (%s) not found" % (label, sig_regex))
    start=m.start()
    # extend start backwards over an immediately-preceding javadoc/comment block and/or
    # annotation lines, so removeBlocks doesn't leave a dangling "/** ... */" behind
    # describing a method that's no longer there, and preserveBlocks keeps a kept member's
    # own doc comment instead of silently dropping it.
    line_start=text.rfind('\n', 0, start)+1
    lines_before=text[:line_start].rstrip('\n').split('\n')
    cut=line_start
    j=len(lines_before)-1
    while j>=0 and lines_before[j].strip().startswith('@'):
        cut-=len(lines_before[j])+1
        j-=1
    if j>=0 and lines_before[j].strip().endswith('*/'):
        while j>=0:
            stripped=lines_before[j].strip()
            cut-=len(lines_before[j])+1
            j-=1
            if stripped.startswith('/*'):
                break
    end=_member_end(text, m.start(), m.end())
    # swallow one trailing newline
    if end < len(text) and text[end]=='\n': end+=1
    return cut, end

def remove_block(text, sig_regex, label):
    """Remove a declaration whose header matches sig_regex, from any leading javadoc/
    @Override/annotation lines through its terminating ';' or brace-balanced body."""
    cut, end=_member_bounds(text, sig_regex, label)
    return text[:cut]+text[end:]

def _format_import(spec):
    if spec.startswith('static '):
        return 'import static %s;\n' % spec[len('static '):]
    return 'import %s;\n' % spec

def preserve_blocks(text, spec):
    """Keep ONLY the named imports and class members, dropping everything else in the
    file. The inverse of stripImports/removeBlocks: for a file where we want a small
    named handful of an otherwise large, unrelated class, listing what to keep is far
    less verbose (and self-correcting against upstream additions) than listing
    everything to remove."""
    pkg_m=re.search(r'^package [\w.]+;\n', text, re.MULTILINE)
    if not pkg_m:
        raise SystemExit("preserveBlocks: no package statement found")
    class_m=re.search(r'\bclass\s+(\w+)', text)
    if not class_m:
        raise SystemExit("preserveBlocks: no class declaration found")
    remaining=text
    members=[]
    for member in spec.get('members', []):
        cut, end=_member_bounds(remaining, member['signature'], member.get('label', member['signature']))
        members.append(remaining[cut:end].strip('\n'))
        remaining=remaining[:cut]+remaining[end:]
    header=text[:pkg_m.end()]
    imports=''.join(_format_import(i) for i in spec.get('imports', []))
    body='\n\n'.join(members)
    return "%s\n%spublic class %s {\n\n%s\n}\n" % (header, imports, class_m.group(1), body)

def qualify_nested_import(text, qualified_name):
    """Drop `import <qualified_name>;` and replace bare (unqualified) uses of its
    simple name with `Outer.Inner` everywhere in the file.

    Works around an OpenRewrite ChangePackage limitation: when a nested class's
    enclosing type ends up in the same package as the importing file after the
    package rewrite, ChangePackage treats the import as a redundant same-package
    import and strips it. That's correct for a top-level type (same-package types
    never need importing) but wrong for a nested one, which still needs either an
    import or full qualification to be referenced by its simple name — so run this
    first and let OpenRewrite strip nothing, rather than relying on an import that
    may or may not survive."""
    # The type path is every dot-separated segment from the first capitalized (class)
    # segment onward — e.g. for org.apache.kafka.common.record.internal.MemoryRecords.
    # RecordFilter.BatchRetention that's "MemoryRecords.RecordFilter.BatchRetention",
    # not just the last two segments, so doubly (or deeper) nested types qualify fully.
    parts=qualified_name.split('.')
    type_path_start=next(i for i, p in enumerate(parts) if p[:1].isupper())
    type_path='.'.join(parts[type_path_start:])
    simple_name=parts[-1]
    import_line_re=re.compile(r'^import %s;\n' % re.escape(qualified_name), re.MULTILINE)
    if not import_line_re.search(text):
        raise SystemExit("SURGERY MISS: qualifyNestedImports (%s) not found" % qualified_name)
    text=import_line_re.sub('', text)
    return re.sub(r'(?<!\.)\b%s\b' % re.escape(simple_name), type_path, text)

def apply_edit(root, entry):
    path = Path(root) / entry['file']

    try:
        text = path.read_text(encoding='utf-8')
        text = strip_imports(text, entry.get('stripImports', []))

        for qualified_name in entry.get('qualifyNestedImports', []):
            text = qualify_nested_import(text, qualified_name)

        for block in entry.get('removeBlocks', []):
            for _ in range(block.get('count', 1)):
                text = remove_block(text, block['signature'], block['label'])

        if 'preserveBlocks' in entry:
            text = preserve_blocks(text, entry['preserveBlocks'])

        path.write_text(text, encoding='utf-8')
        print(f"edited {entry['file']}")

    except (UnicodeDecodeError, OSError) as e:
        print(f"Skipping {entry['file']} due to error: {e}")

def main():
    root, edits_path = sys.argv[1], sys.argv[2]
    with open(edits_path, encoding='utf-8') as f:
        edits=yaml.safe_load(f)
    for entry in edits:
        apply_edit(root, entry)

if __name__=='__main__':
    main()
