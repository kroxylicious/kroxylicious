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

def remove_block(text, sig_regex, label):
    """Remove a declaration whose header matches sig_regex, from any leading
    @Override/annotation lines through its brace-balanced body."""
    m=re.search(sig_regex, text)
    if not m:
        raise SystemExit("SURGERY MISS: %s (%s) not found" % (label, sig_regex))
    start=m.start()
    # extend start backwards over immediately-preceding annotation lines + blank
    line_start=text.rfind('\n', 0, start)+1
    # walk up over @Override / annotation-only lines
    lines_before=text[:line_start].rstrip('\n').split('\n')
    cut=line_start
    j=len(lines_before)-1
    while j>=0 and lines_before[j].strip().startswith('@'):
        cut-=len(lines_before[j])+1
        j-=1
    open_idx=text.index('{', m.end()-1) if '{' in text[m.start():m.end()] else text.index('{', m.end())
    close_idx=_matching_brace(text, open_idx)
    # swallow one trailing newline
    end=close_idx+1
    if end < len(text) and text[end]=='\n': end+=1
    return text[:cut]+text[end:]

def apply_edit(root, entry):
    path=os.path.join(root, entry['file'])
    text=open(path, encoding='utf-8').read()
    text=strip_imports(text, entry.get('stripImports', []))
    for block in entry.get('removeBlocks', []):
        for _ in range(block.get('count', 1)):
            text=remove_block(text, block['signature'], block['label'])
    open(path, 'w', encoding='utf-8').write(text)
    print("edited "+entry['file'])

def main():
    root, edits_path = sys.argv[1], sys.argv[2]
    with open(edits_path, encoding='utf-8') as f:
        edits=yaml.safe_load(f)
    for entry in edits:
        apply_edit(root, entry)

if __name__=='__main__':
    main()
