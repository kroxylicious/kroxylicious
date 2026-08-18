#!/usr/bin/env python3
"""Content-based surgery for vendored Kafka support classes.

Removes server-side/config/file edges that would otherwise drag org.apache.kafka
packages we deliberately do not vendor into the copy-closure. Matches by content
(signature regex + brace balancing), so it survives line-number drift between
Kafka releases; if a target can no longer be found it fails loudly, which is the
correct signal for a human to re-review the surgery against the new source.

Usage: surgery.py <file> (dispatches on basename)
"""
import sys, re, os

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
    prefix_end=line_start
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

def process(path):
    base=os.path.basename(path)
    text=open(path, encoding='utf-8').read()
    if base=='CompressionType.java':
        text=strip_imports(text, [r'\.config\.ConfigDef', r'\.config\.ConfigException',
                                   r'\.config\.ConfigDef\.Range\.between'])
        # 3 enum-constant overrides + the base declaration
        for _ in range(4):
            text=remove_block(text, r'public\s+ConfigDef\.Validator\s+levelValidator\s*\(\s*\)',
                              'CompressionType.levelValidator')
    elif base=='Utils.java':
        # Only the config-machinery methods pull org.apache.kafka.common.config into the
        # closure. The nio.file methods are pure JDK (kept); TransferableChannel/tryWriteTo
        # are genuine records API (kept, TransferableChannel is copied faithfully).
        text=strip_imports(text, [r'\.config\.ConfigDef', r'\.config\.ConfigException'])
        for sig,label in [
            (r'public static Map<String, Object> propsToMap\(', 'Utils.propsToMap'),
            (r'public static Map<String, Object> castToStringObjectMap\(', 'Utils.castToStringObjectMap'),
            (r'public static void ensureConcreteSubclass\(', 'Utils.ensureConcreteSubclass'),
            (r'public static ConfigDef mergeConfigs\(', 'Utils.mergeConfigs'),
        ]:
            text=remove_block(text, sig, label)
    elif base=='DefaultRecordBatch.java':
        text=strip_imports(text, [r'record\.internal\.FileLogInputStream', r'record\.internal\.FileRecords'])
        text=remove_block(text, r'static class DefaultFileChannelRecordBatch extends FileLogInputStream',
                          'DefaultRecordBatch.DefaultFileChannelRecordBatch')
    elif base=='AbstractLegacyRecordBatch.java':
        text=strip_imports(text, [r'record\.internal\.FileLogInputStream', r'record\.internal\.FileRecords'])
        text=remove_block(text, r'static class LegacyFileChannelRecordBatch extends FileLogInputStream',
                          'AbstractLegacyRecordBatch.LegacyFileChannelRecordBatch')
    else:
        raise SystemExit("no surgery defined for "+base)
    open(path, 'w', encoding='utf-8').write(text)
    print("surgered "+base)

if __name__=='__main__':
    process(sys.argv[1])
