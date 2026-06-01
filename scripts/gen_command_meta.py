#!/usr/bin/env python3
# Copyright (C) 2011-2026 Redis Labs Ltd.
#
# Generate command_meta_data.h from a directory of Redis commands.json files.
#
# Reads every *.json file in --in, parses its single top-level entry (the
# command name maps to its metadata), and emits a single header containing
# static initializers usable from command_meta.cpp.
#
# A command file may describe a subcommand: the JSON entry then carries
# "container": "XGROUP" and the canonical name we emit is "XGROUP CREATE".

import argparse
import json
import sys
from pathlib import Path

# Reply-shape overrides for the well-known set. Keys are canonical uppercase
# names (subcommands written as "XGROUP CREATE"). When a name appears here we
# trust this table over reply_schema inference, since the JSON schemas are
# inconsistent across Redis versions.
SHAPE_OVERRIDES = {
    'GET': 'SingleNullBulk',
    'GETEX': 'SingleNullBulk',
    'GETDEL': 'SingleNullBulk',
    'HGET': 'SingleNullBulk',
    'ZSCORE': 'SingleNullBulk',
    'LPOP': 'SingleNullBulk',
    'RPOP': 'SingleNullBulk',
    'SPOP': 'SingleNullBulk',
    'RANDOMKEY': 'SingleNullBulk',
    'LINDEX': 'SingleNullBulk',
    'OBJECT ENCODING': 'SingleNullBulk',
    'MGET': 'ArrayPerElementNulls',
    'HMGET': 'ArrayPerElementNulls',
    'ZMSCORE': 'ArrayPerElementNulls',
    'SMEMBERS': 'EmptyCollection',
    'LRANGE': 'EmptyCollection',
    'HGETALL': 'EmptyCollection',
    'HKEYS': 'EmptyCollection',
    'HVALS': 'EmptyCollection',
    'ZRANGE': 'EmptyCollection',
    'EXISTS': 'IntegerMembership',
    'SISMEMBER': 'IntegerMembership',
    'HEXISTS': 'IntegerMembership',
}


def infer_shape(name, doc):
    if name in SHAPE_OVERRIDES:
        return SHAPE_OVERRIDES[name]
    schema = doc.get('reply_schema')
    if schema is None:
        return 'Unknown'
    if isinstance(schema, dict) and schema.get('type') == 'array':
        items = schema.get('items')
        if isinstance(items, dict) and 'oneOf' in items:
            if any(o.get('type') == 'null' for o in items['oneOf'] if isinstance(o, dict)):
                return 'ArrayPerElementNulls'
    if isinstance(schema, dict) and 'oneOf' in schema:
        if any(o.get('type') == 'null' for o in schema['oneOf'] if isinstance(o, dict)):
            return 'SingleNullBulk'
    return 'NotMissable'


def parse_keyspec(spec):
    begin = spec.get('begin_search', {}) or {}
    find = spec.get('find_keys', {}) or {}

    out = {
        'begin_type': 'Unknown', 'begin_pos': 0,
        'begin_keyword': 'nullptr', 'begin_startfrom': 0,
        'find_type': 'Unknown',
        'lastkey': 0, 'step': 0, 'limit': 0,
        'keynumidx': 0, 'firstkey': 0, 'keynum_step': 0,
    }

    if 'index' in begin:
        out['begin_type'] = 'Index'
        out['begin_pos'] = int(begin['index'].get('pos', 0))
    elif 'keyword' in begin:
        out['begin_type'] = 'Keyword'
        kw = begin['keyword']
        out['begin_keyword'] = c_str_literal(kw.get('keyword', ''))
        out['begin_startfrom'] = int(kw.get('startfrom', 0))

    if 'range' in find:
        out['find_type'] = 'Range'
        rng = find['range']
        out['lastkey'] = int(rng.get('lastkey', 0))
        out['step'] = int(rng.get('step', 1))
        out['limit'] = int(rng.get('limit', 0))
    elif 'keynum' in find:
        out['find_type'] = 'Keynum'
        kn = find['keynum']
        out['keynumidx'] = int(kn.get('keynumidx', 0))
        out['firstkey'] = int(kn.get('firstkey', 0))
        out['keynum_step'] = int(kn.get('step', 1))

    return out


def c_str_literal(s):
    if s is None:
        return 'nullptr'
    s = s.replace('\\', '\\\\').replace('"', '\\"')
    return '"{}"'.format(s)


def c_identifier(name):
    return name.replace(' ', '_').replace('-', '_').replace('|', '_')


def load_commands(input_dir):
    for path in sorted(Path(input_dir).glob('*.json')):
        try:
            with open(path) as f:
                data = json.load(f)
        except json.JSONDecodeError as e:
            print('WARN: skipping {}: {}'.format(path.name, e), file=sys.stderr)
            continue
        if not isinstance(data, dict) or len(data) != 1:
            print('WARN: skipping {}: expected single top-level key'.format(path.name), file=sys.stderr)
            continue
        subcmd_name, doc = next(iter(data.items()))
        container = doc.get('container')
        if container:
            canonical = '{} {}'.format(container.upper(), subcmd_name.upper())
        else:
            canonical = subcmd_name.upper()
        yield canonical, doc


def main():
    ap = argparse.ArgumentParser(description='Generate command_meta_data.h from commands JSON.')
    ap.add_argument('--in', dest='input_dir', required=True)
    ap.add_argument('--out', dest='output_path', required=True)
    args = ap.parse_args()

    tag_path = Path(args.input_dir) / 'REDIS_TAG.txt'
    provenance = '<unknown>'
    if tag_path.exists():
        provenance = '; '.join(line.strip() for line in tag_path.read_text().splitlines() if line.strip())

    out = []
    out.append('// Auto-generated by scripts/gen_command_meta.py - do not edit.')
    out.append('// Source: redis/redis ({})'.format(provenance))
    out.append('#pragma once')
    out.append('')
    out.append('#include "command_meta.h"')
    out.append('')
    out.append('namespace memtier {')
    out.append('namespace command_meta {')
    out.append('')

    spec_blocks = []
    cmd_rows = []
    n_with_specs = 0
    shape_counts = {}

    for name, doc in load_commands(args.input_dir):
        ident = c_identifier(name)
        key_specs = doc.get('key_specs', []) or []
        arity = int(doc.get('arity', 0))
        flags = [f.upper() for f in (doc.get('command_flags', []) or [])]
        movable = 'MOVABLEKEYS' in flags
        shape = infer_shape(name, doc)
        shape_counts[shape] = shape_counts.get(shape, 0) + 1

        if key_specs:
            n_with_specs += 1
            spec_blocks.append('static const KeySpec kKeySpecs_{}[{}] = {{'.format(ident, len(key_specs)))
            for spec in key_specs:
                p = parse_keyspec(spec)
                spec_blocks.append(
                    '    {{ {{BeginSearchType::{}, {}, {}, {}}}, '
                    '{{FindKeysType::{}, {}, {}, {}, {}, {}, {}}} }},'.format(
                        p['begin_type'], p['begin_pos'], p['begin_keyword'], p['begin_startfrom'],
                        p['find_type'], p['lastkey'], p['step'], p['limit'],
                        p['keynumidx'], p['firstkey'], p['keynum_step'],
                    ))
            spec_blocks.append('};')
            spec_blocks.append('')
            specs_ref = 'kKeySpecs_{}'.format(ident)
        else:
            specs_ref = 'nullptr'

        cmd_rows.append('    {{ {}, {}, {}, {}, {}, ReplyShape::{} }},'.format(
            c_str_literal(name), arity,
            'true' if movable else 'false',
            len(key_specs), specs_ref, shape,
        ))

    out.extend(spec_blocks)
    out.append('static const CommandSpec kCommands[] = {')
    out.extend(cmd_rows)
    out.append('};')
    out.append('static constexpr size_t kCommandsCount = sizeof(kCommands) / sizeof(kCommands[0]);')
    out.append('')
    out.append('}  // namespace command_meta')
    out.append('}  // namespace memtier')
    out.append('')

    Path(args.output_path).write_text('\n'.join(out))
    print('[gen_command_meta] wrote {} ({} commands; {} with key_specs)'.format(
        args.output_path, len(cmd_rows), n_with_specs))
    print('[gen_command_meta] reply_shape distribution:')
    for shape in sorted(shape_counts):
        print('  {:24s} {}'.format(shape, shape_counts[shape]))


if __name__ == '__main__':
    main()
