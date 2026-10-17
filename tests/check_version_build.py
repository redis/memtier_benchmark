#!/usr/bin/env python3
"""Standalone CI check of version.h generation; does not require RLTest."""

import argparse
import os
from pathlib import Path
import shlex
import shutil
import subprocess
import tempfile


def run(command, cwd, env=None):
    return subprocess.run(command, cwd=cwd, env=env, check=True, text=True,
                          stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                          timeout=30).stdout


def check(build_dir):
    makefile = (build_dir / 'Makefile').resolve()
    script = Path(__file__).resolve().parents[1] / 'version.sh'
    with tempfile.TemporaryDirectory(prefix='memtier-version-') as temp:
        root = Path(temp)
        source = root / 'source'
        build = root / 'build'
        source.mkdir()
        build.mkdir()
        shutil.copyfile(script, source / 'version.sh')
        (source / 'tracked').write_text('original\n')
        run(['git', 'init', '-q'], source)
        run(['git', 'add', 'version.sh', 'tracked'], source)

        def commit(message):
            run(['git', '-c', 'user.name=Version regression test',
                 '-c', 'user.email=version-test@example.invalid',
                 'commit', '-q', '--allow-empty', '-m', message], source)
            return run(['git', 'rev-parse', '--short=8', 'HEAD'], source).strip()

        first = commit('first fixture revision')
        (build / 'probe.c').write_text(
            '#include <stdio.h>\n#include "version.h"\n'
            'int main(void) { puts(MEMTIER_GIT_SHA1 ":" MEMTIER_GIT_DIRTY); }\n')
        (build / 'probe.mk').write_text(
            'probe: probe.c version.h\n\t$(CC) -o $@ probe.c\n')

        def make(target='probe', src=source, env=None):
            # Use the actual configured production rule. Only the tiny consumer
            # is a fixture; no version dependency or recipe is duplicated here.
            return run(['make', '--no-print-directory', '-j4', '-f', str(makefile),
                        '-f', 'probe.mk', 'srcdir=' + str(src),
                        'top_srcdir=' + str(src), target], build, env=env)

        def expect(value):
            assert run([str(build / 'probe')], build).strip() == value

        def stamps():
            return tuple((build / f).stat().st_mtime_ns for f in ('version.h', 'probe'))

        make()
        expect(first + ':0')
        unchanged = stamps()
        make()
        assert stamps() == unchanged, 'unchanged metadata rebuilt its consumer'

        second = commit('advance HEAD without touching build inputs')
        make()
        expect(second + ':0')
        assert stamps() != unchanged

        (source / 'tracked').write_text('dirty\n')
        make()
        expect(second + ':1')
        unchanged = stamps()
        make()
        assert stamps() == unchanged, 'unchanged dirty metadata rebuilt its consumer'

        (source / 'tracked').write_text('original\n')
        run(['git', 'update-index', '--refresh'], source)
        make()
        expect(second + ':0')

        # Metadata errors warn and use explicitly unknown provenance, preserving
        # build compatibility without claiming a possibly stale SHA. Exercise
        # each Git failure; ordinary diff-index status 1 was covered above.
        fake_bin = root / 'fake-bin'
        fake_bin.mkdir()
        fake_git = fake_bin / 'git'
        real_git = shlex.quote(shutil.which('git'))
        refused_env = dict(os.environ, PATH=str(fake_bin) + os.pathsep + os.environ['PATH'])
        for operation in ('ls-files', 'rev-parse', 'diff-index'):
            fake_git.write_text('#!/bin/sh\n'
                                'if [ "$3" = "' + operation + '" ]; then exit 128; fi\n'
                                'exec ' + real_git + ' "$@"\n')
            fake_git.chmod(0o755)
            output = make(env=refused_env)
            assert 'warning: cannot read Git metadata' in output, output
            expect('00000000:0')
            unchanged = stamps()
            output = make(env=refused_env)
            assert 'warning: cannot read Git metadata' in output, output
            assert stamps() == unchanged, operation + ' failure repeatedly rebuilt artifacts'
            make()
            expect(second + ':0')

        # Archives can be unpacked within an unrelated repository. They must
        # report unknown provenance instead of that surrounding checkout's SHA.
        archive = source / 'archive'
        archive.mkdir()
        shutil.copyfile(script, archive / 'version.sh')
        make(src=archive)
        expect('00000000:0')
        unchanged = stamps()
        make(src=archive)
        assert stamps() == unchanged

        make(src=source)
        expect(second + ':0')
        print('PASS: first/direct build, HEAD advance, dirty/clean transitions, '
              'unchanged timestamps, metadata failures, VPATH source and archive provenance')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--build-dir', type=Path, default=Path.cwd())
    args = parser.parse_args()
    try:
        check(args.build_dir)
    except subprocess.CalledProcessError as error:
        print(error.stdout)
        raise
