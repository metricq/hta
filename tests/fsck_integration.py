"""Corrupt synthetic metrics, compare complete files against hta_repair using SHA-256."""
import hashlib
import bisect
import errno
import fcntl
import json
import os
import pathlib
import pty
import random
import re
import select
import shutil
import signal
import struct
import subprocess
import sys
import tempfile
import termios
import time
import unittest
import xml.etree.ElementTree as ET

FSCK, REPAIR, FIXTURE, FAULT = map(str, map(pathlib.Path.resolve, map(pathlib.Path, sys.argv[1:5])))
del sys.argv[1:5]
HEADER = 80
ROW = 56


def check_process(result, report=None):
    if result.returncode < 0:
        raise AssertionError(f'Process terminated by signal {-result.returncode}\n{result.stderr}')
    if report is not None:
        try:
            root = ET.parse(report).getroot()
        except (OSError, ET.ParseError) as error:
            raise AssertionError(f'Missing/incomplete Valgrind report: {report}') from error
        errors = [e for e in root.findall('error') if e.findtext('kind') != 'Leak_StillReachable']
        finished = any(s.findtext('state') == 'FINISHED' for s in root.findall('status'))
        if result.returncode == 97 or errors or not finished or root.tag != 'valgrindoutput':
            raise AssertionError(f'Valgrind detected errors or did not finish; see {report}')


def run(*args, valgrind=None):
    checked = valgrind or (args[0] == FSCK and os.environ.get('HTA_FSCK_VALGRIND'))
    report = None
    if checked:
        logdir = pathlib.Path(os.environ.get('HTA_FSCK_VALGRIND_LOGS', tempfile.gettempdir()))
        logdir.mkdir(parents=True, exist_ok=True)
        prefix = 'hta_fsck.' if args[0] == FSCK else 'harness-expected-error.'
        fd, report = tempfile.mkstemp(prefix=prefix, suffix='.xml', dir=logdir)
        os.close(fd)
        tool = os.environ.get('HTA_FSCK_VALGRIND_TOOL', 'memcheck') if args[0] == FSCK else 'memcheck'
        options = ('--leak-check=full', '--show-leak-kinds=all',
                   '--errors-for-leak-kinds=definite,indirect,possible', '--track-origins=yes')
        if tool == 'helgrind':
            options = ('--suppressions=' + str(pathlib.Path(__file__).with_name('fsck_helgrind.supp')),)
        args = (checked, '--tool=' + tool, *options, '--error-exitcode=97',
                '--xml=yes', f'--xml-file={report}', f'--log-file={report}.log', *args)
    result = subprocess.run(args, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                            timeout=120 if checked else 30)
    check_process(result, report)
    return result


def hashes(directory):
    return {p.name: hashlib.sha256(p.read_bytes()).hexdigest()
            for p in directory.glob('*.hta')}


def snapshot(directory):
    # Access times may change through reads. No bytes, mtimes, modes, sizes,
    # inodes or directory entries may change on a healthy metric.
    return {str(p.relative_to(directory)): (p.stat().st_ino, p.stat().st_size,
             p.stat().st_mtime_ns, p.stat().st_mode,
             hashlib.sha256(p.read_bytes()).hexdigest() if p.is_file() else None)
            for p in [directory, *directory.rglob('*')]}


class Recovery(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.TemporaryDirectory(prefix='hta-fsck-test-')
        cls.root = pathlib.Path(cls.tmp.name)
        cls.clean = cls.root / 'clean'
        subprocess.run([FIXTURE, str(cls.clean), '30000'], check=True)
        cls.oracle = cls.root / 'oracle'
        shutil.copytree(cls.clean, cls.oracle)
        result = run(REPAIR, str(cls.oracle))
        if result.returncode:
            raise RuntimeError(result.stderr)
        # hta_repair is itself checked against normal insertion, including float bits.
        for name, digest in hashes(cls.clean).items():
            assert hashes(cls.oracle)[name] == digest, name

    @classmethod
    def tearDownClass(cls):
        cls.tmp.cleanup()

    def setUp(self):
        self.case = pathlib.Path(tempfile.mkdtemp(dir=self.root)) / 'metric'
        shutil.copytree(self.clean, self.case)

    def apply(self, *options, expected=None):
        tree_before = snapshot(self.case)
        originals = {p.name: p.read_bytes() for p in self.case.glob('*.hta')}
        # Run the real hta_repair on the SAME damaged input. It cannot open a
        # partial raw record, so normalize only that incomplete record in its copy.
        reference = pathlib.Path(tempfile.mkdtemp(dir=self.root)) / 'reference'
        reference.mkdir()
        for p in self.case.glob('*.hta'):
            shutil.copy2(p, reference / p.name)
        p = reference / 'raw.hta'
        with p.open('r+b') as f:
            f.truncate(p.stat().st_size - (p.stat().st_size - HEADER) % 16)
        repaired = run(REPAIR, str(reference))
        self.assertEqual(repaired.returncode, 0, repaired.stderr)
        expected = hashes(self.oracle) if expected is None else expected
        self.assertEqual(hashes(reference), expected)
        result = run(FSCK, '--metric', '--apply', *options, str(self.case))
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual(hashes(self.case), expected)
        # Byte comparison in addition to hashes, and a repeat must change nothing.
        for name in expected:
            self.assertEqual((self.case / name).read_bytes(), (reference / name).read_bytes())
        # Only repaired HTA files and the declared recovery journal may differ.
        tree_after = snapshot(self.case)
        journal = self.case / '.hta-fsck-recovery'
        manifest = json.loads((journal / 'manifest.json').read_text())
        self.assertEqual(manifest['state'], 'complete')
        changed = {entry['name'] for entry in manifest['files']}
        self.assertEqual(len(changed), len(manifest['files']))
        journal_paths = {'.hta-fsck-recovery', '.hta-fsck-recovery/manifest.json'}
        for entry in manifest['files']:
            name, offset = entry['name'], entry['offset']
            journal_paths.update(f'.hta-fsck-recovery/{name}{suffix}'
                                 for suffix in ('.before', '.after'))
            self.assertEqual(entry['existed'], name in originals)
            self.assertEqual((journal / (name + '.before')).read_bytes(),
                             originals.get(name, b'')[offset:])
            self.assertEqual((journal / (name + '.after')).read_bytes(),
                             (self.case / name).read_bytes()[offset:])
        self.assertEqual(set(tree_after), set(tree_before) | set(expected) | journal_paths)
        for path, state in tree_before.items():
            if path != '.' and path not in changed:
                self.assertEqual(tree_after[path], state, path)
        result = run(FSCK, '--metric', '--apply', str(self.case))
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual(snapshot(self.case), tree_after)
        return result

    def archive_backup(self, name):
        backup = self.case / '.hta-fsck-recovery'
        if backup.exists():
            backup.rename(self.case / name)

    def truncate(self, name, count):
        p = self.case / name
        with p.open('r+b') as f:
            f.truncate(p.stat().st_size - count)

    def test_missing_tail_all_levels(self):
        for p in self.case.glob('*.hta'):
            if p.name != 'raw.hta':
                self.truncate(p.name, ROW * 2 + 13)
        self.apply()

    def test_partial_upper_with_missing_lower(self):
        self.truncate('1000.hta', ROW)
        self.truncate('10000.hta', 1)
        self.apply()

    def test_all_partial_aggregate_lengths(self):
        for length in range(1, ROW):
            with self.subTest(length=length):
                # Slice a genuine final record, not just append arbitrary junk.
                self.truncate('1000.hta', length)
                self.apply()
                self.archive_backup(f'backup-{length}')

    def test_lost_last_raw_record(self):
        reference = pathlib.Path(tempfile.mkdtemp(dir=self.root)) / 'shorter'
        subprocess.run([FIXTURE, str(reference), '29999'], check=True)
        self.truncate('raw.hta', 3)
        self.apply(expected=hashes(reference))

    def test_raw_tail_loss_removes_unclosed_aggregates_not_shortens_them(self):
        raw = (self.clean / 'raw.hta').read_bytes()
        timestamps = [t for t, _ in struct.iter_unpack('=qd', raw[HEADER:])]
        aggregates = {p.name: p.read_bytes() for p in self.clean.glob('*.hta')
                      if p.name != 'raw.hta'}
        # Lose thousands of raw records with ALL originally written aggregates
        # still present. Keep the last point just before/after a boundary of
        # the largest level, exercising the closing-point rule on every level.
        boundary = timestamps[len(timestamps) // 2] // 1000000 * 1000000
        crossing = bisect.bisect_left(timestamps, boundary)
        self.assertGreater(crossing, 1)
        self.assertLess(crossing + 1, len(timestamps) - 1000)
        self.assertLess(timestamps[crossing - 1], boundary)
        self.assertGreaterEqual(timestamps[crossing], boundary)
        for keep in (crossing, crossing + 1):
            reference = pathlib.Path(tempfile.mkdtemp(dir=self.root)) / 'surviving-raw'
            subprocess.run([FIXTURE, str(reference), str(keep)], check=True)
            expected = hashes(reference)
            for full in (False, True):
                for partial in (0, 9):
                    with self.subTest(keep=keep, full=full, partial=partial):
                        self.case = pathlib.Path(tempfile.mkdtemp(dir=self.root)) / 'metric'
                        shutil.copytree(self.clean, self.case)
                        self.truncate('raw.hta', len(raw) - (HEADER + keep * 16 + partial))
                        self.assertEqual({name: (self.case / name).read_bytes()
                                          for name in aggregates}, aggregates)
                        damaged = hashes(self.case)
                        before = snapshot(self.case)
                        options = ('--full',) if full else ()
                        plan = run(FSCK, '--metric', *options, str(self.case))
                        self.assertEqual(plan.returncode, 0, plan.stderr)
                        self.assertIn('1 need repair', plan.stdout)
                        self.assertEqual(snapshot(self.case), before)

                        # The common helper compares hashes AND bytes against
                        # hta_repair on the damaged input, validates the entire
                        # journal/tree, and checks that a repeat is a no-op.
                        self.apply(*options, expected=expected)
                        self.assertEqual((self.case / 'raw.hta').read_bytes(),
                                         raw[:HEADER + keep * 16])
                        last = timestamps[keep - 1]
                        for name, original in aggregates.items():
                            interval = int(pathlib.Path(name).stem)
                            data = (self.case / name).read_bytes()
                            self.assertLess(len(data), len(original), name)
                            self.assertEqual(data, original[:len(data)], name)
                            rows = list(struct.iter_unpack('=qdddQdq', data[HEADER:]))
                            self.assertGreater(len(rows), 1, name)
                            epoch = timestamps[0] // interval * interval
                            self.assertEqual(len(rows), (last // interval * interval - epoch)
                                             // interval, name)
                            for i, row in enumerate(rows):
                                time, active_time = row[0], row[6]
                                self.assertEqual(time, epoch + i * interval, name)
                                self.assertLessEqual(time + interval, last, name)
                                # Only the FIRST row may have reduced active_time;
                                # the final surviving row must remain full width.
                                self.assertEqual(active_time,
                                                 epoch + interval - timestamps[0]
                                                 if i == 0 else interval, name)
                            self.assertLess(rows[0][6], interval, name)
                            self.assertEqual(rows[-1][0] + interval,
                                             last // interval * interval, name)
                            self.assertEqual(rows[-1][6], interval, name)

                        result = run(FSCK, '--metric', '--rollback', str(self.case))
                        self.assertEqual(result.returncode, 0, result.stderr)
                        # Including the formerly ahead-of-raw aggregates and
                        # any original partial raw record: rollback is lossless.
                        self.assertEqual(hashes(self.case), damaged)

    def test_nonfinite_aggregate_tail(self):
        p = self.case / '1000.hta'
        data = bytearray(p.read_bytes())
        struct.pack_into('=d', data, len(data) - ROW + 40, float('nan'))
        p.write_bytes(data)
        self.apply()

    def test_full_detects_isolated_old_corruption(self):
        p = self.case / '1000.hta'
        data = bytearray(p.read_bytes())
        struct.pack_into('=Q', data, HEADER + 17 * ROW + 32, 999999)
        p.write_bytes(data)
        self.apply('--full')

    def test_deterministic_random_truncations(self):
        rng = random.Random(20260917)
        for case in range(10):
            with self.subTest(case=case):
                for p in self.case.glob('*.hta'):
                    if p.name != 'raw.hta':
                        self.truncate(p.name, rng.randrange(0, min(400, p.stat().st_size - HEADER)))
                self.apply()
                self.archive_backup(f'backup-random-{case}')

    def test_fast_path_does_not_scan_all_raw(self):
        before = hashes(self.case)
        result = run(FSCK, '--metric', str(self.case))
        self.assertEqual(result.returncode, 0, result.stderr)
        reads = int(re.search(r'cached blocks\): (\d+)', result.stdout)[1])
        self.assertLess(reads, 30000)
        self.assertEqual(hashes(self.case), before)
        self.assertEqual(set(p.name for p in self.case.iterdir()), set(before))

    def test_healthy_metric_is_unchanged(self):
        before = snapshot(self.case)
        for options in ((), ('--apply',), ('--full',), ('--full', '--apply')):
            with self.subTest(options=options):
                result = run(FSCK, '--metric', *options, str(self.case))
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertIn('1 unchanged, 0', result.stdout)
                self.assertEqual(snapshot(self.case), before)

    def database(self):
        db = pathlib.Path(tempfile.mkdtemp(dir=self.root))
        for name in ('a.healthy', 'b.damaged', 'c.healthy', 'd.damaged', 'e.backup-12345'):
            shutil.copytree(self.clean, db / name)
        p = db / 'b.damaged' / '1000.hta'
        p.write_bytes(p.read_bytes()[:-ROW * 2 - 5])
        (db / 'd.damaged' / '10000.hta').unlink()
        # Backups are deliberately corrupt but must be skipped entirely.
        (db / 'e.backup-12345' / 'raw.hta').write_bytes(b'broken backup')
        return db

    def test_database_mode_preserves_healthy_metrics(self):
        db = self.database()
        healthy = {name: snapshot(db / name) for name in ('a.healthy', 'c.healthy', 'e.backup-12345')}
        before = snapshot(db)
        plan = run(FSCK, str(db))
        self.assertEqual(plan.returncode, 0, plan.stderr)
        self.assertIn('2 unchanged, 2 need repair, 0 failed', plan.stdout)
        self.assertEqual(snapshot(db), before)
        result = run(FSCK, '--apply', str(db))
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn('2 unchanged, 2 repaired, 0 failed', result.stdout)
        for name, state in healthy.items():
            self.assertEqual(snapshot(db / name), state)
        for name in ('b.damaged', 'd.damaged'):
            self.assertEqual(hashes(db / name), hashes(self.oracle))
        after = snapshot(db)
        result = run(FSCK, '--apply', str(db))
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn('4 unchanged, 0 repaired, 0 failed', result.stdout)
        self.assertEqual(snapshot(db), after)

    def test_database_mode_continues_after_failure(self):
        db = self.database()
        (db / 'a.healthy' / 'raw.hta').write_bytes(b'broken header')
        before = snapshot(db / 'a.healthy')
        result = run(FSCK, '--apply', str(db))
        self.assertEqual(result.returncode, 1, result.stderr)
        self.assertIn('1 unchanged, 2 repaired, 1 failed', result.stdout)
        self.assertEqual(snapshot(db / 'a.healthy'), before)
        for name in ('b.damaged', 'd.damaged'):
            self.assertEqual(hashes(db / name), hashes(self.oracle))

    def test_database_rollback(self):
        db = self.database()
        before = {p.name: hashes(p) for p in db.iterdir()}
        self.assertEqual(run(FSCK, '--apply', str(db)).returncode, 0)
        result = run(FSCK, '--rollback', str(db))
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual({p.name: hashes(p) for p in db.iterdir()}, before)

    def test_explicit_modes_reject_wrong_layout_without_writes(self):
        db = self.database()
        for options, path in (((), self.case), (('--metric',), db)):
            before = snapshot(path)
            result = run(FSCK, '--apply', *options, str(path))
            self.assertEqual(result.returncode, 1, result.stdout)
            self.assertEqual(snapshot(path), before)
        for name in ('stray.hta', 'raw.hta'):
            with self.subTest(name=name):
                stray = db / name
                stray.write_bytes(b'not a metric')
                before = snapshot(db)
                result = run(FSCK, '--apply', str(db))
                self.assertEqual(result.returncode, 1)
                self.assertIn('unexpected .hta file in database root', result.stderr)
                self.assertEqual(snapshot(db), before)
                stray.unlink()

    def test_backup_directories_are_skipped_in_all_database_modes(self):
        db = pathlib.Path(tempfile.mkdtemp(dir=self.root))
        shutil.copytree(self.clean, db / 'healthy')
        backups = ('metric.backup-1', 'metric.backup-20260918',
                   'metric.backup-1726653600123456789', 'metric.backup-1.backup-2')
        for name in backups:
            backup = db / name
            backup.mkdir()
            (backup / 'raw.hta').write_bytes(b'broken backup, must not be opened')
            (backup / '.hta-fsck-recovery').mkdir()
            (backup / '.hta-fsck-recovery' / 'manifest.json').write_bytes(b'invalid journal')
        before = snapshot(db)
        for options in ((), ('--full',), ('--apply',), ('--full', '--apply'), ('--rollback',)):
            with self.subTest(options=options):
                result = run(FSCK, *options, str(db))
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertIn('1 unchanged, 0', result.stdout)
                self.assertEqual(result.stdout.count('Skipped backup directory:'), len(backups))
                for name in backups:
                    self.assertIn('Skipped backup directory: ' + str(db / name), result.stdout)
                self.assertEqual(snapshot(db), before)

    def test_backup_pattern_does_not_skip_other_metric_names(self):
        db = pathlib.Path(tempfile.mkdtemp(dir=self.root))
        names = ('metric.backup-', 'metric.backup-123x', 'metric.backup-123.extra',
                 'metric.backup-2026-09-18', '.backup-123', '.hta-fsck-real', 'metric.hta')
        for name in names:
            shutil.copytree(self.clean, db / name)
            path = db / name / '1000.hta'
            path.write_bytes(path.read_bytes()[:-ROW])
        result = run(FSCK, '--apply', str(db))
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn(f'{len(names)} repaired, 0 failed', result.stdout)
        self.assertNotIn('Skipped backup directory:', result.stdout)
        for name in names:
            self.assertEqual(hashes(db / name), hashes(self.oracle))

    def test_metric_flag_can_repair_and_rollback_a_backup_directory(self):
        backup = self.case.with_name('metric.backup-12345')
        self.case.rename(backup)
        self.case = backup
        self.truncate('1000.hta', ROW)
        damaged = hashes(self.case)
        before = snapshot(self.case)
        result = run(FSCK, '--apply', str(self.case.parent))
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn('Skipped backup directory:', result.stdout)
        self.assertIn('0 unchanged, 0 repaired, 0 failed', result.stdout)
        self.assertEqual(snapshot(self.case), before)
        self.apply()
        repaired = snapshot(self.case)
        result = run(FSCK, '--rollback', str(self.case.parent))
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(snapshot(self.case), repaired)
        result = run(FSCK, '--metric', '--rollback', str(self.case))
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(hashes(self.case), damaged)

    def test_exclude_validation(self):
        before = snapshot(self.case)
        for args in (('--metric', '--exclude', 'backup'), ('--exclude', '../backup'),
                     ('--exclude', '.'), ('--exclude', 'missing'), ('--exclude',)):
            result = run(FSCK, str(self.case), *args)
            self.assertEqual(result.returncode, 1)
            self.assertEqual(snapshot(self.case), before)

    def test_no_journal_rollback_is_unchanged_in_both_modes(self):
        before = snapshot(self.case)
        for args in (('--metric', str(self.case)), (str(self.case.parent),)):
            result = run(FSCK, '--rollback', *args)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertIn('1 unchanged, 0 rolled back', result.stdout)
            self.assertEqual(snapshot(self.case), before)

    def test_completed_rollback_allows_checks_and_requires_archive_for_repair(self):
        self.truncate('1000.hta', ROW)
        self.apply()
        result = run(FSCK, '--metric', '--rollback', str(self.case))
        self.assertEqual(result.returncode, 0, result.stderr)
        before = snapshot(self.case)
        journal = self.case / '.hta-fsck-recovery' / 'manifest.json'
        self.assertEqual(json.loads(journal.read_text())['state'], 'rolled-back')
        for args in (('--metric', str(self.case)), (str(self.case.parent),)):
            for full in ((), ('--full',)):
                with self.subTest(args=args, full=full):
                    result = run(FSCK, *full, *args)
                    self.assertEqual(result.returncode, 0, result.stderr)
                    self.assertIn('1 need repair', result.stdout)
                    self.assertEqual(snapshot(self.case), before)
                    result = run(FSCK, '--apply', *full, *args)
                    self.assertEqual(result.returncode, 1)
                    self.assertIn('archive existing .hta-fsck-recovery', result.stderr)
                    self.assertNotIn('unfinished recovery', result.stderr)
                    self.assertEqual(snapshot(self.case), before)
            result = run(FSCK, '--rollback', *args)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertIn('Already rolled back', result.stdout)
            self.assertIn('1 unchanged, 0 rolled back', result.stdout)
            self.assertEqual(snapshot(self.case), before)
        self.archive_backup('archived-rollback')
        self.apply()

    def test_symlink_paths_are_explicitly_refused_without_touching_targets(self):
        self.truncate('1000.hta', ROW)
        before = snapshot(self.case)
        links = pathlib.Path(tempfile.mkdtemp(dir=self.root))
        alias = links / 'alias'
        alias.symlink_to(self.case, target_is_directory=True)
        parent_alias = links / 'parent'
        parent_alias.symlink_to(self.case.parent, target_is_directory=True)
        for path in (str(alias), str(alias) + '/', str(alias) + '/.',
                     str(parent_alias / self.case.name)):
            for options in ((), ('--apply',), ('--rollback',)):
                result = run(FSCK, '--metric', *options, path)
                self.assertEqual(result.returncode, 1)
                self.assertIn('symlinks are not supported:', result.stderr)
                self.assertNotIn('Not a directory', result.stderr)
                self.assertEqual(snapshot(self.case), before)
        result = run(FSCK, '--apply', str(parent_alias))
        self.assertEqual(result.returncode, 1)
        self.assertIn('symlinks are not supported:', result.stderr)
        self.assertEqual(snapshot(self.case), before)

    def test_database_symlinks_report_errors_or_can_be_excluded(self):
        db = pathlib.Path(tempfile.mkdtemp(dir=self.root))
        shutil.copytree(self.clean, db / 'healthy')
        (db / 'alias').symlink_to(self.case, target_is_directory=True)
        (db / 'broken').symlink_to(db / 'absent', target_is_directory=True)
        self.truncate('1000.hta', ROW)
        before = snapshot(self.case)
        healthy = snapshot(db / 'healthy')
        result = run(FSCK, '--apply', str(db))
        self.assertEqual(result.returncode, 1)
        self.assertEqual(result.stderr.count('symlinks are not supported:'), 2)
        self.assertIn('1 unchanged, 0 repaired, 2 failed', result.stdout)
        self.assertEqual(snapshot(self.case), before)
        self.assertEqual(snapshot(db / 'healthy'), healthy)
        result = run(FSCK, '--apply', '--exclude', 'alias', '--exclude', 'broken', str(db))
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.count('Excluded directory:'), 2)
        self.assertEqual(snapshot(self.case), before)
        self.assertEqual(snapshot(db / 'healthy'), healthy)

    def test_lost_found_is_an_explicit_exclusion_not_a_name_heuristic(self):
        db = pathlib.Path(tempfile.mkdtemp(dir=self.root))
        shutil.copytree(self.clean, db / 'healthy')
        (db / 'lost+found').mkdir()
        before = snapshot(db)
        result = run(FSCK, '--apply', str(db))
        self.assertEqual(result.returncode, 1)
        self.assertIn('lost+found', result.stderr)
        self.assertIn('1 unchanged, 0 repaired, 1 failed', result.stdout)
        self.assertEqual(snapshot(db), before)
        result = run(FSCK, '--apply', '--exclude', 'lost+found', str(db))
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn('Excluded directory:', result.stdout)
        self.assertEqual(snapshot(db), before)
        # The same name can also be a real metric: never skip it implicitly.
        shutil.copytree(self.clean, db / 'lost+found', dirs_exist_ok=True)
        path = db / 'lost+found' / '1000.hta'
        path.write_bytes(path.read_bytes()[:-ROW])
        result = run(FSCK, '--apply', str(db))
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn('1 unchanged, 1 repaired', result.stdout)
        self.assertEqual(hashes(db / 'lost+found'), hashes(self.oracle))

    def test_orphan_staging_after_crash_is_reported_and_preserved(self):
        self.truncate('1000.hta', ROW)
        before = hashes(self.case)
        result = self.fault('manifest.tmp', crash=True)
        self.assertEqual(result.returncode, 99, result.stderr)
        self.assertEqual(hashes(self.case), before)
        stages = list(self.case.glob('.hta-fsck-stage-*'))
        self.assertEqual(len(stages), 1)
        orphan = snapshot(stages[0])
        result = run(FSCK, '--metric', str(self.case))
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn('orphan staging entry', result.stderr)
        self.assertIn(str(stages[0]), result.stderr)
        self.apply()
        self.assertEqual(snapshot(stages[0]), orphan)

    def test_rebuild_preserves_unrelated_tree(self):
        extra = self.case / 'notes' / 'nested'
        extra.mkdir(parents=True)
        (extra / 'keep.txt').write_bytes(b'important unrelated contents')
        (self.case / 'README').write_bytes(b'keep this too')
        self.truncate('1000.hta', ROW)
        self.apply()

    def test_streaming_raw_read_budget_for_full_scan_and_rebuild(self):
        count = (self.case / 'raw.hta').stat().st_size // 16
        for rebuild in (False, True):
            if rebuild:
                for path in self.case.glob('*.hta'):
                    if path.name != 'raw.hta':
                        path.unlink()
            result = run(FSCK, '--metric', '--full', '--apply', str(self.case))
            self.assertEqual(result.returncode, 0, result.stderr)
            requests = int(re.search(r'Raw read requests .*: (\d+)', result.stdout)[1])
            self.assertLess(requests, 6 * ((count + 4095) // 4096) + 64)
            self.assertEqual(hashes(self.case), hashes(self.oracle))

    def test_fast_tail_check_avoids_global_raw_probes(self):
        result = run(FSCK, '--metric', str(self.case))
        self.assertEqual(result.returncode, 0, result.stderr)
        requests = int(re.search(r'Raw read requests .*: (\d+)', result.stdout)[1])
        records = int(re.search(r'Raw records read .*: (\d+)', result.stdout)[1])
        self.assertLessEqual(requests, 5, result.stdout)
        self.assertLessEqual(records, 8194, result.stdout)
        self.assertIn('[unchanged]', result.stdout)

    def test_parallel_modes_match_serial_bytes_and_group_complete_plans(self):
        db = self.database()
        serial = pathlib.Path(tempfile.mkdtemp(dir=self.root)) / 'db'
        shutil.copytree(db, serial)
        before = snapshot(db)
        for jobs in (1, 2, 4):
            result = run(FSCK, '--jobs', str(jobs), str(db))
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(snapshot(db), before)
            self.assertIn('Summary: 2 unchanged, 2 need repair, 0 failed', result.stdout)
            lines = result.stdout.splitlines()
            for i, line in enumerate(lines):
                if line.startswith(('[unchanged]', '[needs repair]')):
                    # The whole plan follows its own completion marker atomically.
                    self.assertTrue(lines[i + 1].startswith('raw.hta:'), result.stdout)
                    self.assertTrue(lines[i + 7].startswith('Raw read requests'), result.stdout)
        result = run(FSCK, '--jobs', '1', '--apply', str(serial))
        self.assertEqual(result.returncode, 0, result.stderr)
        result = run(FSCK, '--jobs', '4', '--apply', str(db))
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual({p: v[-1] for p, v in snapshot(db).items()},
                         {p: v[-1] for p, v in snapshot(serial).items()})
        after = snapshot(db)
        for name in ('a.healthy', 'c.healthy', 'e.backup-12345'):
            self.assertEqual({p: v for p, v in before.items() if p == name or p.startswith(name + '/')},
                             {p: v for p, v in after.items() if p == name or p.startswith(name + '/')})
        result = run(FSCK, '--jobs', '4', '--rollback', str(db))
        self.assertEqual(result.returncode, 0, result.stderr)
        restored = snapshot(db)
        for name, original in before.items():
            if not (db / name).is_dir():
                self.assertEqual(restored[name][-1], original[-1])

    def test_parallel_failure_does_not_skip_other_metrics(self):
        db = self.database()
        (db / 'a.healthy' / 'raw.hta').write_bytes(b'broken')
        result = run(FSCK, '--jobs', '3', '--apply', str(db))
        self.assertEqual(result.returncode, 1, result.stderr)
        self.assertIn('Summary: 1 unchanged, 2 repaired, 1 failed', result.stdout)
        self.assertEqual(hashes(db / 'b.damaged'), hashes(self.oracle))
        self.assertEqual(hashes(db / 'd.damaged'), hashes(self.oracle))

    def test_parallel_full_rebuild_matches_serial(self):
        db = self.database()
        # Exercise raw truncation, a complete missing bottom level and a bad
        # aggregate header while other workers scan all records concurrently.
        with (db / 'b.damaged' / 'raw.hta').open('r+b') as stream:
            stream.truncate(HEADER + 17000 * 16 + 7)
        (db / 'd.damaged' / '1000.hta').unlink()
        with (db / 'd.damaged' / '100000.hta').open('r+b') as stream:
            stream.write(b'BADMAGIC')
        serial = pathlib.Path(tempfile.mkdtemp(dir=self.root)) / 'db'
        shutil.copytree(db, serial)
        for jobs, target in ((1, serial), (4, db)):
            result = run(FSCK, '--jobs', str(jobs), '--full', '--apply', str(target))
            self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual({p: v[-1] for p, v in snapshot(db).items()},
                         {p: v[-1] for p, v in snapshot(serial).items()})
        before = snapshot(db)
        result = run(FSCK, '--jobs', '4', '--full', '--apply', str(db))
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(snapshot(db), before)

    def test_parallel_enospc_keeps_other_workers_and_rollback_working(self):
        db = self.database()
        before = snapshot(db)
        result = subprocess.run([FSCK, '--jobs', '4', '--apply', str(db)],
                     env=dict(os.environ, LD_PRELOAD=FAULT, FSCK_TEST_FAIL_SUFFIX='/1000.hta'),
                     text=True, capture_output=True, timeout=30)
        self.assertEqual(result.returncode, 1, result.stderr)
        self.assertIn('Summary: 2 unchanged, 1 repaired, 1 failed', result.stdout)
        self.assertEqual(hashes(db / 'd.damaged'), hashes(self.oracle))
        result = run(FSCK, '--jobs', '4', '--rollback', str(db))
        self.assertEqual(result.returncode, 0, result.stderr)
        after = snapshot(db)
        for name, original in before.items():
            if not (db / name).is_dir():
                self.assertEqual(after[name][-1], original[-1])

    def test_jobs_arguments_are_bounded(self):
        before = snapshot(self.case)
        for value in ('0', '-1', '65', '1.5', '2x', '999999999999999999999999'):
            result = run(FSCK, '--metric', '--jobs', value, str(self.case))
            self.assertEqual(result.returncode, 1, result.stderr)
            self.assertIn('--jobs requires', result.stderr)
        self.assertEqual(run(FSCK, '--jobs').returncode, 1)
        self.assertEqual(run(FSCK, '--metric', '-j', '64', str(self.case)).returncode, 0)
        empty = pathlib.Path(tempfile.mkdtemp(dir=self.root))
        self.assertEqual(run(FSCK, '--jobs', '4', str(empty)).returncode, 0)
        self.assertEqual(snapshot(self.case), before)

    def gated_database(self):
        root = pathlib.Path(tempfile.mkdtemp(dir=self.root))
        db, gate = root / 'db', root / 'gate'
        db.mkdir()
        gate.mkdir()
        for index in range(7):
            metric = db / f'metric-{index}'
            shutil.copytree(self.clean, metric)
            path = metric / '1000.hta'
            with path.open('r+b') as stream:
                stream.truncate(path.stat().st_size - ROW)
        return db, gate

    def wait_for_workers(self, process, gate, jobs):
        deadline = time.monotonic() + 10
        while len(list(gate.glob('ready-*'))) < jobs:
            self.assertIsNone(process.poll(), 'worker process exited before rendezvous')
            self.assertLess(time.monotonic(), deadline, 'workers did not execute concurrently')
            time.sleep(0.01)
        self.assertEqual(len(list(gate.glob('ready-*'))), jobs)

    def wait_for_stop_message(self, process):
        text = b''
        deadline = time.monotonic() + 10
        while b'finishing active metrics.' not in text:
            self.assertLess(time.monotonic(), deadline, 'stop was not acknowledged')
            if select.select([process.stderr], [], [], 0.1)[0]:
                chunk = os.read(process.stderr.fileno(), 4096)
                self.assertTrue(chunk, 'process exited before stop acknowledgement')
                text += chunk
        return text

    def test_parallel_graceful_stop_finishes_active_and_preserves_queued(self):
        for jobs, sig, apply, installing in ((1, signal.SIGINT, True, False),
                                            (3, signal.SIGINT, True, False),
                                            (3, signal.SIGTERM, True, False),
                                            (3, signal.SIGINT, False, False),
                                            (3, signal.SIGINT, True, True)):
            with self.subTest(jobs=jobs, signal=sig, apply=apply, installing=installing):
                db, gate = self.gated_database()
                before = {p.name: snapshot(p) for p in db.iterdir()}
                args = ['--apply'] if apply else []
                env = dict(os.environ, LD_PRELOAD=FAULT, FSCK_TEST_GATE_DIR=str(gate))
                if installing:
                    env['FSCK_TEST_GATE_INSTALL'] = '1'
                process = subprocess.Popen([FSCK, '--jobs', str(jobs), *args, str(db)],
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                            env=env)
                try:
                    self.wait_for_workers(process, gate, jobs)
                    process.send_signal(sig)
                    self.wait_for_stop_message(process)
                    (gate / 'release').touch()
                    out, err = process.communicate(timeout=30)
                    self.assertEqual(process.returncode, 128 + sig, err)
                    self.assertIn(f'{7 - jobs} not started (interrupted)', out.decode())
                    self.assertEqual(len(list(gate.glob('ready-*'))), jobs)
                    active = {p.name[len('ready-'):] for p in gate.glob('ready-*')}
                    for metric in db.iterdir():
                        if apply and metric.name in active:
                            self.assertEqual(hashes(metric), hashes(self.oracle))
                            manifest = json.loads((metric / '.hta-fsck-recovery' / 'manifest.json').read_text())
                            self.assertEqual(manifest['state'], 'complete')
                        else:
                            self.assertEqual(snapshot(metric), before[metric.name])
                finally:
                    if process.poll() is None:
                        process.kill()
                    process.communicate()

    def test_second_signal_exits_with_recoverable_parallel_journals(self):
        db, gate = self.gated_database()
        before = {p.name: hashes(p) for p in db.iterdir()}
        process = subprocess.Popen([FSCK, '--jobs', '2', '--apply', str(db)],
                    stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                    env=dict(os.environ, LD_PRELOAD=FAULT, FSCK_TEST_GATE_DIR=str(gate),
                             FSCK_TEST_GATE_INSTALL='1'))
        try:
            self.wait_for_workers(process, gate, 2)
            process.send_signal(signal.SIGINT)
            self.wait_for_stop_message(process)
            process.send_signal(signal.SIGTERM)
            out, err = process.communicate(timeout=10)
            self.assertEqual(process.returncode, 143, err)
            self.assertEqual(len(list(gate.glob('ready-*'))), 2)
            check = run(FSCK, '--jobs', '2', str(db))
            self.assertEqual(check.returncode, 1)
            self.assertIn('unfinished recovery', check.stderr)
            rollback = run(FSCK, '--jobs', '2', '--rollback', str(db))
            self.assertEqual(rollback.returncode, 0, rollback.stderr)
            self.assertEqual({p.name: hashes(p) for p in db.iterdir()}, before)
        finally:
            if process.poll() is None:
                process.kill()
            process.communicate()

    def test_flock_error_reports_errno_without_claiming_contention(self):
        before = snapshot(self.case)
        env = dict(os.environ, LD_PRELOAD=FAULT, FSCK_TEST_FLOCK_ERROR='1')
        result = subprocess.run([FSCK, '--metric', '--apply', str(self.case)], env=env,
                                text=True, capture_output=True, timeout=30)
        self.assertEqual(result.returncode, 1)
        self.assertIn('cannot lock metric:', result.stderr)
        self.assertIn(os.strerror(errno.ENOLCK), result.stderr)
        self.assertNotIn('another fsck', result.stderr)
        self.assertEqual(snapshot(self.case), before)

    def terminal_run(self, *args, term='xterm', width=100, env=None, on_started=None):
        master, slave = pty.openpty()
        fcntl.ioctl(slave, termios.TIOCSWINSZ, struct.pack('HHHH', 25, width, 0, 0))
        # Use the same Valgrind XML checks as pipe-based tests when requested.
        command = [FSCK, *args]
        report = None
        if os.environ.get('HTA_FSCK_VALGRIND'):
            fd, report = tempfile.mkstemp(prefix='hta_fsck.tty.', suffix='.xml',
                                         dir=os.environ.get('HTA_FSCK_VALGRIND_LOGS'))
            os.close(fd)
            command = [os.environ['HTA_FSCK_VALGRIND'], '--tool=memcheck', '--leak-check=full',
                       '--show-leak-kinds=all', '--errors-for-leak-kinds=definite,indirect,possible',
                       '--track-origins=yes', '--error-exitcode=97', '--xml=yes',
                       f'--xml-file={report}', f'--log-file={report}.log', *command]
        process = subprocess.Popen(command, stdout=slave, stderr=slave,
                                   env=dict(os.environ, TERM=term, **(env or {})))
        os.close(slave)
        output = bytearray()
        deadline = time.monotonic() + 120
        try:
            if on_started:
                on_started(process)
            while True:
                self.assertLess(time.monotonic(), deadline, 'terminal process timed out')
                if not select.select([master], [], [], 0.2)[0]:
                    continue
                try:
                    chunk = os.read(master, 65536)
                except OSError as error:
                    if error.errno == errno.EIO:
                        break
                    raise
                if not chunk:
                    break
                output.extend(chunk)
            process.wait(timeout=10)
        finally:
            os.close(master)
            if process.poll() is None:
                process.kill()
                process.wait()
        text = output.decode(errors='replace')
        check_process(subprocess.CompletedProcess(command, process.returncode, text, text), report)
        return process.returncode, text

    def test_parallel_terminal_shows_multiple_active_metrics(self):
        db, gate = self.gated_database()
        def release(process):
            self.wait_for_workers(process, gate, 3)
            (gate / 'release').touch()
        code, text = self.terminal_run('--jobs', '3', str(db),
                                      env=dict(LD_PRELOAD=FAULT, FSCK_TEST_GATE_DIR=str(gate)),
                                      on_started=release)
        self.assertEqual(code, 0, text)
        frames = re.findall(r'(?:Current:[^\r\n]*\r?\n){3}', text)
        self.assertTrue(any(set(re.findall(r'Current: (metric-\d+)', frame)) ==
                            {'metric-0', 'metric-1', 'metric-2'} for frame in frames), text)
        self.assertEqual(text.count('[needs repair]'), 7)
        self.assertIn('100% 7/7 ETA 0m 0s', text)

    def test_terminal_progress_completed_list_and_eta(self):
        db = self.database()
        code, text = self.terminal_run('--apply', str(db))
        self.assertEqual(code, 0, text)
        self.assertIn('\x1b[2K', text)
        self.assertIn('Current: b.damaged', text)
        self.assertIn('[unchanged] ' + str(db / 'a.healthy'), text)
        self.assertIn('[repaired] ' + str(db / 'b.damaged'), text)
        self.assertIn('[repaired] ' + str(db / 'd.damaged'), text)
        self.assertNotIn('Raw records read', text)
        self.assertRegex(text.splitlines()[-1], r'\[=+\] 100% 4/4 ETA 0m 0s')
        self.assertLess(text.index('[unchanged] '), text.index('[repaired] '))

    def test_terminal_dry_run_shows_same_per_level_plan_as_pipe(self):
        self.truncate('1000.hta', ROW)
        with (self.case / 'raw.hta').open('ab') as stream:
            stream.write(b'partial')
        before = snapshot(self.case)
        for options in ((), ('--full',)):
            result = run(FSCK, '--metric', *options, str(self.case))
            self.assertEqual(result.returncode, 0, result.stderr)
            plan = [line for line in result.stdout.splitlines()
                    if re.match(r'^(raw|\d+)\.hta:', line)]
            self.assertEqual(len(plan), 5)
            code, text = self.terminal_run('--metric', *options, str(self.case))
            self.assertEqual(code, 0, text)
            self.assertIn('\x1b[2K', text)
            for line in plan:
                self.assertIn('  ' + line + '\r\n', text)
            self.assertLess(text.index('[needs repair]'), text.index('  raw.hta:'))
            self.assertLess(text.index('  raw.hta:'), text.index('Summary:'))
            self.assertEqual(snapshot(self.case), before)

    def test_terminal_verbose_and_recovery_hints(self):
        for verbose in (False, True):
            with self.subTest(verbose=verbose):
                self.case = pathlib.Path(tempfile.mkdtemp(dir=self.root)) / 'metric'
                shutil.copytree(self.clean, self.case)
                self.truncate('1000.hta', ROW)
                options = ('--verbose',) if verbose else ()
                code, text = self.terminal_run('--metric', '--apply', *options, str(self.case))
                self.assertEqual(code, 0, text)
                self.assertIn('Original suffixes saved in', text)
                self.assertEqual('Raw records read' in text, verbose)
                self.assertEqual('1000.hta: keep' in text, verbose)
                self.assertEqual(hashes(self.case), hashes(self.oracle))
                code, text = self.terminal_run('--metric', '--rollback', str(self.case))
                self.assertEqual(code, 0, text)
                self.assertIn('Original file suffixes restored; archive', text)
                self.assertIn('before another repair.', text)

    def test_control_characters_in_paths_are_escaped_in_logs_and_terminal(self):
        name = 'metric\n[unchanged] forged\r\t\x1b[2J\\n\x7f'
        escaped = r'metric\n[unchanged] forged\r\t\x1b[2J\\n\x7f'
        renamed = self.case.parent / name
        self.case.rename(renamed)
        self.case = renamed
        before = snapshot(self.case)
        # Discover this name via the DB root: Valgrind embeds argv in its XML
        # without escaping ESC bytes, which would invalidate its own report.
        # The actual fsck paths/output still contain the adversarial filename.
        result = run(FSCK, str(self.case.parent))
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn(escaped, result.stdout)
        self.assertNotIn(name, result.stdout + result.stderr)
        self.assertNotIn('\x1b', result.stdout + result.stderr)
        self.assertEqual(sum(line.startswith('[unchanged]')
                             for line in result.stdout.splitlines()), 1)
        code, text = self.terminal_run(str(self.case.parent))
        self.assertEqual(code, 0, text)
        self.assertIn(escaped, text)
        self.assertNotIn('\x1b[2J', text)
        self.assertNotIn('\n[unchanged] forged', text)
        self.assertEqual(snapshot(self.case), before)
        # Newline/CR/tab remain adversarial in log output but are legal XML
        # characters in argv; cover the --exclude output path separately.
        excluded_name = 'excluded\n[unchanged] forged\r\t\\n\x7f'
        excluded_escaped = r'excluded\n[unchanged] forged\r\t\\n\x7f'
        excluded_db = pathlib.Path(tempfile.mkdtemp(dir=self.root))
        (excluded_db / excluded_name).mkdir()
        result = run(FSCK, '--exclude', excluded_name, str(excluded_db))
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn('Excluded directory: ' + str(excluded_db / excluded_escaped), result.stdout)
        self.assertNotIn('\n[unchanged] forged', result.stdout)
        # Warnings and repair/rollback hints must use the same escaping.
        orphan = self.case / '.hta-fsck-stage-\n[unchanged] forged'
        orphan.mkdir()
        self.truncate('1000.hta', ROW)
        result = run(FSCK, '--apply', str(self.case.parent))
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn(r'.hta-fsck-stage-\n[unchanged] forged', result.stderr)
        self.assertIn('Original suffixes saved in ' + str(self.case.parent / escaped), result.stdout)
        result = run(FSCK, '--rollback', str(self.case.parent))
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn('Original file suffixes restored; archive ' +
                      str(self.case.parent / escaped), result.stdout)
        (self.case / 'raw.hta').write_bytes(b'broken')
        result = run(FSCK, str(self.case.parent))
        self.assertEqual(result.returncode, 1)
        self.assertIn(escaped, result.stderr)
        self.assertNotIn('\n[unchanged] forged', result.stderr)
        self.assertNotIn('\x1b', result.stderr)

    def test_preflight_errors_escape_paths_without_progress_object(self):
        name = 'bad\n[unchanged] forged\x1b[2J.hta'
        db = pathlib.Path(tempfile.mkdtemp(dir=self.root))
        (db / name).write_bytes(b'broken')
        before = snapshot(db)
        result = run(FSCK, '--apply', str(db))
        self.assertEqual(result.returncode, 1)
        self.assertIn(r'bad\n[unchanged] forged\x1b[2J.hta', result.stderr)
        self.assertEqual(len(result.stderr.splitlines()), 1)
        self.assertNotIn('\x1b', result.stderr)
        code, text = self.terminal_run('--apply', str(db))
        self.assertEqual(code, 1, text)
        self.assertNotIn('\x1b', text)
        self.assertNotIn('\n[unchanged] forged', text)
        self.assertEqual(snapshot(db), before)

    def test_terminal_failure_warning_and_empty_database(self):
        orphan = self.case / '.hta-fsck-stage-orphan'
        orphan.mkdir()
        code, text = self.terminal_run('--metric', str(self.case))
        self.assertEqual(code, 0, text)
        self.assertIn('orphan staging entry', text)
        self.assertIn('inspect/archive', text)
        (self.case / 'raw.hta').write_bytes(b'broken')
        code, text = self.terminal_run('--metric', str(self.case))
        self.assertEqual(code, 1, text)
        self.assertIn('[FAILED]', text)
        self.assertIn('truncated HTA header', text)
        empty = pathlib.Path(tempfile.mkdtemp(dir=self.root))
        code, text = self.terminal_run(str(empty))
        self.assertEqual(code, 0, text)
        self.assertIn('100% 0/0 ETA 0m 0s', text)

    def test_plain_logs_for_pipes_and_dumb_terminal(self):
        before = snapshot(self.case)
        result = run(FSCK, '--metric', '--apply', str(self.case))
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertNotIn('\x1b', result.stdout + result.stderr)
        self.assertNotIn('\r', result.stdout + result.stderr)
        self.assertIn('[unchanged] ' + str(self.case), result.stdout)
        code, text = self.terminal_run('--metric', str(self.case), term='dumb')
        self.assertEqual(code, 0, text)
        self.assertNotIn('\x1b', text)
        self.assertNotIn('ETA', text)
        self.assertIn('Raw records read', text)
        self.assertEqual(snapshot(self.case), before)

    def test_narrow_terminal_live_lines_do_not_wrap(self):
        code, text = self.terminal_run('--metric', str(self.case), width=35)
        self.assertEqual(code, 0, text)
        for current, bar in re.findall(r'(Current:[^\r\n]*)\r?\n([^\r\n]*)', text):
            self.assertLessEqual(len(current), 34)
            self.assertLessEqual(len(bar), 34)
        self.assertIn('100% 1/1 ETA 0m 0s', text)

    def test_concurrent_fsck_is_refused(self):
        before = hashes(self.case)
        fd = os.open(self.case, os.O_RDONLY | os.O_DIRECTORY)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            result = run(FSCK, '--metric', '--apply', str(self.case))
            self.assertEqual(result.returncode, 1)
            self.assertIn('another fsck', result.stderr)
            self.assertEqual(hashes(self.case), before)
        finally:
            os.close(fd)

    def test_zero_and_single_point_metrics(self):
        for count in (0, 1):
            with self.subTest(count=count):
                metric = pathlib.Path(tempfile.mkdtemp(dir=self.root)) / 'tiny'
                subprocess.run([FIXTURE, str(metric), str(count)], check=True)
                before = hashes(metric)
                with (metric / 'raw.hta').open('ab') as f:
                    f.write(b'incomplete')
                result = run(FSCK, '--metric', '--apply', str(metric))
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(hashes(metric), before)

    def test_nonmonotonic_raw_tail_no_writes(self):
        p = self.case / 'raw.hta'
        data = bytearray(p.read_bytes())
        data[-16:-8] = data[-32:-24]  # Duplicate final timestamp, complete record.
        p.write_bytes(data + b'partial')
        before = hashes(self.case)
        result = run(FSCK, '--metric', '--apply', str(self.case))
        self.assertEqual(result.returncode, 1)
        self.assertEqual(hashes(self.case), before)

    def test_full_refuses_bad_old_raw_value(self):
        p = self.case / 'raw.hta'
        data = bytearray(p.read_bytes())
        struct.pack_into('=d', data, HEADER + 100 * 16 + 8, float('inf'))
        p.write_bytes(data)
        before = hashes(self.case)
        result = run(FSCK, '--metric', '--full', '--apply', str(self.case))
        self.assertEqual(result.returncode, 1)
        self.assertEqual(hashes(self.case), before)

    def test_corrupt_metadata_no_writes(self):
        p = self.case / 'raw.hta'
        original = p.read_bytes()
        for offset, value in ((64, 1), (56, 0), (72, 2**63 - 1)):
            with self.subTest(offset=offset):
                data = bytearray(original)
                struct.pack_into('=q', data, offset, value)
                p.write_bytes(data + b'partial')
                before = hashes(self.case)
                result = run(FSCK, '--metric', '--apply', str(self.case))
                self.assertEqual(result.returncode, 1)
                self.assertEqual(hashes(self.case), before)

    def test_unchanged_files_and_prefix_preserved(self):
        levels = sorted((p.name for p in self.clean.glob('*.hta') if p.name != 'raw.hta'),
                        key=lambda name: int(pathlib.Path(name).stem))
        self.assertEqual(levels, ['1000.hta', '10000.hta', '100000.hta', '1000000.hta'])
        for level_index, name in enumerate(levels):
            for full in (False, True):
                with self.subTest(level=name, full=full):
                    # Each case loses precisely ONE complete final aggregate,
                    # with intact raw data and every other level intact.
                    self.case = pathlib.Path(tempfile.mkdtemp(dir=self.root)) / 'metric'
                    shutil.copytree(self.clean, self.case)
                    original = (self.case / name).read_bytes()
                    self.assertGreaterEqual(len(original), HEADER + ROW * 2)
                    prefix = original[:-ROW]
                    before = snapshot(self.case)
                    self.truncate(name, ROW)
                    self.assertEqual((self.case / name).read_bytes(), prefix)
                    damaged = snapshot(self.case)
                    for other in ['raw.hta', *levels]:
                        if other != name:
                            self.assertEqual(damaged[other], before[other], other)

                    # Also checks all repaired bytes/hashes against hta_repair,
                    # recovery journal contents, and repeat-run idempotence.
                    self.apply(*(('--full',) if full else ()))
                    repaired = (self.case / name).read_bytes()
                    self.assertEqual(repaired[:len(prefix)], prefix)
                    self.assertEqual(repaired, original)
                    self.assertEqual((self.case / name).stat().st_ino, before[name][0])
                    after = snapshot(self.case)
                    # Repair must not modify raw data or any lower-level file,
                    # including their inode, size, mode and modification time.
                    for unchanged in ['raw.hta', *levels[:level_index]]:
                        self.assertEqual(after[unchanged], before[unchanged], unchanged)

    def fault(self, suffix, crash=False):
        env = dict(os.environ, LD_PRELOAD=FAULT, FSCK_TEST_FAIL_SUFFIX=suffix)
        if crash:
            env['FSCK_TEST_CRASH'] = '1'
        return subprocess.run([FSCK, '--metric', '--apply', str(self.case)], env=env,
                              text=True, capture_output=True, timeout=30)

    def test_enospc_before_commit_keeps_originals(self):
        self.truncate('1000.hta', ROW * 3 + 1)
        before = hashes(self.case)
        for suffix in ('.after', '.before', 'manifest.tmp'):
            with self.subTest(suffix=suffix):
                result = self.fault(suffix)
                self.assertEqual(result.returncode, 1)
                self.assertEqual(hashes(self.case), before)
                self.assertFalse((self.case / '.hta-fsck-recovery').exists())

    def test_partial_commit_enospc_and_crash_are_recoverable(self):
        for crash in (False, True):
            with self.subTest(crash=crash):
                for name in ('1000.hta', '10000.hta'):
                    (self.case / name).write_bytes((self.clean / name).read_bytes()[:-ROW * 2])
                before = hashes(self.case)
                result = self.fault('/10000.hta', crash=crash)
                self.assertEqual(result.returncode, 99 if crash else 1, result.stderr)
                self.assertNotEqual(hashes(self.case), before)
                refused = run(FSCK, '--metric', '--apply', str(self.case))
                self.assertEqual(refused.returncode, 1)
                result = run(FSCK, '--metric', '--rollback', str(self.case))
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(hashes(self.case), before)
                self.archive_backup(f'backup-crash-{crash}')
                self.apply()
                self.archive_backup(f'backup-retry-{crash}')

    def test_every_partial_raw_length(self):
        for length in range(1, 16):
            with self.subTest(length=length):
                p = self.case / 'raw.hta'
                with p.open('ab') as f:
                    f.write(bytes(range(length)))
                before = hashes(self.case)
                dry = run(FSCK, '--metric', str(self.case))
                self.assertEqual(dry.returncode, 0, dry.stderr)
                self.assertIn('1000000.hta', dry.stdout)
                self.assertEqual(hashes(self.case), before)
                self.apply()
                # Retained recovery records are archived by the caller before another incident.
                backup = self.case / '.hta-fsck-recovery'
                if backup.exists():
                    backup.rename(self.case / f'backup-{length}')

    def test_missing_and_truncated_headers(self):
        (self.case / '1000.hta').unlink()
        (self.case / '10000.hta').write_bytes(b'HTA')
        (self.case / '100000.hta').write_bytes(b'')
        self.apply()

    def test_missing_empty_levels_and_short_healthy_metrics(self):
        for count in (0, 1, 2, 5, 30, 300, 3000):
            with self.subTest(count=count):
                metric = pathlib.Path(tempfile.mkdtemp(dir=self.root)) / 'short'
                subprocess.run([FIXTURE, str(metric), str(count)], check=True)
                reference = metric.parent / 'reference'
                shutil.copytree(metric, reference)
                # Legacy hta_repair divides by the time range in its progress
                # display; for 0/1 samples use the normal writer as the oracle.
                if count > 1:
                    result = run(REPAIR, str(reference))
                    self.assertEqual(result.returncode, 0, result.stderr)
                expected = hashes(reference)
                before = snapshot(metric)
                for options in (('--apply',), ('--full', '--apply')):
                    result = run(FSCK, '--metric', *options, str(metric))
                    self.assertEqual(result.returncode, 0, result.stderr)
                    self.assertEqual(snapshot(metric), before)
                missing = [p for p in metric.glob('*.hta')
                           if p.name != 'raw.hta' and p.stat().st_size == HEADER]
                if count == 5:
                    self.assertEqual([p.name for p in missing], ['10000.hta'])
                for p in missing:
                    p.unlink()
                damaged = hashes(metric)
                plan = run(FSCK, '--metric', str(metric))
                self.assertEqual(plan.returncode, 0, plan.stderr)
                self.assertEqual(hashes(metric), damaged)
                if missing:
                    self.assertIn('1 need repair', plan.stdout)
                result = run(FSCK, '--metric', '--apply', str(metric))
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(hashes(metric), expected)
                for name in expected:
                    self.assertEqual((metric / name).read_bytes(), (reference / name).read_bytes())
                after = snapshot(metric)
                self.assertEqual(run(FSCK, '--metric', '--apply', str(metric)).returncode, 0)
                self.assertEqual(snapshot(metric), after)
                if missing:
                    self.assertEqual(run(FSCK, '--metric', '--rollback', str(metric)).returncode, 0)
                    self.assertEqual(hashes(metric), damaged)

    def test_interrupted_rollback_blocks_checks_until_resumed(self):
        for crash in (False, True):
            with self.subTest(crash=crash):
                self.case = pathlib.Path(tempfile.mkdtemp(dir=self.root)) / 'metric'
                shutil.copytree(self.clean, self.case)
                p = self.case / '1000.hta'
                data = bytearray(p.read_bytes())
                struct.pack_into('=Q', data, HEADER + 17 * ROW + 32, 999999)
                p.write_bytes(data)
                before = hashes(self.case)
                self.apply('--full')
                env = dict(os.environ, LD_PRELOAD=FAULT, FSCK_TEST_FAIL_SUFFIX='/1000.hta')
                if crash:
                    env['FSCK_TEST_CRASH'] = '1'
                result = subprocess.run([FSCK, '--metric', '--rollback', str(self.case)], env=env,
                                        text=True, capture_output=True, timeout=30)
                self.assertEqual(result.returncode, 99 if crash else 1, result.stderr)
                journal = self.case / '.hta-fsck-recovery' / 'manifest.json'
                self.assertEqual(json.loads(journal.read_text())['state'], 'rolling-back')
                # Corruption has already been restored before the injected failure.
                self.assertEqual(struct.unpack_from('=Q', p.read_bytes(), HEADER + 17 * ROW + 32)[0],
                                 999999)
                interrupted = snapshot(self.case)
                for options in ((), ('--apply',), ('--full',), ('--full', '--apply')):
                    result = run(FSCK, '--metric', *options, str(self.case))
                    self.assertEqual(result.returncode, 1)
                    self.assertIn('unfinished recovery', result.stderr)
                    self.assertEqual(snapshot(self.case), interrupted)
                result = run(FSCK, '--metric', '--rollback', str(self.case))
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(hashes(self.case), before)
                self.assertEqual(json.loads(journal.read_text())['state'], 'rolled-back')
                self.archive_backup(f'rollback-failed-{crash}')
                self.apply('--full')
                self.archive_backup(f'rollback-retry-{crash}')

    def test_rollback_state_write_failure_keeps_repaired_files(self):
        self.truncate('1000.hta', ROW)
        self.apply()
        before = hashes(self.case)
        env = dict(os.environ, LD_PRELOAD=FAULT, FSCK_TEST_FAIL_SUFFIX='manifest.tmp')
        result = subprocess.run([FSCK, '--metric', '--rollback', str(self.case)], env=env,
                                text=True, capture_output=True, timeout=30)
        self.assertEqual(result.returncode, 1)
        self.assertEqual(hashes(self.case), before)
        journal = self.case / '.hta-fsck-recovery' / 'manifest.json'
        self.assertEqual(json.loads(journal.read_text())['state'], 'complete')

    def test_finite_wrong_tail_counts_and_sum(self):
        for name in ('1000.hta', '10000.hta'):
            p = self.case / name
            data = bytearray(p.read_bytes())
            # Last three rows: finite sum/count corruption passes the old valid().
            for offset in range(len(data) - 3 * ROW, len(data), ROW):
                struct.pack_into('=dQ', data, offset + 24, 123.0, 999999)
            p.write_bytes(data)
        self.apply()

    def test_lower_damage_propagates_past_matching_upper_tail(self):
        p = self.case / '1000.hta'
        data = bytearray(p.read_bytes())
        # A long damaged lower suffix overlaps several high-level intervals.
        start = len(data) - ROW * 2800
        time = struct.unpack_from('=q', data, start)[0]
        for offset in range(start, len(data), ROW):
            struct.pack_into('=Q', data, offset + 32, 999999)
        p.write_bytes(data)
        p = self.case / '1000000.hta'
        data = bytearray(p.read_bytes())
        epoch = struct.unpack_from('=q', data, HEADER)[0]
        index = (time // 1000000 * 1000000 - epoch) // 1000000
        self.assertLess(HEADER + (index + 1) * ROW, len(data) - ROW)
        struct.pack_into('=d', data, HEADER + index * ROW + 24, 123.0)
        # The last upper row remains correct: only dependency propagation finds this.
        p.write_bytes(data)
        self.apply()

    def test_bogus_timestamp_tail(self):
        p = self.case / '1000.hta'
        data = bytearray(p.read_bytes())
        for offset in range(len(data) - 5 * ROW, len(data), ROW):
            struct.pack_into('=q', data, offset, 0)
        p.write_bytes(data)
        self.apply()

    def test_extra_complete_rows(self):
        p = self.case / '1000.hta'
        with p.open('ab') as f:
            f.write(p.read_bytes()[-ROW:] * 3)
        self.apply()

    def test_rebuild_first_interval(self):
        for p in self.case.glob('*.hta'):
            if p.name != 'raw.hta':
                p.write_bytes(p.read_bytes()[:HEADER])
        self.apply()

    def test_invalid_raw_header_no_writes(self):
        p = self.case / 'raw.hta'
        data = bytearray(p.read_bytes())
        data[0] = 0
        p.write_bytes(data + b'partial')
        before = hashes(self.case)
        result = run(FSCK, '--metric', '--apply', str(self.case))
        self.assertEqual(result.returncode, 1)
        self.assertEqual(hashes(self.case), before)

    def test_extended_raw_header_no_destructive_guess(self):
        p = self.case / 'raw.hta'
        data = bytearray(p.read_bytes())
        struct.pack_into('=Q', data, 16, 64)
        p.write_bytes(data[:HEADER] + b'extended' + data[HEADER:])
        before = hashes(self.case)
        result = run(FSCK, '--metric', '--apply', str(self.case))
        self.assertEqual(result.returncode, 1)
        self.assertEqual(hashes(self.case), before)

    def test_raw_header_size_version_and_period_are_validated(self):
        path = self.case / 'raw.hta'
        original = path.read_bytes()
        for offset, value, message in ((16, 0, 'unsupported HTA header size'),
                                       (16, 55, 'unsupported HTA header size'),
                                       (16, 2**64 - 1, 'unsupported HTA header size'),
                                       (24, 3, 'Unsupported HTA file format version'),
                                       (40, 2, 'Unsupported HTA chrono duration period')):
            with self.subTest(offset=offset, value=value):
                data = bytearray(original)
                struct.pack_into('=Q', data, offset, value)
                path.write_bytes(data)
                before = snapshot(self.case)
                result = run(FSCK, '--metric', '--apply', str(self.case))
                self.assertEqual(result.returncode, 1)
                self.assertIn(message, result.stderr)
                self.assertEqual(snapshot(self.case), before)

    def test_invalid_raw_tail_no_writes(self):
        p = self.case / 'raw.hta'
        data = bytearray(p.read_bytes())
        struct.pack_into('=d', data, len(data)-8, float('nan'))
        p.write_bytes(data)
        self.truncate('1000.hta', ROW)
        before = hashes(self.case)
        result = run(FSCK, '--metric', '--apply', str(self.case))
        self.assertEqual(result.returncode, 1)
        self.assertEqual(hashes(self.case), before)

    def test_rollback_restores_corrupted_input_exactly(self):
        self.truncate('1000.hta', ROW + 3)
        (self.case / '10000.hta').unlink()
        with (self.case / 'raw.hta').open('ab') as f:
            f.write(b'partial')
        before = hashes(self.case)
        self.apply()
        result = run(FSCK, '--metric', '--rollback', str(self.case))
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(hashes(self.case), before)


class Harness(unittest.TestCase):
    def test_signals_are_never_expected_failures(self):
        result = subprocess.CompletedProcess([], -11, '', '')
        with self.assertRaisesRegex(AssertionError, 'signal 11'):
            check_process(result)

    def test_valgrind_xml_checked_even_with_zero_exit(self):
        with tempfile.TemporaryDirectory() as directory:
            report = pathlib.Path(directory) / 'report.xml'
            result = subprocess.CompletedProcess([], 0, '', '')
            for body in ('<error><kind>InvalidWrite</kind></error>',
                         '<error><kind>Leak_DefinitelyLost</kind></error>'):
                report.write_text('<valgrindoutput><status><state>FINISHED</state></status>'
                                  + body + '</valgrindoutput>')
                with self.assertRaisesRegex(AssertionError, 'Valgrind detected errors'):
                    check_process(result, report)
            report.write_text('<valgrindoutput><status><state>RUNNING</state></status></valgrindoutput>')
            with self.assertRaises(AssertionError):
                check_process(result, report)
            report.write_text('<valgrindoutput>')
            with self.assertRaises(AssertionError):
                check_process(result, report)
            report.unlink()
            with self.assertRaises(AssertionError):
                check_process(result, report)

    @unittest.skipUnless(shutil.which('valgrind'), 'Valgrind not installed')
    def test_actual_invalid_write_and_sigsegv_are_rejected(self):
        with self.assertRaisesRegex(AssertionError, 'signal 11'):
            run(FIXTURE, '--crash', valgrind=shutil.which('valgrind'))


if __name__ == '__main__':
    unittest.main()
