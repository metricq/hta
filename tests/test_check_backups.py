"""Synthetic, read-only backup comparison tests; no HTA build required."""
import importlib.util
import json
import pathlib
import struct
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

SCRIPT = pathlib.Path(__file__).resolve().parents[1] / 'helpers' / 'check_backups.py'
SPEC = importlib.util.spec_from_file_location('check_backups', SCRIPT)
check = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(check)


def header(endian='<', factor=10):
    return check.MAGIC + struct.pack(endian + 'QQQqQQqqq', check.BOM, 56, 2, 0,
                                   1, 1000000000, 1000, factor, 1000000)


class BackupCheck(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix='hta-backup-check-')
        self.addCleanup(self.temporary.cleanup)
        self.root = pathlib.Path(self.temporary.name)

    def metric(self, name, count=1000, tail=b'', endian='<'):
        directory = self.root / name
        directory.mkdir()
        data = header(endian) + b''.join(struct.pack(endian + 'qd', i + 1, i * 0.5)
                                        for i in range(count)) + tail
        (directory / 'raw.hta').write_bytes(data)
        return directory

    def pair(self, **kwargs):
        return check.compare_pair(self.root / 'm.backup-123', self.root / 'm', **kwargs)

    def cli(self, *args):
        return subprocess.run([sys.executable, str(SCRIPT), str(self.root), *args],
                              capture_output=True, text=True)

    def test_identical_and_no_writes(self):
        self.metric('m.backup-123')
        self.metric('m')
        before = {p: (p.read_bytes(), check.stamp(p.stat()))
                  for p in self.root.rglob('raw.hta')}
        self.assertEqual(self.pair()['status'], 'MATCH_SAMPLED')
        self.assertEqual(self.pair(full=True)['status'], 'MATCH_FULL')
        self.assertEqual(self.cli().returncode, 0)
        after = {p: (p.read_bytes(), check.stamp(p.stat()))
                 for p in self.root.rglob('raw.hta')}
        self.assertEqual(before, after)
        self.assertEqual(len(list(self.root.rglob('*'))), 4)

    def test_grown_and_shorter(self):
        backup = self.metric('m.backup-123', count=1000)
        current = self.metric('m', count=1200)
        result = self.pair()
        self.assertEqual(result['status'], 'CURRENT_GROWN')
        self.assertEqual(result['delta_points'], 200)
        result = check.compare_pair(current, backup)
        self.assertEqual(result['status'], 'CURRENT_SHORTER')
        self.assertEqual(result['delta_points'], -200)

    def test_sampled_corruption_at_beginning_middle_and_end(self):
        self.metric('m.backup-123', count=10000)
        current = self.metric('m', count=10000) / 'raw.hta'
        original = current.read_bytes()
        for index in (0, 5000, 9999):
            with self.subTest(index=index):
                data = bytearray(original)
                struct.pack_into('<d', data, 80 + index * 16 + 8, -99)
                current.write_bytes(data)
                self.assertEqual(self.pair()['status'], 'DIFFERENT')

    def test_full_detects_change_between_samples(self):
        self.metric('m.backup-123', count=10000)
        current = self.metric('m', count=10000) / 'raw.hta'
        with current.open('r+b') as stream:
            stream.seek(80 + 500 * 16 + 8)
            stream.write(struct.pack('<d', -99))
        self.assertEqual(self.pair()['status'], 'MATCH_SAMPLED')
        self.assertEqual(self.pair(full=True)['status'], 'DIFFERENT')

    def test_partial_tails(self):
        self.metric('m.backup-123')
        current = self.metric('m') / 'raw.hta'
        data = current.read_bytes()
        for size in range(1, 16):
            current.write_bytes(data + b'x' * size)
            self.assertEqual(self.pair()['status'], 'PARTIAL')
        self.assertEqual(self.cli().returncode, 1)

    def test_empty_and_missing(self):
        self.metric('m.backup-123', count=0)
        self.assertEqual(self.pair()['status'], 'ERROR')
        self.metric('m', count=0)
        self.assertEqual(self.pair()['status'], 'EMPTY_BACKUP')

    def test_malformed_and_different_headers(self):
        self.metric('m.backup-123')
        path = self.metric('m') / 'raw.hta'
        original = path.read_bytes()
        for malformed in (b'', original[:79], b'BADMAGIC' + original[8:],
                          original[:16] + struct.pack('<Q', 57) + original[24:]):
            path.write_bytes(malformed)
            self.assertEqual(self.pair()['status'], 'ERROR')
        path.write_bytes(header(factor=20) + original[80:])
        result = self.pair(full=True)
        self.assertEqual(result['status'], 'DIFFERENT')
        self.assertEqual(result['compared_bytes_per_file'], 0)

    def test_big_endian(self):
        self.metric('m.backup-123', endian='>')
        self.metric('m', endian='>')
        self.assertEqual(self.pair(full=True)['status'], 'MATCH_FULL')

    def test_discovery_json_and_escaped_names(self):
        name = 'm\n[FAKE]\x1b'
        for suffix in ('', '.backup-1', '.backup-2', '.backup-1.backup-3'):
            self.metric(name + suffix)
        self.metric('unrelated')
        self.metric('ignored.backup-text')
        result = self.cli('--jsonl')
        self.assertEqual(result.returncode, 0, result.stderr)
        rows = [json.loads(line) for line in result.stdout.splitlines()]
        self.assertEqual(len(rows), 3)
        self.assertEqual({row['metric'] for row in rows}, {name, name + '.backup-1'})
        self.assertNotIn('\x1b', self.cli().stdout)
        self.assertEqual(len(self.cli().stdout.splitlines()), 3)
        self.assertEqual(self.cli('--only-problems').stdout, '')

    def test_no_backups_and_invalid_samples(self):
        self.assertEqual(self.cli().returncode, 2)
        self.assertEqual(self.cli('--samples', '1').returncode, 2)

    def test_symlinks_refused(self):
        backup = self.metric('m.backup-123')
        (self.root / 'm').symlink_to(backup, target_is_directory=True)
        self.assertEqual(self.pair()['status'], 'ERROR')
        (self.root / 'm').unlink()
        (self.root / 'm').mkdir()
        (self.root / 'm' / 'raw.hta').symlink_to(backup / 'raw.hta')
        self.assertEqual(self.pair()['status'], 'ERROR')

    def test_concurrent_append_detected(self):
        self.metric('m.backup-123')
        path = self.metric('m') / 'raw.hta'
        original_compare = check.compare_data

        def append_after_compare(*args):
            result = original_compare(*args)
            with path.open('ab') as stream:
                stream.write(struct.pack('<qd', 1001, 3.0))
            return result

        with mock.patch.object(check, 'compare_data', side_effect=append_after_compare):
            self.assertEqual(self.pair()['status'], 'CHANGED_DURING_CHECK')

    def test_sparse_offsets_above_32_bit_record_indices(self):
        count = 2**32 + 1000
        for name in ('m', 'm.backup-123'):
            path = self.metric(name, count=1) / 'raw.hta'
            with path.open('r+b') as stream:
                stream.truncate(80 + count * 16)
                stream.seek(80 + (count - 1) * 16)
                stream.write(struct.pack('<qd', count, 1.0))
        result = self.pair()
        self.assertEqual(result['status'], 'MATCH_SAMPLED')
        self.assertEqual(result['backup_raw']['points'], count)
        self.assertLessEqual(result['compared_bytes_per_file'], 9 * 4096)
        with (self.root / 'm' / 'raw.hta').open('r+b') as stream:
            stream.seek(80 + (count - 1) * 16 + 8)
            stream.write(struct.pack('<d', 99))
        self.assertEqual(self.pair()['status'], 'DIFFERENT')


if __name__ == '__main__':
    unittest.main()
