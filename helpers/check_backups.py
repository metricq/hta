#!/usr/bin/env python3
"""Read-only raw.hta comparison of METRIC.backup-DIGITS against METRIC."""
import argparse
import collections
import datetime
import json
import math
import os
import pathlib
import re
import stat
import struct
import sys

MAGIC = b'HTA\x1a\xc5\x2c\xcc\x1d'
BOM = 0xf8f9fafbfcfdfeff
HEADER = 80
RECORD = 16
BLOCK = 4096
BACKUP = re.compile(r'^(.+)\.backup-([0-9]+)$', re.DOTALL)
GOOD = {'MATCH_SAMPLED', 'MATCH_FULL', 'CURRENT_GROWN'}


def read_at(fd, size, offset):
    data = bytearray()
    while len(data) < size:
        chunk = os.pread(fd, size - len(data), offset + len(data))
        if not chunk:
            raise ValueError(f'short read at byte {offset + len(data)}')
        data.extend(chunk)
    return bytes(data)


def stamp(info):
    return info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns


def utc(timestamp):
    if timestamp is None:
        return None
    seconds, nanos = divmod(timestamp, 1000000000)
    date = datetime.datetime.fromtimestamp(seconds, datetime.timezone.utc)
    return date.strftime('%Y-%m-%dT%H:%M:%S') + f'.{nanos:09d}Z'


class Raw:
    def __init__(self, directory):
        if directory.is_symlink():
            raise ValueError(f'symlink metric directory refused: {directory}')
        self.path = directory / 'raw.hta'
        self.fd = os.open(self.path, os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW | os.O_NONBLOCK)
        try:
            self.initial = os.fstat(self.fd)
            if not stat.S_ISREG(self.initial.st_mode):
                raise ValueError(f'not a regular file: {self.path}')
            self.header = read_at(self.fd, HEADER, 0)
            if self.header[:8] != MAGIC:
                raise ValueError(f'invalid HTA magic: {self.path}')
            self.endian = next((e for e in ('<', '>')
                                if self.header[8:16] == struct.pack(e + 'Q', BOM)), None)
            if self.endian is None:
                raise ValueError(f'unsupported byte order: {self.path}')
            size, version, interval, num, den, minimum, factor, maximum = struct.unpack(
                self.endian + 'QQqQQqqq', self.header[16:])
            if (size, version, interval, num, den) != (56, 2, 0, 1, 1000000000):
                raise ValueError(f'unsupported raw header (requires HTA v2, 56-byte header): {self.path}')
            if minimum <= 0 or factor <= 1 or maximum < minimum:
                raise ValueError(f'invalid aggregation metadata: {self.path}')
            self.count, self.partial = divmod(self.initial.st_size - HEADER, RECORD)
            self.first = self.point_time(0) if self.count else None
            self.last = self.point_time(self.count - 1) if self.count else None
            if self.count > 1 and self.first >= self.last:
                raise ValueError(f'invalid first/last timestamp range: {self.path}')
        except BaseException:
            os.close(self.fd)
            raise

    def point_time(self, index):
        timestamp, value = struct.unpack(self.endian + 'qd', read_at(self.fd, RECORD, HEADER + index * RECORD))
        if timestamp <= 0 or not math.isfinite(value):
            raise ValueError(f'invalid endpoint record {index}: {self.path}')
        return timestamp

    def stable(self):
        return (stamp(self.initial) == stamp(os.fstat(self.fd)) ==
                stamp(os.stat(self.path, follow_symlinks=False)))

    def close(self):
        os.close(self.fd)

    def info(self):
        return {'bytes': self.initial.st_size, 'points': self.count, 'partial_bytes': self.partial,
                'first_ns': self.first, 'last_ns': self.last,
                'first_utc': utc(self.first), 'last_utc': utc(self.last)}


def sample_windows(count, samples):
    """Aligned, bounded windows; include beginning and end of the common prefix."""
    width = min(count, BLOCK // RECORD)
    if not width:
        return []
    return [(start, width) for start in sorted({i * (count - width) // (samples - 1)
                                               for i in range(samples)})]


def compare_data(backup, current, samples, full):
    count = min(backup.count, current.count)
    if full:
        step = 1024 * 1024 // RECORD
        windows = ((start, min(step, count - start)) for start in range(0, count, step))
    else:
        windows = sample_windows(count, samples)
    compared = 0
    for start, length in windows:
        offset, size = HEADER + start * RECORD, length * RECORD
        left = read_at(backup.fd, size, offset)
        right = read_at(current.fd, size, offset)
        compared += size
        if left != right:
            return False, compared, start
    return True, compared, None


def compare_pair(backup_dir, current_dir, samples=9, full=False):
    result = {'backup': backup_dir.name, 'metric': current_dir.name,
              'comparison': 'full-prefix' if full else 'sampled-prefix'}
    opened = []
    try:
        backup = Raw(backup_dir)
        opened.append(backup)
        current = Raw(current_dir)
        opened.append(current)
        result.update(backup_raw=backup.info(), current_raw=current.info(),
                      delta_points=current.count - backup.count)
        equal, compared, mismatch = (compare_data(backup, current, samples, full)
                                    if backup.header == current.header else (False, 0, None))
        result['compared_bytes_per_file'] = compared
        if backup.header != current.header:
            status, detail = 'DIFFERENT', 'raw headers differ'
        elif not equal:
            status, detail = 'DIFFERENT', f'raw bytes differ in block starting at record {mismatch}'
        elif not backup.count:
            status, detail = 'EMPTY_BACKUP', 'backup contains no complete raw points; success cannot be confirmed'
        elif current.count < backup.count:
            status, detail = 'CURRENT_SHORTER', 'current metric has fewer raw points than backup; repair may be incomplete'
        elif current.count > backup.count:
            status, detail = 'CURRENT_GROWN', 'common prefix matches; current metric has additional raw points'
        else:
            status = 'MATCH_FULL' if full else 'MATCH_SAMPLED'
            detail = 'equal raw point counts and matching compared bytes'
        partials = backup.partial or current.partial
        if partials:
            detail += f'; incomplete raw tails: backup={backup.partial}, current={current.partial} bytes'
            if status in GOOD:
                status = 'PARTIAL'
        result.update(status=status, detail=detail)
        if not all(raw.stable() for raw in opened):
            result.update(status='CHANGED_DURING_CHECK', detail='file changed or was replaced; rerun on a stable snapshot')
    except (OSError, ValueError) as error:
        result.update(status='ERROR', detail=str(error))
    finally:
        for raw in opened:
            raw.close()
    return result


def positive_samples(value):
    number = int(value)
    if not 2 <= number <= 10000:
        raise argparse.ArgumentTypeError('samples must be between 2 and 10000')
    return number


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('directory', type=pathlib.Path, help='HTA database directory containing metrics and backups')
    parser.add_argument('--samples', type=positive_samples, default=9, help='4 KiB windows per common prefix (default: 9)')
    parser.add_argument('--full', action='store_true', help='compare the entire common raw prefix (reads large files completely)')
    parser.add_argument('--only-problems', action='store_true', help='hide matching/grown pairs; still include all pairs in summary')
    parser.add_argument('--jsonl', action='store_true', help='one JSON result per pair; summary remains on stderr')
    args = parser.parse_args(argv)
    try:
        backups = sorted((path for path in args.directory.iterdir()
                          if BACKUP.fullmatch(path.name) and (path.is_symlink() or path.is_dir())),
                         key=lambda path: path.name)
    except OSError as error:
        parser.error(str(error))
    if not backups:
        print('No METRIC.backup-DIGITS directories found; nothing verified.', file=sys.stderr)
        return 2
    print('Read-only. Comparing raw.hta only; metrics without backups are not checked. '
          + ('Full common-prefix comparison.' if args.full else
             f'Sampling {args.samples} x 4 KiB per file, plus header/endpoints; this is NOT proof of full equality.'),
          file=sys.stderr)
    counts = collections.Counter()
    for backup in backups:
        metric = args.directory / BACKUP.fullmatch(backup.name)[1]
        result = compare_pair(backup, metric, args.samples, args.full)
        counts[result['status']] += 1
        if args.only_problems and result['status'] in GOOD:
            continue
        if args.jsonl:
            print(json.dumps(result, ensure_ascii=True), flush=True)
        else:
            details = ''
            if 'backup_raw' in result:
                b, c = result['backup_raw'], result['current_raw']
                details = (f' points={b["points"]}->{c["points"]} delta={result["delta_points"]:+d}'
                           f' backup_end={b["last_utc"]} current_end={c["last_utc"]}')
            print(f'{result["status"]} backup={json.dumps(result["backup"])} '
                  f'metric={json.dumps(result["metric"])}{details} detail={json.dumps(result["detail"])}', flush=True)
    print(f'Summary: {len(backups)} backup pairs; ' + ', '.join(f'{key}={counts[key]}' for key in sorted(counts)),
          file=sys.stderr)
    return int(any(status not in GOOD for status in counts))


if __name__ == '__main__':
    sys.exit(main())
