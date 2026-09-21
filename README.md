![Build](https://github.com/metricq/hta/workflows/Build/badge.svg)

Hierarchical Timeline Aggregation
=================================

Hierarchical Timeline Aggregation (or short HTA) is reference library for a file format tailored towards a fast store and load of metric data.

To speed up the access, it comprises raw data as well as aggregated information organized in plain files. The raw data is stored as timestamp-value pairs. The aggregates comprise values for the count, minimum, maximum, sum and integral over a given time interval. A hierarchy organizes the aggregates, where an aggregate of one hierarchy level includes several aggregates of the subsequent level. Each hierarchy level aggregates the previous level in intervals with a constant duration within the same level. The relation between the span of the interval of two consecutive hierarchy levels is called the *interval factor*. The interval factor is constant over all levels within the hierarchy for one specific metric.  The interval duration of the first and the last hierarchy level is called *min interval* and *max interval*, respectively.


Data layout
-----------

The raw metric data comprises a timestamp and a value, where the timestamp is a 64-bit signed integer denoting the POSIX timestamp in nanoseconds. The value is represented by an IEEE-754 floating point number with 64-bit precision.

An aggregate consists of an unsigned 64-bit integer for the count and an IEEE-754 floating point number with 64-bit precision for each of min, max, sum and integral, where the integral is defined as the sum weighted according to the time a data point was valid under the assumption of a last semantic. The aggregate also contains and signed 64-bit integer representing the duration for when actual data points back the aggregate. This active time is only of interest for intervals beginning before the first raw data point; otherwise, it is equivalent to the duration of the interval.


Storing a metric in an HTA
--------------------------

### Selecting good metric names

See [the Metric page in the MetricQ Wiki](https://github.com/metricq/metricq/wiki/Metrics#selecting-good-metric-names).

### Planning the aggregation parameters

A proper selection of the interval min, interval max, and interval factor parameters plays a big roll in the balance between the spacial overhead of the aggregation and the performance of the database.

Selecting the interval factor might be the most straightforward task. While the interval factor can be any number larger than `1`, it's recommended to simply use `10`, because of the more comfortable readable interval levels.

The interval min and max parameters, however, should be selected carefully with regards to the measurement sampling rate of the raw metric and the expected duration of the measurement. Both parameters are given as a duration in nanoseconds resolution and should be a multiple of the interval factor. Note that, due to downtimes of the measurement itself, the measurement sampling rate might differ from the average sampling rate.

For the interval min parameter, which defines the length of the intervals in the first aggregation level, we recommend a value equal to 30 times the length between two measurement samples. With this value, the first level comprises one-tenth the amount of storage space than the raw data level, because the aggregation data layout is about thrice as large as a single raw data point.

For long-running metrics with a sampling rate higher than once per second, we recommend as the interval max parameter the largest value of `interval_min * interval_factor ^ n`, which is smaller than one day. For metrics with a lower sampling rate, we recommend limiting the number of levels by selecting a smaller value for the interval max parameter.

With the recommended values for interval factor and interval min, the storage overhead for the aggregation of one metric is about 11 percent in comparison to the raw storage as a 128-bit timestamp-value pair list.

For example, given a raw metric with a sampling rate of 1000 Samples per second, we'd recommend an interval min of `3e7 ns` and thus an interval max of `3e14 ns`. These settings result in an approximate data rate of 17.7 kB/s or about 560GB per year.

Repairing corrupted database
----------------------------

### Finding corrupted metrics

```hta_check``` checks a single metric for inconsistencies. With the option ```--fast``` the tool only checks the most recent data.

The helper script ```check_db_directory.sh``` checks a whole database directory with many metrics. It utilizes GNU parallel to perform the check in parallel.

```bash
./helpers/check_db_directory.sh <path to the db> [<parallel jobs count>]
```

This script outputs a file containing all corrupted metrics.

### Repairing corrupted metrics

```hta_repair``` repairs a corrupted metric. It requires the path to corrupted metric as first argument.

To repair metrics in parallel based on the file created by ```check_db_directory.sh``` run the following:

```bash
cat <output file of corrupted metrics> | parallel --load 100% --noswap --jobs <parallel jobs count> --results <folder for stdout and stderr of parallel programs> --eta <path to hta_repair, e.g. ./build/hta_repair> <path to db directory>/{}
```

### Fast offline recovery after an interrupted append: `hta_fsck`

Stop the database and all other writers before running this tool. `hta_fsck`
accepts the database directory and processes its immediate metric subdirectories
sequentially. An individual metric directory requires **`--metric`**; there is
no automatic layout detection. A `.hta` file in the database root is an error
reported before any repair starts. Passing a database root with `--metric` fails
because it has no `raw.hta`.
In database mode, entries named `<metric>.backup-<digits>` are automatically
skipped, matching the timestamped backups created by `hta_repair`. Each skip is
printed as `Skipped backup directory`; skipped entries are neither opened nor
included in progress/summary counts. This applies to checking, `--apply` and
`--rollback`. The metric prefix and numeric suffix must both be nonempty.
To intentionally check/repair/roll back one of these directories, address it
directly with `--metric`. This also handles a real metric whose name happens to
match the backup pattern.
All other immediate subdirectories are checked, including names starting with
`.hta-fsck-`. Use repeatable `--exclude NAME` for further exclusions (exact immediate
directory names, database mode only). Every exclusion is printed; nonexistent
exclusions are errors.
For a database at the root of a dedicated volume, explicitly exclude the
filesystem's `lost+found` directory with `--exclude lost+found`. It is not skipped
by name: a real metric could have the same name.
Symlink input paths (including symlink parent components) and non-skipped symlink
entries discovered in a database are refused with an explicit error. Use the
real path, or exclude an unwanted symlink entry with `--exclude NAME`; skipped
backup entries and excluded symlinks are not followed, including dangling ones.
A failure in one metric is reported and
does not prevent the remaining metrics from being checked; the overall exit code
is nonzero if any metric failed. It leaves complete raw records intact,
removes an incomplete final raw record (1–15 bytes), and rebuilds affected
aggregation suffixes from the surviving raw data. Invalid complete raw records
are reported and refused, rather than silently discarded.
The empty parent file opened by the normal writer is also recreated when missing;
unused higher levels remain absent on healthy short metrics.

```bash
hta_fsck /path/to/db                       # Read-only plan for all metrics
hta_fsck --apply /path/to/db                # Repair affected metrics
hta_fsck --full /path/to/db                 # Also check older raw/aggregation records
hta_fsck --full --apply /path/to/db
hta_fsck --apply --metric /path/to/db/metric
hta_fsck --apply --metric /path/to/db/metric.backup-12345  # Explicitly repair a backup
hta_fsck --apply --exclude lost+found /mountpoint
hta_fsck --apply --verbose --metric /path/to/db/metric
```

When stdout and stderr are terminals (and `TERM` is not `dumb`), completed
metrics remain as a list with their result. Below them, two live lines show the
current metric/phase and the overall progress bar with ETA. Processing remains
sequential. The ETA is approximate: it weights metrics equally and accounts for
phase/record progress within the current metric; differently sized metrics can
make it fluctuate. The final bar indicates processing completion, not success:
failed metrics remain marked `FAILED` and the exit status is nonzero.
With redirected output or `TERM=dumb`, ordinary per-metric/per-level log lines
are emitted without cursor movement or a live bar. Completed metric results and
the final summary are printed in both modes.
Terminal dry-runs retain the full per-level plan below each completed metric,
above the live progress lines for the next metric. `--verbose` (or `-v`) also
enables these details for terminal repair runs. Recovery/rollback journal hints
are always printed, even in the compact terminal view.
All output modes escape control bytes in paths/messages (for example, a newline
becomes the two characters `\n`). Backslashes become `\\`; other nonprintable
and non-ASCII bytes become `\xHH`. This prevents filenames from injecting extra
log lines or terminal commands, including in exclusions and preflight errors.

Healthy metrics are not rewritten: file bytes, sizes, inodes, modes and modification
times (including the metric directory's modification time) are preserved. No
staging or recovery files are created inside a healthy metric. Normal read access
may update access times according to the filesystem's mount options.

The default mode assumes an append-only failure: complete older records before
the damaged suffix are trusted. It checks the last 4096 raw records, determines
each level's expected length, and compares its last surviving complete aggregate
against independently reconstructed data. It walks backwards through mismatches
until a matching boundary is found. A changed lower interval invalidates the
overlapping upper intervals. This is not a full integrity check: an isolated
older corruption separated from the end by correct rows requires `--full`.
Neither mode can detect a plausible but incorrect raw value without an external
reference or checksum.

Raw lookup uses the raw file directly, never the aggregation indexes. Sequential
checks and rebuilds advance a raw cursor using block reads instead of starting
a binary search for each interval. Backward suffix checks still use binary
searches. Plain logs report raw record reads and block/probe read requests.
Higher
levels are reconstructed from smaller levels with the same summation order as
normal insertion. Reads and writes use bounded buffers. Missing and truncated
aggregation files can be rebuilt in full; a damaged/unsupported raw header is
refused before any original file is modified. The implementation targets Linux
and the native HTA v2 format; extended headers are not supported for raw files.

Before the first original file is changed, replacement suffixes and backups of
all affected original bytes are written and synced in `.hta-fsck-recovery` inside
the metric directory. This requires space for both suffixes, not a copy of the
whole raw file. There must still be free space available on the volume. An I/O
failure while preparing replacements/backups leaves the original files unchanged.
A failure during installation leaves a recovery journal; **do not start the DB**
with a partially installed repair. Restore the exact previous bytes with:

```bash
hta_fsck --rollback --metric /path/to/db/metric
```

`--rollback /path/to/db` restores metrics with recovery journals. Metrics without
journals are unchanged (exit 0), consistently in database and `--metric` modes.

Rollback is only for an offline metric that has not received new data since the
repair. It restores even the original incomplete bytes and removes aggregation
files created by this repair. Backups are retained after success and rollback;
before restoring any data, rollback durably records `rolling-back`. If rollback
is interrupted, normal checks/repairs refuse the metric until `--rollback` is
rerun successfully. Failure to persist the initial state leaves the repaired
data untouched.
After a completed rollback or repair,
archive/move `.hta-fsck-recovery` before a subsequent repair of the same metric.
Read-only checks are allowed with both `complete` and `rolled-back` journals.
A repeated rollback of a `rolled-back` journal is a no-op, preserving both the
data and journal. Only `prepared`/`rolling-back` states require finishing rollback
before checking again; unknown states are refused explicitly.
If another repair is needed, `--apply` reports that the old journal must be
archived, before creating any staging directory or changing files. An unchanged
metric can be checked/reapplied without altering its files.
The directory lock prevents concurrent `hta_fsck` processes, but does not lock
out the database writer. Interrupted preparation may leave `.hta-fsck-stage-*`
directories; no original files were changed until the recovery journal exists.
Subsequent runs warn about each orphan staging entry and retain it unchanged.
Inspect/archive these entries before removing them to reclaim space; the tool
does not automatically delete potential recovery data.

The default and `--full` plans return 0 when preparation succeeds (including when
repairs are needed), and 1 for an error. Plain logs report each level, extra records,
and incomplete suffix bytes. Dry runs use temporary staging outside the metric
only when replacements are needed. The final summary counts unchanged metrics,
metrics needing repair (or repaired metrics with `--apply`), and failures.

For multiple metrics, use the checker's list as before:

```bash
parallel --jobs 4 --noswap --joblog fsck-jobs.log --results fsck-results \
    hta_fsck --apply --metric /path/to/db/{} :::: corrupted_metrics_TIMESTAMP
```

Check job exit codes and run the checker again before restarting the DB. If the
goal is validation of all historical data, use `hta_fsck --full` as well.

### Recovery regression tests

From the metricq-db-hta repository root, configure with `BUILD_TESTING=ON`, then:

```bash
cmake --build build --target hta_fsck hta_repair hta_fsck_fixture hta_fsck_fault -j4
ctest --test-dir build -R '^hta.fsck$' --output-on-failure
```

The tests generate synthetic metrics using the regular HTA writer, damage copies,
and compare all resulting files using SHA-256 and byte equality against the real
`hta_repair` applied to copies of the same damaged input. Only incomplete raw
suffixes are normalized first in the `hta_repair` copy, because that tool cannot
open a partial raw record. The suite includes every partial raw/aggregate record
length, missing levels, malformed tails, propagation across levels, first-interval
arithmetic, older corruption in full mode, idempotence, and exact rollback.
For every aggregation level separately, one complete final row is removed and
repaired in both fast/full modes. These cases check byte/hash equality with
`hta_repair`, preservation of the entire surviving prefix, and unchanged raw
and lower-level files including their metadata.
One targeted regression drops thousands of raw points while leaving every
aggregation file intact, with the surviving raw endpoint just before/after an
upper-level interval boundary. Both fast/full modes and complete/partial raw
tails must remove unclosed aggregate rows without shortening their `active_time`;
only the first surviving row per level may have reduced `active_time`.
It also checks byte/hash equality with normal insertion and `hta_repair`,
preservation of the surviving raw prefix, and exact rollback of the damaged input.
The common repair helper also checks the complete directory tree, preserves
unrelated files and archived backups, validates journal contents against original
and repaired suffixes, and checks repeat-run metadata/byte identity. Regression
tests cover explicit directory modes, backup-name collisions, orphan staging,
lock errors, streaming read budgets, and terminal/plain progress output via a PTY.
Tests also cover post-rollback checks without archiving first, no-op repeated
rollback, terminal dry-run plans/verbose output, symlink rejection, explicit
`lost+found` exclusions, and control-character escaping in logs and the terminal.
Empty and single-point metrics are compared against the normal writer instead:
the legacy repair tool's progress calculation is undefined for a zero time range.
A test-only preload library injects short writes, ENOSPC, and process termination
during preparation/installation; production `hta_fsck` has no fault-injection hooks.

When Valgrind is installed at CMake configuration time, a separate Memcheck test
is available. It covers healthy files, database mode, full rebuilding, dependency
propagation, malformed inputs and rollback, using `--leak-check=full`,
`--track-origins=yes` and `--error-exitcode=97`:

```bash
ctest --test-dir build -R '^hta.fsck.valgrind$' --output-on-failure
```

Per-process XML reports and logs are retained in `build/fsck-valgrind/` for a
root-project build. The harness rejects signal termination, reported memory/leak
errors and missing/incomplete XML reports; expected tool failures must return
exactly 1. A deliberately crashing fixture verifies that SIGSEGV cannot pass as
an expected tool failure. Its intentional-error reports use the separate prefix
`harness-expected-error.*`, while actual fsck reports use `hta_fsck.*`.
