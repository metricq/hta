// Offline recovery of append-only HTA files. See README.md for the trust boundary.
#include "../storage/file/metric.hpp"
#include "fsck_progress.hpp"
#include "fsck_search.hpp"
#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <cmath>
#include <condition_variable>
#include <csignal>
#include <cstring>
#include <fcntl.h>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <limits>
#include <memory>
#include <nlohmann/json.hpp>
#include <optional>
#include <stdexcept>
#include <string>
#include <sys/file.h>
#include <thread>
#include <unistd.h>
#include <vector>

namespace fs = std::filesystem;
using namespace hta;
using Header = hta::storage::file::Metric::Header;
using json = nlohmann::json;
namespace format = hta::storage::file::format;
constexpr uint64_t preamble = format::header_begin;
constexpr uint64_t data_begin = preamble + sizeof(Header);
static_assert(sizeof(TimeValue) == 16 && sizeof(TimeAggregate) == 56);

void require(bool ok, const std::string& message)
{
    if (!ok)
        throw std::runtime_error(message);
}

// Check components before following directory paths, including "link/" and
// "link/.". Writers must still be stopped; this is not a concurrent-path sandbox.
void require_no_symlinks(const fs::path& path)
{
    fs::path part;
    for (const auto& component : path)
    {
        part /= component;
        require(!fs::is_symlink(fs::symlink_status(part)),
                "symlinks are not supported: " + part.string() +
                    "; use the real path or --exclude NAME in database mode");
    }
}

struct Fd
{
    int fd;
    explicit Fd(const fs::path& path, int flags) : fd(-1)
    {
        require(!fs::is_symlink(fs::symlink_status(path)),
                "symlinks are not supported: " + path.string());
        fd = ::open(path.c_str(), flags | O_CLOEXEC | O_NOFOLLOW, 0644);
        require(fd >= 0, path.string() + ": " + std::strerror(errno));
    }
    ~Fd()
    {
        ::close(fd);
    }
    Fd(const Fd&) = delete;
    Fd& operator=(const Fd&) = delete;
    void sync()
    {
        require(::fsync(fd) == 0, "fsync failed: " + std::string(std::strerror(errno)));
    }
};

void read_at(int fd, void* data, size_t bytes, uint64_t offset)
{
    auto p = static_cast<char*>(data);
    while (bytes)
    {
        auto n = ::pread(fd, p, bytes, offset);
        if (n < 0 && errno == EINTR)
            continue;
        require(n > 0, "short/failed read at " + std::to_string(offset));
        p += n;
        bytes -= n;
        offset += n;
    }
}

void write_at(int fd, const void* data, size_t bytes, uint64_t offset)
{
    auto p = static_cast<const char*>(data);
    while (bytes)
    {
        auto n = ::pwrite(fd, p, bytes, offset);
        if (n < 0 && errno == EINTR)
            continue;
        require(n > 0, "write failed: " + std::string(std::strerror(errno)));
        p += n;
        bytes -= n;
        offset += n;
    }
}

template <class T>
T read_object(int fd, uint64_t offset)
{
    T value{};
    read_at(fd, &value, sizeof(value), offset);
    return value;
}

Header read_header(int fd, uint64_t bytes)
{
    require(bytes >= data_begin, "truncated HTA header");
    require(read_object<std::array<char, 8>>(fd, 0) == format::magic, "invalid HTA magic");
    require(read_object<uint64_t>(fd, 8) == format::bom, "unsupported byte order");
    const auto header_bytes = read_object<uint64_t>(fd, 16);
    require(header_bytes == sizeof(Header), "unsupported HTA header size");
    auto header = read_object<Header>(fd, preamble);
    header.restore(header_bytes);
    return header;
}

void write_header(int fd, Duration interval, Meta meta)
{
    Header header(interval, meta);
    uint64_t size = sizeof(header);
    write_at(fd, format::magic.data(), format::magic.size(), 0);
    write_at(fd, &format::bom, sizeof(format::bom), 8);
    write_at(fd, &size, sizeof(size), 16);
    write_at(fd, &header, sizeof(header), preamble);
}

template <class T>
struct Records
{
    Fd file;
    uint64_t count, base;
    uint64_t cache_begin = std::numeric_limits<uint64_t>::max();
    std::vector<T> cache;
    uint64_t reads = 0;
    uint64_t read_requests = 0;
    Progress* progress = nullptr;
    Records(const fs::path& path, uint64_t size, uint64_t offset = data_begin)
    : file(path, O_RDONLY), count(size), base(offset)
    {
    }
    T get(uint64_t index)
    {
        require(index < count, "record index out of bounds");
        if (cache_begin == std::numeric_limits<uint64_t>::max() || index < cache_begin ||
            index - cache_begin >= cache.size())
        {
            cache_begin = index / 4096 * 4096;
            cache.resize(std::min<uint64_t>(4096, count - cache_begin));
            read_at(file.fd, cache.data(), cache.size() * sizeof(T),
                    base + cache_begin * sizeof(T));
            reads += cache.size();
            ++read_requests;
            if (progress)
                progress->refresh();
        }
        return cache[index - cache_begin];
    }
    T probe(uint64_t index)
    {
        require(index < count, "record index out of bounds");
        if (cache_begin != std::numeric_limits<uint64_t>::max() && index >= cache_begin &&
            index - cache_begin < cache.size())
            return cache[index - cache_begin];
        ++reads;
        ++read_requests;
        return read_object<T>(file.fd, base + index * sizeof(T));
    }
    uint64_t tail_begin() const
    {
        return cache_begin < count && cache.size() == count - cache_begin ? cache_begin : count;
    }
};

void validate_tv(TimeValue tv)
{
    require(tv.time.time_since_epoch().count() > 0 && std::isfinite(tv.value),
            "invalid complete raw record; refusing to discard data");
}

// Match Metric::insert(TimeValue), including first-interval and closing-point semantics.
struct RawCursor
{
    std::optional<TimePoint> next_begin;
    uint64_t index = 0;

    uint64_t locate(Records<TimeValue>& raw, TimePoint begin, TimePoint first)
    {
        if (!next_begin || begin < *next_begin)
            index = begin <= first ? 0 : tail_lower_bound(raw, begin);
        else
            while (index < raw.count && raw.get(index).time < begin)
                ++index;
        return index;
    }
};

Aggregate from_raw(Records<TimeValue>& raw, RawCursor& cursor, TimePoint first, TimePoint begin,
                   TimePoint end)
{
    Aggregate result;
    auto previous = std::max(begin, first);
    auto index = cursor.locate(raw, begin, first);
    std::optional<TimePoint> previous_record;
    if (index)
        previous_record = raw.get(index - 1).time;
    for (; index < raw.count; ++index)
    {
        auto tv = raw.get(index);
        validate_tv(tv);
        require(!previous_record || *previous_record < tv.time, "non-monotonic raw timestamps");
        previous_record = tv.time;
        if (tv.time >= end)
        {
            // Reuse the closing point for the next interval. Forward rebuilds
            // stream raw blocks rather than binary-searching for every row.
            cursor.index = index;
            cursor.next_begin = end;
            auto duration = end - previous;
            result += Aggregate(tv.value, tv.value, 0, 0, tv.value * duration.count(), duration);
            return result;
        }
        result += Aggregate(tv.value, tv.time - previous);
        previous = tv.time;
    }
    throw std::runtime_error("raw data cannot close requested interval");
}

bool same(const TimeAggregate& a, const TimeAggregate& b)
{
    // The supported HTA on-disk record has no padding. Preserve exact float bits.
    return std::memcmp(&a, &b, sizeof(a)) == 0;
}

struct Temporary
{
    fs::path path;
    explicit Temporary(const fs::path& parent)
    {
        std::string pattern = (parent / ".hta-fsck-stage-XXXXXX").string();
        auto result = ::mkdtemp(pattern.data());
        require(result != nullptr,
                "cannot create staging directory: " + std::string(std::strerror(errno)));
        path = result;
    }
    ~Temporary()
    {
        if (!path.empty())
        {
            std::error_code ec;
            fs::remove_all(path, ec);
        }
    }
};

struct Level
{
    Duration interval;
    TimePoint epoch;
    uint64_t expected = 0, keep = 0;
    std::unique_ptr<Records<TimeAggregate>> original, replacement;
    TimeAggregate get(uint64_t index)
    {
        return index < keep ? original->get(index) : replacement->get(index - keep);
    }
};

void copy_range(int src, int dst, uint64_t src_offset, uint64_t dst_offset, uint64_t bytes,
                Progress& progress)
{
    std::array<char, 128 * 1024> buffer;
    while (bytes)
    {
        auto n = std::min<uint64_t>(buffer.size(), bytes);
        read_at(src, buffer.data(), n, src_offset);
        write_at(dst, buffer.data(), n, dst_offset);
        src_offset += n;
        dst_offset += n;
        bytes -= n;
        progress.refresh();
    }
}

void save_manifest(const fs::path& directory, const json& manifest)
{
    auto text = manifest.dump(2);
    Fd file(directory / "manifest.tmp", O_WRONLY | O_CREAT | O_TRUNC);
    write_at(file.fd, text.data(), text.size(), 0);
    file.sync();
    fs::rename(directory / "manifest.tmp", directory / "manifest.json");
    Fd dir(directory, O_RDONLY | O_DIRECTORY);
    dir.sync();
}

json load_manifest(const fs::path& directory)
{
    std::ifstream input(directory / "manifest.json");
    require(bool(input), "missing recovery manifest");
    json result;
    input >> result;
    return result;
}

std::string recovery_name(const json& entry)
{
    const auto name = entry.at("name").get<std::string>();
    require(fs::path(name).filename() == name && fs::path(name).extension() == ".hta",
            "invalid recovery filename");
    return name;
}

void install_suffix(const fs::path& target, const fs::path& source, uint64_t offset,
                    Progress& progress)
{
    Fd input(source, O_RDONLY);
    Fd output(target, O_WRONLY | O_CREAT);
    auto bytes = fs::file_size(source);
    copy_range(input.fd, output.fd, 0, offset, bytes, progress);
    require(::ftruncate(output.fd, offset + bytes) == 0, "truncate failed");
    output.sync();
}

void apply_entry(const fs::path& metric, const fs::path& journal, const json& entry,
                 Progress& progress)
{
    const auto name = recovery_name(entry);
    install_suffix(metric / name, journal / (name + ".after"), entry.at("offset").get<uint64_t>(),
                   progress);
}

void restore_entry(const fs::path& metric, const fs::path& journal, const json& entry,
                   Progress& progress)
{
    const auto name = recovery_name(entry);
    if (!entry.at("existed").get<bool>())
        fs::remove(metric / name);
    else
        install_suffix(metric / name, journal / (name + ".before"),
                       entry.at("offset").get<uint64_t>(), progress);
}

std::string recovery_state(const json& manifest)
{
    auto state = manifest.at("state").get<std::string>();
    require(state == "prepared" || state == "rolling-back" || state == "complete" ||
                state == "rolled-back",
            "unknown recovery state: " + state);
    return state;
}

bool rollback(const fs::path& metric, Progress& progress)
{
    const auto journal = metric / ".hta-fsck-recovery";
    auto manifest = load_manifest(journal);
    if (recovery_state(manifest) == "rolled-back")
    {
        progress.detail_line(
            "Already rolled back; archive " + journal.string() + " before another repair.", true);
        return false;
    }
    // Commit this state before touching data, including when retrying a failed
    // rollback. A crash must never leave partially restored files marked complete.
    manifest["state"] = "rolling-back";
    save_manifest(journal, manifest);
    size_t restored = 0;
    for (const auto& entry : manifest.at("files"))
    {
        progress.update(0.1 + 0.8 * restored++ / manifest.at("files").size(),
                        "restoring " + entry.at("name").get<std::string>());
        restore_entry(metric, journal, entry, progress);
    }
    Fd dir(metric, O_RDONLY | O_DIRECTORY);
    dir.sync();
    manifest["state"] = "rolled-back";
    save_manifest(journal, manifest);
    progress.detail_line("Original file suffixes restored; archive " + journal.string() +
                             " before another repair.",
                         true);
    return true;
}

bool recover_metric(const fs::path& metric, bool apply, bool full, bool undo, Progress& progress)
{
    require_no_symlinks(metric);
    Fd lock(metric, O_RDONLY | O_DIRECTORY);
    if (::flock(lock.fd, LOCK_EX | LOCK_NB) != 0)
    {
        const int error = errno;
        throw std::runtime_error(
            std::string("cannot lock metric: ") + std::strerror(error) +
            (error == EWOULDBLOCK ? " (another fsck is using this metric)" : ""));
    }
    for (const auto& entry : fs::directory_iterator(metric))
        if (entry.path().filename().string().rfind(".hta-fsck-stage-", 0) == 0)
            progress.warning("orphan staging entry " + entry.path().string() +
                             "; retained, inspect/archive it to reclaim space");
    const auto journal = metric / ".hta-fsck-recovery";
    require(!fs::is_symlink(fs::symlink_status(journal)),
            "symlinks are not supported: " + journal.string());
    if (undo)
    {
        if (!fs::exists(journal))
        {
            require(fs::is_regular_file(metric / "raw.hta"), "missing raw.hta in metric");
            progress.detail_line("No recovery journal; unchanged.", true);
            return false;
        }
        return rollback(metric, progress);
    }
    if (fs::exists(journal))
    {
        const auto state = recovery_state(load_manifest(journal));
        require(state == "complete" || state == "rolled-back",
                "unfinished recovery: use --rollback before retrying");
    }
    auto raw_path = metric / "raw.hta";
    auto raw_bytes = fs::file_size(raw_path);
    Fd raw_fd(raw_path, O_RDONLY);
    auto header = read_header(raw_fd.fd, raw_bytes);
    require(header.interval == 0 && header.interval_min > 0 && header.interval_factor > 1 &&
                header.interval_max >= header.interval_min,
            "invalid raw metadata");
    require(header.interval_max <= std::numeric_limits<int64_t>::max() / header.interval_factor,
            "aggregation intervals overflow");
    Meta meta(Duration(header.interval_min), Duration(header.interval_max), header.interval_factor);
    auto count = (raw_bytes - data_begin) / sizeof(TimeValue);
    auto partial = (raw_bytes - data_begin) % sizeof(TimeValue);
    Records<TimeValue> raw(raw_path, count);
    raw.progress = &progress;
    TimePoint first, last;
    if (count)
    {
        const auto first_record = raw.probe(0);
        validate_tv(first_record);
        first = first_record.time;
        last = raw.probe(count - 1).time;
        require(last >= first && last.time_since_epoch().count() <=
                                     std::numeric_limits<int64_t>::max() - header.interval_max,
                "invalid raw time range");
        auto start = full || count <= 4096 ? 0 : count - 4096;
        TimePoint previous;
        if (start)
            previous = raw.get(start - 1).time;
        for (uint64_t i = start; i < count; ++i)
        {
            if (i % 4096 == 0)
                progress.update(0.1 * (i - start) / (count - start), "validating raw.hta");
            auto tv = raw.get(i);
            validate_tv(tv);
            if (i)
                require(previous < tv.time, "non-monotonic raw timestamps");
            previous = tv.time;
        }
    }
    // Do not even create a temporary directory inside a healthy metric:
    // that would change its directory mtime despite leaving file bytes intact.
    std::unique_ptr<Temporary> stage;
    auto staging = [&]() -> const fs::path& {
        // A completed journal may be inspected, but must never be overwritten.
        // Refuse before creating staging entries inside the metric directory.
        require(!apply || !fs::exists(journal),
                "archive existing .hta-fsck-recovery before a new repair");
        if (!stage)
            stage = std::make_unique<Temporary>(apply ? metric : fs::temp_directory_path());
        return stage->path;
    };
    json manifest = { { "state", "prepared" }, { "files", json::array() } };
    auto add_change = [&](const std::string& name, uint64_t offset, bool existed) {
        manifest["files"].push_back(
            { { "name", name }, { "offset", offset }, { "existed", existed } });
    };
    progress.detail_line("raw.hta: " + std::to_string(count) + " complete records, remove " +
                         std::to_string(partial) + " trailing bytes");
    if (partial)
    {
        Fd replacement(staging() / "raw.hta.after", O_WRONLY | O_CREAT | O_EXCL);
        add_change("raw.hta", raw_bytes - partial, true);
    }
    std::vector<std::unique_ptr<Level>> levels;
    RawCursor raw_cursor;
    size_t level_count = 0;
    for (auto interval = meta.interval_min; interval <= meta.interval_max;
         interval *= meta.interval_factor)
        ++level_count;
    for (auto interval = meta.interval_min; interval <= meta.interval_max;
         interval *= meta.interval_factor)
    {
        auto level = std::make_unique<Level>();
        level->interval = interval;
        level->epoch = interval_begin(first, interval);
        level->expected = count ? (interval_begin(last, interval) - level->epoch) / interval : 0;
        auto name = std::to_string(interval.count()) + ".hta";
        const double level_start = 0.1 + 0.7 * levels.size() / level_count;
        const double level_weight = 0.7 / level_count;
        progress.update(level_start, "checking " + name);
        auto path = metric / name;
        auto existed = fs::exists(path);
        uint64_t bytes = existed ? fs::file_size(path) : 0;
        bool good_header = false;
        if (existed && bytes >= data_begin)
        {
            Fd input(path, O_RDONLY);
            try
            {
                auto h = read_header(input.fd, bytes);
                good_header = h.interval == interval.count() &&
                              h.interval_min == header.interval_min &&
                              h.interval_max == header.interval_max &&
                              h.interval_factor == header.interval_factor;
            }
            catch (const std::runtime_error&)
            { /* Aggregation headers are reconstructible. */
            }
        }
        uint64_t present = good_header ? (bytes - data_begin) / sizeof(TimeAggregate) : 0;
        if (good_header)
            level->original = std::make_unique<Records<TimeAggregate>>(path, present);
        level->keep = std::min(present, level->expected);
        Level* lower = levels.empty() ? nullptr : levels.back().get();
        if (lower && lower->keep < lower->expected)
        {
            TimePoint changed = lower->epoch + lower->interval * static_cast<int64_t>(lower->keep);
            auto parent = (interval_begin(changed, interval) - level->epoch) / interval;
            level->keep = std::min(level->keep, static_cast<uint64_t>(parent));
        }
        auto compute = [&](uint64_t index) {
            progress.refresh();
            TimePoint begin = level->epoch + interval * static_cast<int64_t>(index);
            TimePoint end = begin + interval;
            Aggregate aggregate;
            if (!lower)
                aggregate = from_raw(raw, raw_cursor, first, begin, end);
            else
            {
                auto start = std::max(begin, lower->epoch);
                auto child = (start - lower->epoch) / lower->interval;
                auto stop = (end - lower->epoch) / lower->interval;
                for (; child < stop; ++child)
                    aggregate += lower->get(child).aggregate;
            }
            return TimeAggregate(begin, aggregate);
        };
        if (full)
        {
            for (uint64_t i = 0; i < level->keep; ++i)
            {
                if (i % 64 == 0)
                    progress.update(level_start + level_weight * i / level->expected,
                                    "checking " + name);
                if (!same(level->original->get(i), compute(i)))
                {
                    level->keep = i;
                    break;
                }
            }
        }
        else
        {
            // Walk back to a complete row that agrees with independently recomputed data.
            while (level->keep &&
                   !same(level->original->get(level->keep - 1), compute(level->keep - 1)))
                --level->keep;
        }
        const auto rebuilt = level->expected - level->keep;
        // Inserting the first closed child interval opens its parent, even if
        // the parent has no closed intervals yet. Recreate that empty header,
        // but leave unused levels absent (as on healthy short/empty metrics).
        const bool missing_parent = !existed && lower && lower->expected > 0;
        bool changed = rebuilt || missing_parent ||
                       (existed && (!good_header ||
                                    bytes != data_begin + level->expected * sizeof(TimeAggregate)));
        // Avoid lazy shared locale-facet initialization in numeric ostreams.
        progress.detail_line(
            name + ": keep " + std::to_string(level->keep) + ", rebuild " +
            std::to_string(rebuilt) + ", remove " +
            std::to_string(present > level->expected ? present - level->expected : 0) +
            " extra records, " +
            std::to_string(good_header ? (bytes - data_begin) % sizeof(TimeAggregate) : 0) +
            " partial bytes" + (changed && !good_header ? ", replace header" : ""));
        if (changed)
        {
            const auto offset = good_header ? data_begin + level->keep * sizeof(TimeAggregate) : 0;
            const auto patch_base = good_header ? 0 : data_begin;
            auto after = staging() / (name + ".after");
            {
                Fd output(after, O_WRONLY | O_CREAT | O_EXCL);
                if (!good_header)
                    write_header(output.fd, interval, meta);
                std::vector<TimeAggregate> buffer;
                buffer.reserve(4096);
                uint64_t pos = patch_base;
                for (uint64_t i = level->keep; i < level->expected; ++i)
                {
                    if ((i - level->keep) % 64 == 0)
                        progress.update(level_start + level_weight * i / level->expected,
                                        "rebuilding " + name);
                    buffer.push_back(compute(i));
                    if (buffer.size() == 4096 || i + 1 == level->expected)
                    {
                        write_at(output.fd, buffer.data(), buffer.size() * sizeof(TimeAggregate),
                                 pos);
                        pos += buffer.size() * sizeof(TimeAggregate);
                        buffer.clear();
                    }
                }
            }
            level->replacement =
                std::make_unique<Records<TimeAggregate>>(after, rebuilt, patch_base);
            add_change(name, offset, existed);
        }
        levels.push_back(std::move(level));
    }
    progress.detail_line("Raw records read (including cached blocks): " +
                         std::to_string(raw.reads));
    progress.detail_line("Raw read requests (blocks and probes): " +
                         std::to_string(raw.read_requests));
    if (!apply || manifest["files"].empty())
        return !manifest["files"].empty();
    require(!fs::exists(journal), "archive existing .hta-fsck-recovery before a new repair");
    progress.update(0.8, "saving recovery journal");
    // Persist all replacement data and original suffixes BEFORE changing any original.
    for (const auto& entry : manifest["files"])
    {
        auto name = entry.at("name").get<std::string>();
        Fd backup(staging() / (name + ".before"), O_WRONLY | O_CREAT | O_EXCL);
        if (entry.at("existed").get<bool>())
        {
            Fd original(metric / name, O_RDONLY);
            auto offset = entry.at("offset").get<uint64_t>();
            copy_range(original.fd, backup.fd, offset, 0, fs::file_size(metric / name) - offset,
                       progress);
        }
        backup.sync();
        Fd replacement(staging() / (name + ".after"), O_RDONLY);
        replacement.sync();
    }
    save_manifest(staging(), manifest);
    fs::rename(staging(), journal);
    stage->path.clear();
    lock.sync();
    size_t installed = 0;
    for (const auto& entry : manifest["files"])
    {
        progress.update(0.9 + 0.09 * installed++ / manifest["files"].size(),
                        "installing " + entry.at("name").get<std::string>());
        apply_entry(metric, journal, entry, progress);
    }
    lock.sync();
    manifest["state"] = "complete";
    save_manifest(journal, manifest);
    progress.detail_line("Recovery complete. Original suffixes saved in " + journal.string(), true);
    return true;
}

bool backup_directory_name(const std::string& name)
{
    const auto marker = name.rfind(".backup-");
    return marker != std::string::npos && marker > 0 && marker + 8 < name.size() &&
           std::all_of(name.begin() + marker + 8, name.end(),
                       [](char c) { return c >= '0' && c <= '9'; });
}

// Lock-free atomics and _exit are safe here; logging and cleanup stay outside
// the handler. A second signal deliberately retains the emergency-exit option.
static_assert(std::atomic<int>::is_always_lock_free);
std::atomic<int> stop_signal{ 0 };
extern "C" void request_shutdown(int signal)
{
    if (stop_signal.exchange(signal, std::memory_order_relaxed))
        ::_exit(128 + signal);
}

struct SignalHandlers
{
    struct sigaction old_int{}, old_term{};
    SignalHandlers()
    {
        struct sigaction action{};
        action.sa_handler = request_shutdown;
        sigemptyset(&action.sa_mask);
        sigaddset(&action.sa_mask, SIGINT);
        sigaddset(&action.sa_mask, SIGTERM);
        action.sa_flags = SA_RESTART;
        require(::sigaction(SIGINT, &action, &old_int) == 0, "cannot install SIGINT handler");
        if (::sigaction(SIGTERM, &action, &old_term) != 0)
        {
            ::sigaction(SIGINT, &old_int, nullptr);
            throw std::runtime_error("cannot install SIGTERM handler");
        }
    }
    ~SignalHandlers()
    {
        ::sigaction(SIGINT, &old_int, nullptr);
        ::sigaction(SIGTERM, &old_term, nullptr);
    }
};

int main(int argc, char** argv)
{
    try
    {
        bool apply = false, full = false, undo = false, single = false, verbose = false;
        size_t jobs = 1;
        std::vector<std::string> excludes;
        fs::path input;
        for (int i = 1; i < argc; ++i)
        {
            std::string arg = argv[i];
            if (arg == "--apply")
                apply = true;
            else if (arg == "--full")
                full = true;
            else if (arg == "--rollback")
                undo = true;
            else if (arg == "--metric")
                single = true;
            else if (arg == "--verbose" || arg == "-v")
                verbose = true;
            else if (arg == "--jobs" || arg == "-j")
            {
                require(i + 1 < argc, "--jobs requires an integer from 1 to 64");
                const std::string value = argv[++i];
                require(!value.empty() && value.size() <= 2 &&
                            std::all_of(value.begin(), value.end(),
                                        [](char c) { return c >= '0' && c <= '9'; }),
                        "--jobs requires an integer from 1 to 64");
                jobs = std::stoul(value);
                require(jobs >= 1 && jobs <= 64, "--jobs requires an integer from 1 to 64");
            }
            else if (arg == "--exclude")
            {
                require(i + 1 < argc, "--exclude requires an immediate directory name");
                std::string name = argv[++i];
                require(!name.empty() && name != "." && name != ".." &&
                            fs::path(name).filename() == name,
                        "--exclude requires an immediate directory name, not a path");
                excludes.push_back(name);
            }
            else if (arg == "--help" || arg == "-h")
            {
                std::cout
                    << "Usage: hta_fsck [--apply] [--full] [--jobs N] [--exclude NAME ...] "
                       "HTA_DIRECTORY\n"
                    << "       hta_fsck [--apply] [--full] --metric METRIC_DIRECTORY\n"
                    << "       hta_fsck --rollback [--metric] DIRECTORY\n"
                    << "Default: database directory. --metric explicitly selects one metric.\n"
                    << "Default: read-only append-tail recovery plan. --full checks all records.\n"
                    << "--jobs N (or -j N): 1..64 concurrent metrics, default 1.\n"
                    << "SIGINT/SIGTERM: stop scheduling and finish active metrics; a second "
                       "signal\n"
                    << "exits immediately. A stopped run returns 128 + signal (130/143).\n"
                    << "Terminal dry-runs show the per-level plan; --verbose also shows it with "
                       "--apply.\n"
                    << "Symlink paths are refused; use the real directory path.\n"
                    << "Database mode skips NAME.backup-DIGITS entries and prints each skip.\n"
                    << "Use --metric to explicitly check/repair one of these backups.\n"
                    << "Healthy metrics are left unchanged. --exclude NAME skips other "
                       "directories.\n"
                    << "Stop all database writers first. --apply keeps original suffixes in each\n"
                    << "metric's .hta-fsck-recovery; archive it before a subsequent repair.\n";
                return 0;
            }
            else
            {
                require(input.empty() && !arg.empty() && arg[0] != '-', "invalid arguments");
                input = arg;
            }
        }
        require(!input.empty() && !(undo && (apply || full)), "invalid arguments (see --help)");
        require(!single || excludes.empty(), "--exclude is only valid in database mode");
        require_no_symlinks(input);
        require(fs::is_directory(input), "input is not a directory");
        std::vector<fs::path> metrics;
        if (single)
            metrics = { input };
        else
        {
            // Validate discovery completely before any repair. A root .hta file
            // is a layout error, never an implicit switch to single-metric mode.
            for (const auto& name : excludes)
                require(fs::is_symlink(fs::symlink_status(input / name)) ||
                            fs::is_directory(input / name),
                        "excluded directory does not exist: " + name);
            for (const auto& entry : fs::directory_iterator(input))
            {
                // Queue symlinks without following them so they receive an
                // explicit per-metric failure (and do not stop other metrics).
                if (entry.is_symlink() || entry.is_directory())
                {
                    const auto name = entry.path().filename().string();
                    if (std::find(excludes.begin(), excludes.end(), name) != excludes.end())
                        std::cout << "Excluded directory: " << escape_log(entry.path().string())
                                  << '\n';
                    else if (backup_directory_name(name))
                        std::cout << "Skipped backup directory: "
                                  << escape_log(entry.path().string()) << '\n';
                    else
                        metrics.push_back(entry.path());
                }
                else
                    require(entry.path().extension() != ".hta",
                            "unexpected .hta file in database root: " + entry.path().string() +
                                "; use --metric for an individual metric");
            }
        }
        std::sort(metrics.begin(), metrics.end());
        SignalHandlers signals;
        size_t changed = 0, unchanged = 0, failed = 0;
        const auto worker_count = std::min(jobs, metrics.size());
        Progress progress(metrics.size(), verbose || (!apply && !undo),
                          std::max<size_t>(1, worker_count));
        std::mutex queue_mutex;
        std::condition_variable start_condition, done_condition;
        size_t next = 0, finished_workers = 0;
        bool start = false, abort_start = false;
        auto work = [&](size_t slot) {
            auto local = progress.worker(slot);
            {
                std::unique_lock<std::mutex> guard(queue_mutex);
                start_condition.wait(guard, [&] { return start; });
                if (abort_start)
                    return;
            }
            for (;;)
            {
                size_t index;
                {
                    std::lock_guard<std::mutex> guard(queue_mutex);
                    if (stop_signal.load(std::memory_order_relaxed) || next == metrics.size())
                        break;
                    index = next++;
                }
                const auto& metric = metrics[index];
                try
                {
                    local.begin(metric.string());
                    const auto modified = recover_metric(metric, apply, full, undo, local);
                    local.finish(modified ? (undo  ? "rolled back" :
                                             apply ? "repaired" :
                                                     "needs repair") :
                                            "unchanged");
                    std::lock_guard<std::mutex> guard(queue_mutex);
                    if (modified)
                        ++changed;
                    else
                        ++unchanged;
                }
                catch (const std::exception& error)
                {
                    local.finish("FAILED", error.what());
                    std::lock_guard<std::mutex> guard(queue_mutex);
                    ++failed;
                }
            }
            {
                std::lock_guard<std::mutex> guard(queue_mutex);
                ++finished_workers;
                done_condition.notify_one();
            }
        };
        std::vector<std::thread> workers;
        workers.reserve(worker_count);
        try
        {
            for (size_t slot = 0; slot < worker_count; ++slot)
                workers.emplace_back(work, slot);
        }
        catch (...)
        {
            // No worker may mutate a metric until all threads were created.
            {
                std::lock_guard<std::mutex> guard(queue_mutex);
                abort_start = start = true;
                start_condition.notify_all();
            }
            for (auto& worker : workers)
                worker.join();
            throw;
        }
        {
            std::lock_guard<std::mutex> guard(queue_mutex);
            start = true;
            start_condition.notify_all();
        }
        {
            std::unique_lock<std::mutex> guard(queue_mutex);
            while (finished_workers < worker_count)
            {
                done_condition.wait_for(guard, std::chrono::milliseconds(100));
                if (stop_signal.load(std::memory_order_relaxed))
                    progress.request_stop();
            }
        }
        for (auto& worker : workers)
            worker.join();
        const auto interrupted = stop_signal.load(std::memory_order_relaxed);
        if (interrupted)
            progress.request_stop();
        progress.clear();
        std::cout << "Summary: " << unchanged << " unchanged, " << changed
                  << (undo  ? " rolled back, " :
                      apply ? " repaired, " :
                              " need repair, ")
                  << failed << " failed";
        if (interrupted)
            std::cout << ", " << metrics.size() - next << " not started (interrupted)";
        std::cout << '\n' << std::flush;
        progress.final_bar();
        return interrupted ? 128 + interrupted : failed ? 1 : 0;
    }
    catch (const std::exception& error)
    {
        std::cerr << "hta_fsck: " << escape_log(error.what()) << '\n';
        return 1;
    }
}
