#pragma once

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <memory>
#include <mutex>
#include <string>
#include <sys/ioctl.h>
#include <unistd.h>
#include <vector>

// Escape untrusted bytes before adding our own line separators/terminal codes.
inline std::string escape_log(const std::string& text)
{
    constexpr char hex[] = "0123456789abcdef";
    std::string result;
    for (unsigned char c : text)
    {
        switch (c)
        {
        case '\\':
            result += "\\\\";
            break;
        case '\n':
            result += "\\n";
            break;
        case '\r':
            result += "\\r";
            break;
        case '\t':
            result += "\\t";
            break;
        default:
            if (c < 32 || c >= 127)
            {
                result += "\\x";
                result += hex[c >> 4];
                result += hex[c & 15];
            }
            else
                result += static_cast<char>(c);
        }
    }
    return result;
}

// Worker-local slots, shared serialized output. Complete plans are printed as
// one block, never interleaved with another worker's plan or terminal refresh.
class Progress
{
    using Clock = std::chrono::steady_clock;
    struct Slot
    {
        std::string metric, phase;
        std::vector<std::string> details;
        double fraction = 0;
        bool active = false;
    };
    struct State
    {
        std::mutex mutex;
        std::vector<Slot> slots;
        size_t total, completed = 0, visible_lines = 0;
        Clock::time_point started = Clock::now(), rendered = started;
        const bool terminal, show_details;
        bool stopping = false;

        static bool is_terminal()
        {
            const char* term = std::getenv("TERM");
            return ::isatty(STDOUT_FILENO) && ::isatty(STDERR_FILENO) &&
                   (!term || std::string(term) != "dumb");
        }
        State(size_t count, bool details, size_t jobs)
        : slots(jobs), total(count), terminal(is_terminal()), show_details(details)
        {
        }
        static std::string duration(double seconds)
        {
            if (!std::isfinite(seconds) || seconds > 365.0 * 86400)
                return ">365d";
            auto s = static_cast<uint64_t>(std::max(0.0, seconds));
            if (s >= 3600)
                return std::to_string(s / 3600) + "h " + std::to_string(s / 60 % 60) + "m";
            return std::to_string(s / 60) + "m " + std::to_string(s % 60) + "s";
        }
        static std::string clipped(std::string text, size_t width)
        {
            text = escape_log(text);
            if (text.size() > width)
                text = width <= 3 ? text.substr(0, width) : text.substr(0, width - 3) + "...";
            return text;
        }
        static winsize dimensions()
        {
            winsize size{};
            if (::ioctl(STDERR_FILENO, TIOCGWINSZ, &size) != 0)
                size = {};
            if (size.ws_col < 2)
                size.ws_col = 80;
            if (size.ws_row < 3)
                size.ws_row = 3;
            return size;
        }
        std::string bar(size_t columns) const
        {
            double work = completed;
            for (const auto& slot : slots)
                if (slot.active)
                    work += slot.fraction;
            const double ratio = total ? std::min(1.0, work / total) : 1.0;
            const auto elapsed = std::chrono::duration<double>(Clock::now() - started).count();
            // Metrics have equal weight, not equal I/O cost: deliberately approximate.
            const std::string eta = stopping           ? "-- (stopping)" :
                                    completed == total ? "0m 0s" :
                                    work > 0 ? "~" + duration(elapsed * (total - work) / work) :
                                               "--";
            const auto suffix = " " + std::to_string(static_cast<int>(ratio * 100)) + "% " +
                                std::to_string(completed) + "/" + std::to_string(total) + " ETA " +
                                eta;
            const auto length =
                columns > suffix.size() + 4 ? std::min<size_t>(30, columns - suffix.size() - 2) : 1;
            const auto filled = static_cast<size_t>(ratio * length);
            return clipped("[" + std::string(filled, '=') + std::string(length - filled, ' ') +
                               "]" + suffix,
                           columns);
        }
        // All methods below require mutex; never called from a signal handler.
        void clear()
        {
            if (!visible_lines)
                return;
            std::cerr << "\r\033[2K";
            while (--visible_lines)
                std::cerr << "\033[1A\r\033[2K";
            std::cerr << std::flush;
        }
        void render(bool force = false)
        {
            if (!terminal)
                return;
            const auto now = Clock::now();
            if (!force && now - rendered < std::chrono::milliseconds(100))
                return;
            clear();
            const auto size = dimensions();
            const size_t columns = size.ws_col - 1;
            const auto active = static_cast<size_t>(
                std::count_if(slots.begin(), slots.end(), [](const Slot& s) { return s.active; }));
            const size_t limit = size.ws_row - 2;
            const size_t shown = active > limit ? limit - 1 : active;
            for (const auto& slot : slots)
            {
                if (!slot.active || visible_lines >= shown)
                    continue;
                std::cerr << clipped("Current: " +
                                         std::filesystem::path(slot.metric).filename().string() +
                                         " | " + slot.phase,
                                     columns)
                          << '\n';
                ++visible_lines;
            }
            if (active > shown)
            {
                std::cerr << clipped(std::to_string(active - shown) + " more active metrics",
                                     columns)
                          << '\n';
                ++visible_lines;
            }
            std::cerr << bar(columns) << std::flush;
            ++visible_lines;
            rendered = now;
        }
    };
    std::shared_ptr<State> state_;
    size_t slot_ = 0;
    bool owner_ = false;
    Clock::time_point refreshed_ = Clock::now(); // Accessed only by this slot's worker.
    Progress(std::shared_ptr<State> state, size_t slot) : state_(std::move(state)), slot_(slot)
    {
    }

public:
    explicit Progress(size_t total, bool details, size_t jobs = 1)
    : state_(std::make_shared<State>(total, details, jobs)), owner_(true)
    {
    }
    Progress(const Progress&) = delete;
    Progress& operator=(const Progress&) = delete;
    ~Progress()
    {
        if (owner_)
            clear();
    }
    Progress worker(size_t slot)
    {
        return Progress(state_, slot);
    }
    bool interactive() const
    {
        return state_->terminal;
    }
    void clear()
    {
        std::lock_guard<std::mutex> guard(state_->mutex);
        state_->clear();
    }
    void refresh(bool force = false)
    {
        if (!state_->terminal)
            return;
        const auto now = Clock::now();
        if (!force && now - refreshed_ < std::chrono::milliseconds(100))
            return;
        refreshed_ = now;
        std::lock_guard<std::mutex> guard(state_->mutex);
        state_->render(force);
    }
    void detail_line(const std::string& text, bool important = false)
    {
        std::lock_guard<std::mutex> guard(state_->mutex);
        if (!state_->terminal || state_->show_details || important)
            state_->slots[slot_].details.push_back(escape_log(text));
    }
    void begin(const std::string& metric)
    {
        std::lock_guard<std::mutex> guard(state_->mutex);
        auto& slot = state_->slots[slot_];
        slot.metric = metric;
        slot.details.clear();
        slot.fraction = 0;
        slot.phase = "checking";
        slot.active = true;
        if (!state_->terminal)
            std::cout << "[" << escape_log(metric) << "]\n" << std::flush;
        state_->render(true);
    }
    void update(double fraction, const std::string& phase)
    {
        std::lock_guard<std::mutex> guard(state_->mutex);
        auto& slot = state_->slots[slot_];
        slot.fraction = std::max(slot.fraction, std::clamp(fraction, 0.0, 0.999));
        slot.phase = phase;
        state_->render();
    }
    void warning(const std::string& message)
    {
        std::lock_guard<std::mutex> guard(state_->mutex);
        state_->clear();
        std::cerr << "hta_fsck: warning: " << escape_log(message) << '\n';
        state_->render(true);
    }
    void request_stop()
    {
        std::lock_guard<std::mutex> guard(state_->mutex);
        if (state_->stopping)
            return;
        state_->stopping = true;
        state_->clear();
        std::cerr << "hta_fsck: stopping: no new metrics; finishing active metrics. "
                     "A second signal exits immediately and may leave unfinished recovery.\n";
        state_->render(true);
    }
    void finish(const std::string& status, const std::string& error = {})
    {
        std::lock_guard<std::mutex> guard(state_->mutex);
        auto& slot = state_->slots[slot_];
        state_->clear();
        std::cout << escape_log("[" + status + "] " + slot.metric) << '\n';
        for (const auto& detail : slot.details)
            std::cout << (state_->terminal ? "  " : "") << detail << '\n';
        slot.details.clear();
        std::cout << std::flush;
        if (!error.empty())
            std::cerr << "hta_fsck: " << escape_log(slot.metric + ": " + error) << '\n';
        ++state_->completed;
        slot.active = false;
        slot.fraction = 0;
        state_->render(true);
    }
    void final_bar()
    {
        std::lock_guard<std::mutex> guard(state_->mutex);
        state_->clear();
        if (state_->terminal)
            std::cerr << state_->bar(State::dimensions().ws_col - 1) << '\n';
    }
};
