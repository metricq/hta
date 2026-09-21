#pragma once

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <string>
#include <sys/ioctl.h>
#include <unistd.h>
#include <vector>

// Escape untrusted bytes before adding our own line separators/terminal codes.
// Escape backslashes as well so a literal "\\n" cannot impersonate a newline.
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

// The two live lines always stay below completed metrics. Pipes get plain logs.
class Progress
{
    using Clock = std::chrono::steady_clock;
    std::vector<std::string> details_;
    size_t total_, completed_ = 0;
    double fraction_ = 0;
    std::string metric_, phase_;
    Clock::time_point started_ = Clock::now(), rendered_ = started_;
    bool terminal_, visible_ = false;
    bool show_details_;

    static std::string duration(double seconds)
    {
        if (!std::isfinite(seconds) || seconds > 365.0 * 86400)
            return ">365d";
        auto s = static_cast<uint64_t>(std::max(0.0, seconds));
        if (s >= 3600)
            return std::to_string(s / 3600) + "h " + std::to_string(s / 60 % 60) + "m";
        return std::to_string(s / 60) + "m " + std::to_string(s % 60) + "s";
    }

    static std::string safe_line(std::string text, size_t width)
    {
        text = escape_log(text);
        if (text.size() > width)
            text = width <= 3 ? text.substr(0, width) : text.substr(0, width - 3) + "...";
        return text;
    }

    size_t width() const
    {
        winsize size{};
        return ::ioctl(STDERR_FILENO, TIOCGWINSZ, &size) == 0 && size.ws_col > 1 ? size.ws_col - 1 :
                                                                                   79;
    }

    std::string bar() const
    {
        const auto work = completed_ + fraction_;
        const double ratio = total_ ? std::min(1.0, work / total_) : 1.0;
        auto elapsed = std::chrono::duration<double>(Clock::now() - started_).count();
        const std::string eta = completed_ == total_ ? "0m 0s" :
                                work > 0 ? "~" + duration(elapsed * (total_ - work) / work) :
                                           "--";
        // ETA weights metrics equally and uses phase/record progress within one
        // metric. It is an estimate, especially for differently sized metrics.
        std::string suffix = " " + std::to_string(static_cast<int>(ratio * 100)) + "% " +
                             std::to_string(completed_) + "/" + std::to_string(total_) + " ETA " +
                             eta;
        const auto columns = width();
        const auto slots =
            columns > suffix.size() + 4 ? std::min<size_t>(30, columns - suffix.size() - 2) : 1;
        const auto filled = static_cast<size_t>(ratio * slots);
        return safe_line("[" + std::string(filled, '=') + std::string(slots - filled, ' ') + "]" +
                             suffix,
                         columns);
    }

public:
    explicit Progress(size_t total, bool show_details) : total_(total), show_details_(show_details)
    {
        const char* term = std::getenv("TERM");
        terminal_ = ::isatty(STDOUT_FILENO) && ::isatty(STDERR_FILENO) &&
                    (!term || std::string(term) != "dumb");
    }
    ~Progress()
    {
        clear();
    }
    bool interactive() const
    {
        return terminal_;
    }
    void detail_line(const std::string& text, bool important = false)
    {
        if (!terminal_)
            std::cout << escape_log(text) << '\n';
        else if (show_details_ || important)
            details_.push_back(escape_log(text));
    }

    void clear()
    {
        if (visible_)
        {
            std::cerr << "\r\033[2K\033[1A\r\033[2K" << std::flush;
            visible_ = false;
        }
    }

    void refresh(bool force = false)
    {
        if (!terminal_)
            return;
        const auto now = Clock::now();
        if (!force && now - rendered_ < std::chrono::milliseconds(100))
            return;
        clear();
        std::cerr << safe_line("Current: " + std::filesystem::path(metric_).filename().string() +
                                   " | " + phase_,
                               width())
                  << '\n'
                  << bar() << std::flush;
        visible_ = true;
        rendered_ = now;
    }

    void begin(const std::string& metric)
    {
        metric_ = metric;
        details_.clear();
        fraction_ = 0;
        phase_ = "checking";
        if (!terminal_)
            std::cout << "[" << escape_log(metric) << "]\n" << std::flush;
        refresh(true);
    }

    void update(double fraction, const std::string& phase)
    {
        fraction_ = std::max(fraction_, std::clamp(fraction, 0.0, 0.999));
        phase_ = phase;
        refresh();
    }

    void warning(const std::string& message)
    {
        clear();
        std::cerr << "hta_fsck: warning: " << escape_log(message) << '\n';
        refresh(true);
    }

    void finish(const std::string& status, const std::string& error = {})
    {
        clear();
        const auto text = "[" + status + "] " + metric_;
        std::cout << escape_log(text) << '\n';
        for (const auto& detail : details_)
            std::cout << "  " << detail << '\n';
        details_.clear();
        std::cout << std::flush;
        if (!error.empty())
            std::cerr << "hta_fsck: " << escape_log(metric_ + ": " + error) << '\n';
        ++completed_;
        fraction_ = 0;
    }

    void final_bar()
    {
        clear();
        if (terminal_)
            std::cerr << bar() << '\n';
    }
};
