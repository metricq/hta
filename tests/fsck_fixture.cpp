// Generate independent reference files through the normal HTA insertion path.
#include "../src/storage/file/metric.hpp"
#include <csignal>
#include <filesystem>
#include <hta/metric.hpp>
#include <memory>
#include <string>
#include <sys/resource.h>

// This deliberate memory fault must also run when the rest of the fixture is
// built with UBSan; otherwise UBSan exits before Memcheck observes the write.
__attribute__((no_sanitize("undefined"))) static void crash_for_harness()
{
    const rlimit limit{ 0, 0 };
    setrlimit(RLIMIT_CORE, &limit);
    volatile int* pointer = nullptr;
    *pointer = 1;
    std::raise(SIGSEGV);
}

int main(int argc, char** argv)
{
    if (argc == 2 && std::string(argv[1]) == "--crash")
    {
        // Test the harness itself: a signal must never pass as an expected error.
        crash_for_harness();
        return 0;
    }
    if (argc != 3)
        return 2;
    namespace file = hta::storage::file;
    std::filesystem::create_directories(argv[1]);
    hta::Meta meta(hta::Duration(1000), hta::Duration(1000000), 10);
    hta::Metric metric(std::make_unique<file::Metric>(file::FileOpenTag::Write(), argv[1], meta));
    int64_t time = 1600000000000000123LL;
    for (int i = 0; i < std::stoi(argv[2]); ++i)
    {
        // Irregular sampling, interval boundaries, negative/fractional values and gaps.
        time += i % 137 == 0 ? 23457 : (i % 7 + 1) * 113;
        metric.insert({ hta::TimePoint(hta::Duration(time)), (i % 79 - 39) / 7.0 });
    }
    metric.flush();
}
