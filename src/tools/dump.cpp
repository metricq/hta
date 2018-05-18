#include "../storage/file/file.hpp"
#include "../storage/file/metric.hpp"

#include <hta/ostream.hpp>

#include <iostream>

#include <cassert>

namespace hta::storage::file
{
void dump_header(const std::filesystem::path& path)
{
    bool is_raw;
    {
        File<Metric::Header, TimePoint> file{ FileOpenTag::Read(), path };
        auto header = file.header();
        std::cout << "HTA header of " << path << "\n";
        std::cout << "version: " << header.version << "\n";
        std::cout << "interval: " << header.interval << "\n";
        std::cout << "duration_period: " << header.duration_period.num << " / "
                  << header.duration_period.den << "\n";
        std::cout << "interval_min: " << header.interval_min << "\n";
        std::cout << "interval_factor: " << header.interval_factor << "\n";
        is_raw = (header.interval == 0);
    }
    if (is_raw)
    {
        File<Metric::Header, TimeValue> file{ FileOpenTag::Read(), path };
        if (file.size() > 0)
        {
            std::cout << "first timestamp: " << file.read(0).time << "\n";
            std::cout << "last timestamp " << file.read(file.size() - 1).time << "\n";
        }
        std::cout << file.size() << " raw values\n";
    }
    else
    {
        {
            File<Metric::Header, TimeAggregate> file{ FileOpenTag::Read(), path };
            if (file.size() > 0)
            {
                std::cout << "first timestamp: " << file.read(0).time << "\n";
                std::cout << "last timestamp " << file.read(file.size() - 1).time << "\n";
            }
            std::cout << file.size() << " aggregate values\n";
        }
    }
}
} // namespace hta::storage::file

int main(int argc, char* argv[])
{
    if (argc != 2)
    {
        return -1;
    }
    hta::storage::file::dump_header(argv[1]);
}
