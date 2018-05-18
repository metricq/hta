#include <hta/hta.hpp>
#include <hta/ostream.hpp>

#include "../../include/hta/hta.hpp"
#include <nlohmann/json.hpp>

#include <filesystem>
#include <fstream>
#include <iostream>
#include <locale>

#include <cassert>

using json = nlohmann::json;

json read_json_from_file(const std::filesystem::path& path)
{
    std::ifstream config_file;
    config_file.exceptions(std::ios::badbit | std::ios::failbit);
    config_file.open(path);
    json config;
    config_file >> config;
    return config;
}

void raw_copy(hta::ReadMetric& src, hta::WriteMetric& dst, hta::Duration chunk_interval)
{
    auto range = src.range();
    size_t processed = 0;
    auto count = src.count();
    hta::TimePoint previous_time;
    for (auto t0 = range.first; t0 <= range.second; t0 += chunk_interval)
    {
        auto data =
            src.retrieve(t0, t0 + chunk_interval, { hta::Scope ::closed, hta::Scope ::open });
        for (auto tv : data)
        {
            processed++;
            if (processed % 100000000 == 0 || processed == count)
            {
                std::cout << processed << " / " << count << std::endl;
            }
            if (tv.time <= previous_time)
            {
                std::cout << "Non-Monotonic time " << tv.time << " at index " << (processed - 1)
                          << std::endl;
                continue;
            }
            previous_time = tv.time;
            dst.insert(tv);
        }
    }
}

int main(int argc, char* argv[])
{
    assert(argc == 4);
    std::string config_file = argv[1];
    std::string src_name = argv[2];
    std::string dst_name = argv[3];

    auto config = read_json_from_file(std::filesystem::path(config_file));
    hta::Directory directory(config);

    std::cout.imbue(std::locale(""));
    raw_copy(*directory.read_metric(src_name), *directory.write_metric(dst_name),
             hta::duration_cast(std::chrono::hours(1024)));
}
