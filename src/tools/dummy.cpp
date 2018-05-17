#include <hta/hta.hpp>

#include <nlohmann/json.hpp>

#include <filesystem>
#include <fstream>
#include <iostream>

#include <cstdint>
#include <cstdlib>

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

int main(int argc, char* argv[])
{
    std::string config_file = "config.json";
    std::int64_t count = 600000000;
    if (argc > 1)
    {
        config_file = argv[1];
    }
    if (argc > 2)
    {
        count = atoll(argv[2]);
    }
    std::string metric_name = "dummy";

    auto config = read_json_from_file(std::filesystem::path(config_file));
    hta::Directory out_directory(config);
    const auto& metric = out_directory[metric_name];
    for (std::int64_t row = 0; row < count; row++)
    {
        if (row % 1000000 == 0)
        {
            std::cout << row << " rows completed." << std::endl;
        }
        metric->insert(hta::TimeValue(
            hta::TimePoint(hta::duration_cast(std::chrono::milliseconds(1 + 50 * row))), 42.0));
    }
}
