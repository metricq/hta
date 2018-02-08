#pragma once

#include <unordered_map>
#if __has_include(<filesystem>)
#include <filesystem>
#else
#include <experimental/filesystem>
namespace std
{
using namespace experimental;
}
#endif

namespace hta
{
class Metric;

namespace storage
{
    class Directory;
}

class Directory
{
public:
    Directory(const std::filesystem::path& config_path);
    Metric* operator[](const std::string& name);
    ~Directory();

private:
    std::unique_ptr<storage::Directory> directory_;
    std::unordered_map<std::string, std::unique_ptr<Metric>> metrics_;
};
}
