#pragma once
#if __has_include(<filesystem>)
#include <filesystem>
#else
#include <experimental/filesystem>
namespace std
{
using namespace experimental;
}
#endif
