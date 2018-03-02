#pragma once
#include <hta/chrono.hpp>

#include <nlohmann/json_fwd.hpp>

#include <cinttypes>

namespace hta
{
using json = nlohmann::json;

struct Meta
{
    // TODO try to delete the default constructor.
    Meta() = default;
    Meta(Duration interval_min, int64_t interval_factor)
    : interval_min(interval_min), interval_factor(interval_factor)
    {
    }
    Meta(const json& config);

    Duration interval_min = duration_cast(std::chrono::seconds(10));
    int64_t interval_factor = 10;
};
}
