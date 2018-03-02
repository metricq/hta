#include <hta/meta.hpp>

#include <nlohmann/json.hpp>

namespace hta
{
using json = nlohmann::json;

Meta::Meta(const json& config)
{
    if (config.count("interval_min"))
    {
        interval_min = Duration(config["interval_min"]);
    }
    if (config.count("interval_factor"))
    {
        interval_factor = config["interval_factor"];
    }
}
}
