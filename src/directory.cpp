// Copyright (c) 2018, ZIH,
// Technische Universitaet Dresden,
// Federal Republic of Germany
//
// All rights reserved.
//
// Redistribution and use in source and binary forms, with or without modification,
// are permitted provided that the following conditions are met:
//
//     * Redistributions of source code must retain the above copyright notice,
//       this list of conditions and the following disclaimer.
//     * Redistributions in binary form must reproduce the above copyright notice,
//       this list of conditions and the following disclaimer in the documentation
//       and/or other materials provided with the distribution.
//     * Neither the name of metricq nor the names of its contributors
//       may be used to endorse or promote products derived from this software
//       without specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
// "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
// LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
// A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR
// CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
// EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
// PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
// PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
// LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
// NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
// SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
#include "storage/file/directory.hpp"

#include <hta/directory.hpp>
#include <hta/exception.hpp>
#include <hta/filesystem.hpp>
#include <hta/metric.hpp>

#include <nlohmann/json.hpp>

#include <cassert>
#include <memory>
#include <string>

namespace hta
{

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

Directory::Directory(const json& config)
{
    auto type = config.at("type").get<std::string>();
    if (type == "file")
    {
        directory_ = std::make_unique<storage::file::Directory>(config["path"].get<std::string>());
    }
    else
    {
        throw_exception("Unknown directory type: ", type);
    }

    if (config.count("metrics"))
    {
        auto& metrics = config.at("metrics");
        if (metrics.is_array())
        {
            // Legacy, TODO remove, doesn't support prefixes!
            for (const auto& metric_config : metrics)
            {
                auto name = metric_config.at("name").get<std::string>();
                metrics_.emplace(std::piecewise_construct, std::forward_as_tuple(name),
                                 std::forward_as_tuple(name, metric_config, *directory_));
            }
        }
        assert(metrics.is_object());
        for (const auto& elem : metrics.items())
        {
            const auto name = elem.key();
            const auto& metric_config = elem.value();

            if (metric_config.count("prefix") && metric_config.at("prefix").get<bool>())
            {
                prefixes_.emplace_back(name, metric_config);
            }
            else
            {
                metrics_.emplace(std::piecewise_construct, std::forward_as_tuple(name),
                                 std::forward_as_tuple(name, metric_config, *directory_));
            }
        }
    }
}

Directory::Directory(const std::filesystem::path& config_path)
: Directory(read_json_from_file(config_path))
{
}

std::vector<std::string> Directory::metric_names()
{
    return directory_->metric_names();
}

VariantMetric& Directory::create_metric(const std::string& name)
{
    for (const auto& elem : prefixes_)
    {
        const auto& prefix = elem.first;
        if (prefix == name.substr(0, prefix.size()))
        {
            auto it = metrics_.emplace(std::piecewise_construct, std::forward_as_tuple(name),
                                       std::forward_as_tuple(name, elem.second, *directory_));
            assert(it.second);
            return it.first->second;
        }
    }
    throw Exception(std::string("no settings found to create metric ") + name);
}

Directory::~Directory() = default;
} // namespace hta
