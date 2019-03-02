// Copyright (c) 2019, ZIH,
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

#include "storage/directory.hpp"

#include <hta/metric.hpp>

#include <nlohmann/json.hpp>

namespace hta
{
using json = nlohmann::json;

VariantMetric::Variant make_variant(const std::string& name, const json& config,
                                    storage::Directory& storage)
{
    std::string mode = "RW"; // default
    if (config.count("mode"))
    {
        mode = config.at("mode").get<std::string>();
    }
    if (mode == "RW")
    {
        return VariantMetric::Variant(
            std::in_place_type<ReadWriteMetric>,
            storage.open(name, storage::OpenMode::read_write, Meta(config)));
    }
    if (mode == "R")
    {
        return VariantMetric::Variant(std::in_place_type<ReadMetric>,
                                      storage.open(name, storage::OpenMode::read, Meta(config)));
    }
    if (mode == "W")
    {
        return VariantMetric::Variant(std::in_place_type<WriteMetric>,
                                      storage.open(name, storage::OpenMode::write, Meta(config)));
    }
    throw std::runtime_error(std::string("unknown metric mode ") + mode +
                             " supported modes are RW,R,W");
}

VariantMetric::VariantMetric(const std::string& name, const hta::json& config,
                             storage::Directory& storage)
: metric_(make_variant(name, config, storage))
{
}

} // namespace hta