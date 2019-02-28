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
#pragma once

#include <hta/filesystem.hpp>

#include <nlohmann/json_fwd.hpp>

#include <string>
#include <unordered_map>
#include <vector>

namespace hta
{
using json = nlohmann::json;

class ReadMetric;
class WriteMetric;
class ReadWriteMetric;

namespace storage
{
    class Directory;
}

class Directory
{
public:
    explicit Directory(const std::filesystem::path& config_path);
    explicit Directory(const json& config);
    ~Directory();

    ReadMetric* read_metric(const std::string& name);
    WriteMetric* write_metric(const std::string& name);
    ReadWriteMetric* operator[](const std::string& name);
    ReadWriteMetric* at(const std::string& name) const;

    std::vector<std::string> metric_names();

private:
    std::unique_ptr<storage::Directory> directory_;
    std::unordered_map<std::string, std::unique_ptr<ReadMetric>> read_metrics_;
    std::unordered_map<std::string, std::unique_ptr<WriteMetric>> write_metrics_;
    std::unordered_map<std::string, std::unique_ptr<ReadWriteMetric>> read_write_metrics_;
};
} // namespace hta
