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
#include "directory.hpp"
#include "metric.hpp"

#include <hta/filesystem.hpp>

#include <stdexcept>
#include <string>

namespace hta::storage::file
{
Directory::Directory(const std::filesystem::path& directory) : directory_(directory)
{
}

std::unique_ptr<storage::Metric> Directory::open(const std::string& name, OpenMode mode, Meta meta)
{
    auto path = directory_ / std::filesystem::path(name);
    // Don't do that in the metric itself (member initialization before constructor body)
    std::filesystem::create_directories(path);
    switch (mode)
    {
    case OpenMode::read:
        return std::make_unique<Metric>(FileOpenTag::Read(), path);
    case OpenMode::write:
        return std::make_unique<Metric>(FileOpenTag::Write(), path, meta);
    case OpenMode::read_write:
        return std::make_unique<Metric>(FileOpenTag::ReadWrite(), path, meta);
    default:
        throw std::logic_error("unknown OpenMode");
    }
}

std::vector<std::string> Directory::metric_names()
{
    std::vector<std::string> result;
    for (const std::filesystem::path &path : std::filesystem::directory_iterator(directory_))
    {
        if (std::filesystem::is_directory(path))
        {
            result.emplace_back(path.filename());
        }
    }
    std::sort(result.begin(), result.end());
    return result;
}
} // namespace hta::storage::file
