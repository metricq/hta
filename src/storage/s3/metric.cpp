// Copyright (c) 2020, ZIH,
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

#include "metric.hpp"

#include <nlohmann/json.hpp>

#include <aws/s3/model/GetObjectRequest.h>

namespace hta::storage::s3
{

Metric::Metric(Mode open_mode, Aws::S3::S3Client& client, const std::string& bucket_name,
               std::size_t raw_writer_buffer_size, std::size_t hta_chunk_size)
: mode_(open_mode), client_(client), bucket_name_(bucket_name), hta_buffer_size_(hta_chunk_size)
{
    raw_write_buffer_.reserve(raw_writer_buffer_size);

    Aws::S3::Model::GetObjectOutcome meta_outcome = client_.GetObject(
        Aws::S3::Model::GetObjectRequest().WithBucket(bucket_name_).WithKey(meta_key_));

    if (meta_outcome.IsSuccess())
    {
        if (mode_ == Mode::write)
        {
            throw std::runtime_error("meta object already exists when opening for write-only");
        }

        // I'm not sure the GetResultWithOwnership here is a good idea, but the AWS example says so
        auto& retrieved_file = meta_outcome.GetResultWithOwnership().GetBody();
        hta::json meta_json;
        retrieved_file >> meta_json;

        //        meta = Meta(meta_json);
    }
    else
    {
        auto err = meta_outcome.GetError();
        std::cout << "Error: GetObject: " << err.GetExceptionName() << ": " << err.GetMessage()
                  << std::endl;
    }
}

Meta Metric::meta() const
{
    return meta_;
}

hta::Mode Metric::mode() const
{
    return mode_;
}

void Metric::insert(hta::TimeValue tv)
{
    assert(raw_write_buffer_.size() < raw_write_buffer_.capacity());
    raw_write_buffer_.emplace_back(tv);

    // Check if we need to flush
}

void Metric::insert(hta::Row row)
{
    //    auto [it, _] = hta_write_buffers_.try_emplace(row.interval);
}

void Metric::flush()
{
    // We can't really flush in the non-appendable object storage
}

std::vector<TimeValue> Metric::get(hta::TimePoint begin, hta::TimePoint end,
                                   hta::IntervalScope scope)
{
    return std::vector<TimeValue>();
}
std::vector<TimeAggregate> Metric::get(hta::TimePoint begin, hta::TimePoint end,
                                       hta::Duration interval, hta::IntervalScope scope)
{
    return std::vector<TimeAggregate>();
}
size_t Metric::count(hta::TimePoint begin, hta::TimePoint end, hta::IntervalScope scope)
{
    return 0;
}
hta::TimeValue Metric::last()
{
    return hta::TimeValue();
}
hta::TimeAggregate Metric::last(hta::Duration interval)
{
    return hta::TimeAggregate();
}
std::pair<TimePoint, TimePoint> Metric::range()
{
    return std::pair<TimePoint, TimePoint>();
}
std::size_t Metric::size()
{
    return 0;
}
std::size_t Metric::size(hta::Duration interval)
{
    return 0;
}
} // namespace hta::storage::s3