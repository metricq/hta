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
#pragma once

#include "../metric.hpp"

#include <hta/meta.hpp>

#include <aws/s3/S3Client.h>

#include <memory>

namespace hta::storage::s3
{
class Metric : public storage::Metric
{
public:
    /**
     * @param open_mode
     * @param client
     * @param bucket_name
     * @param raw_chunk_size number of elements of type TimeValue in each s3 object (NOT bytes)
     * @param hta_chunk_size number of elements of type TimeAggregate in each s3 object (NOT bytes)
     */
    Metric(Mode open_mode, Aws::S3::S3Client& client, const std::string& bucket_name,
           std::size_t raw_writer_buffer_size, std::size_t hta_chunk_size);

    Meta meta() const override;
    Mode mode() const override;

    void insert(TimeValue tv) override;
    void insert(Row row) override;

    void flush() override;

    std::vector<TimeValue> get(TimePoint begin, TimePoint end, IntervalScope scope) override;
    std::vector<TimeAggregate> get(TimePoint begin, TimePoint end, Duration interval,
                                   IntervalScope scope) override;

    size_t count(TimePoint begin, TimePoint end, IntervalScope scope) override;

    TimeValue last() override;
    TimeAggregate last(Duration interval) override;
    std::pair<TimePoint, TimePoint> range() override;

    std::size_t size() override;
    std::size_t size(Duration interval) override;

private:
    // I thought constexpr std::string was C++20. Is it not?
    // Doesn't work with gcc 10.1.0 or clang 10.0.1
    static constexpr char meta_key_[] = "meta";

private:
    Mode mode_;
    Meta meta_;

    Aws::S3::S3Client& client_;
    std::string bucket_name_;
    size_t hta_buffer_size_;
    std::vector<TimeValue> raw_write_buffer_;
    std::map<Duration, std::vector<TimeAggregate>> hta_write_buffers_;

    std::map<TimePoint, std::vector<TimeValue>> raw_read_buffer_;
    std::map<Duration, std::map<TimePoint, std::vector<TimeAggregate>>> hta_read_buffers_;
};
} // namespace hta::storage::s3