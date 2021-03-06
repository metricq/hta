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

#include "../mode.hpp"

#include <hta/meta.hpp>
#include <hta/types.hpp>

#include <cstdint>
#include <vector>

namespace hta::storage
{
class json;

class Metric
{
public:
    virtual Meta meta() const = 0;
    virtual Mode mode() const = 0;

    virtual void insert(TimeValue tv) = 0;
    virtual void insert(Row row) = 0;

    virtual void flush() = 0;

    virtual std::vector<TimeValue> get(TimePoint begin, TimePoint end, IntervalScope scope) = 0;
    virtual std::vector<TimeAggregate> get(TimePoint begin, TimePoint end, Duration interval,
                                           IntervalScope scope) = 0;

    virtual size_t count(TimePoint begin, TimePoint end, IntervalScope scope) = 0;

    virtual TimeValue last() = 0;
    virtual TimeAggregate last(Duration interval) = 0;

    virtual std::size_t size() = 0;
    virtual std::size_t size(Duration interval) = 0;

    virtual std::pair<TimePoint, TimePoint> range() = 0;

    virtual ~Metric() = default;
};
} // namespace hta::storage
