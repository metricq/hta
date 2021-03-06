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

#include "hta.hpp"
#include <ostream>

namespace std
{
inline std::ostream& operator<<(std::ostream& os, hta::Duration duration)
{
    os << duration.count();
    return os;
}
} // namespace std
namespace hta
{
inline std::ostream& operator<<(std::ostream& os, hta::TimePoint tp)
{
    os << tp.time_since_epoch();
    return os;
}
inline std::ostream& operator<<(std::ostream& os, const Aggregate& aggregate)
{
    os << "min: " << aggregate.minimum << ", max: " << aggregate.maximum
       << ", mean: " << aggregate.mean() << ", count: " << aggregate.count
       << ", active: " << aggregate.active_time;
    return os;
}

inline std::ostream& operator<<(std::ostream& os, const Row& row)
{
    os << "[ " << row.time << " - " << row.end_time() << " ] (" << row.interval << ") "
       << row.aggregate;
    return os;
}
} // namespace hta
