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
#include <hta/exception.hpp>
#include <hta/meta.hpp>
#include <hta/ostream.hpp>

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
    if (config.count("interval_max"))
    {
        interval_max = Duration(config["interval_max"]);
    }
    if (interval_min.count() % interval_factor != 0)
    {
        throw_exception("interval_min of ", interval_min, " not divisible by ", interval_factor);
    }
    if (interval_min.count() <= 0)
    {
        throw_exception("interval_min not positive ", interval_min);
    }
    if (interval_max < interval_min)
    {
        throw_exception("interval_max (", interval_max, ")not larger than interval_min (",
                        interval_min, ")");
    }
}
} // namespace hta
