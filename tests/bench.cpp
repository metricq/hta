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
#include <hta/chrono.hpp>
#include <hta/directory.hpp>
#include <hta/filesystem.hpp>
#include <hta/metric.hpp>

#include <nlohmann/json.hpp>

#include <benchmark/benchmark.h>

#include <chrono>

using json = nlohmann::json;

void BM_insert(benchmark::State& state)
{
    auto test_pwd = std::filesystem::current_path() / "hta_bench.tmp";
    json config = {
        { "type", "file" },
        { "path", test_pwd },
        { "metrics",
          {
              { "foo", {} },
          } },
    };
    for (auto _ : state)
    {
        // Remove leftovers from past runs
        std::filesystem::remove_all(test_pwd);
        auto created = std::filesystem::create_directories(test_pwd);
        (void)created;
        assert(created);
        hta::Directory dir(config);
        auto metric = dir["foo"];

        auto time = hta::TimePoint(hta::Duration(1519832293179227888));
        auto time_delta = hta::Duration(state.range(1));
        auto start = std::chrono::high_resolution_clock::now();
        for (int64_t i = 0; i < state.range(0); i++)
        {
            metric->insert({ time, i + 1. / 3. });
            time += time_delta;
        }
        auto end = std::chrono::high_resolution_clock::now();

        auto elapsed_seconds =
            std::chrono::duration_cast<std::chrono::duration<double>>(end - start);
        state.SetIterationTime(elapsed_seconds.count());
        state.SetBytesProcessed(state.range(0));
    }
}
static void CustomArguments(benchmark::internal::Benchmark* b)
{
    for (int count = (1 << 10); count <= (1 << 26); count *= 4)
        for (int time_delta : { 1000000000ll / 1210000, 1000000000ll / 20, 1000000000ll })
            b->Args({ count, time_delta });
}
BENCHMARK(BM_insert)->Apply(CustomArguments)->UseManualTime();

BENCHMARK_MAIN();
