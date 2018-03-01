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
    json config;
    auto test_pwd = std::filesystem::current_path() / "hta_bench.tmp";
    config["type"] = "file";
    config["path"] = test_pwd;
    for (auto _ : state)
    {
        // Remove leftovers from past runs
        std::filesystem::remove_all(test_pwd);
        auto created = std::filesystem::create_directories(test_pwd);
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
