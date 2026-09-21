#include "../src/tools/fsck_search.hpp"
#include <limits>
#include <random>
#include <stdexcept>
#include <vector>

static void check(bool ok)
{
    if (!ok)
        throw std::runtime_error("tail lower-bound regression");
}

struct VirtualRaw
{
    struct Point
    {
        uint64_t time;
    };
    uint64_t count, cache_begin, calls = 0, uncached = 0;
    uint64_t tail_begin() const
    {
        return cache_begin;
    }
    Point probe(uint64_t index)
    {
        check(index < count);
        check(++calls <= 256);
        uncached += index < cache_begin;
        return { index };
    }
};

int main()
{
    const uint64_t maximum = std::numeric_limits<uint64_t>::max();
    // Includes >100 GB logical raw files and uint64_t saturation boundaries.
    for (uint64_t count : { uint64_t(0), uint64_t(1), uint64_t(4095), uint64_t(4096),
                            uint64_t(4097), uint64_t(100000), uint64_t(1) << 33, maximum })
    {
        const std::vector<uint64_t> queries = { 0,     1,      count / 2, count ? count - 1 : 0,
                                                count, maximum };
        for (uint64_t cache_size : { uint64_t(0), uint64_t(1), uint64_t(4096) })
            for (auto query : queries)
            {
                VirtualRaw raw{ count, count - std::min(count, cache_size) };
                check(tail_lower_bound(raw, query) == std::min(query, count));
                if (query > raw.cache_begin && cache_size)
                    check(raw.uncached == 0);
            }
    }
    struct VectorRaw
    {
        std::vector<VirtualRaw::Point> points;
        uint64_t count, cached;
        uint64_t tail_begin() const
        {
            return cached;
        }
        VirtualRaw::Point probe(uint64_t index)
        {
            return points.at(index);
        }
    };
    std::mt19937 random(12345);
    for (size_t trial = 0; trial < 100; ++trial)
    {
        VectorRaw raw{ {}, random() % 20000, 0 };
        raw.cached = raw.count - std::min<uint64_t>(raw.count, random() % 5000);
        uint64_t time = 10;
        for (uint64_t i = 0; i < raw.count; ++i)
        {
            time += random() % 50; // Include duplicates: true lower-bound, not arbitrary match.
            raw.points.push_back({ time });
        }
        for (size_t query = 0; query < 200; ++query)
        {
            const uint64_t target = random() % (time + 100);
            const auto expected =
                std::lower_bound(raw.points.begin(), raw.points.end(), target,
                                 [](auto point, auto value) { return point.time < value; }) -
                raw.points.begin();
            check(tail_lower_bound(raw, target) == static_cast<uint64_t>(expected));
        }
    }
}
