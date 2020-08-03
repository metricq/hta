#include <hta/hta.hpp>

#include <aws/s3/S3Client.h>
#include <aws/s3/model/DeleteObjectRequest.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/PutObjectRequest.h>

#include <benchmark/benchmark.h>

#include <cassert>
#include <chrono>
#include <iostream>
#include <random>
#include <vector>

template <class T>
class S3Bench
{
    std::vector<T> write_chunk_;
    std::vector<T> read_chunk_;
};

static std::string key_prefix =
    std::to_string(std::chrono::duration_cast<std::chrono::seconds>(
                       std::chrono::system_clock::now().time_since_epoch())
                       .count());
static int key_count = 0;

template <class T>
class S3WriteFixture : public benchmark::Fixture
{
public:
    S3WriteFixture() : key("hta-bench-" + key_prefix + "-" + std::to_string(key_count++))
    {
    }

    void randomize(hta::TimeValue& tv, size_t i)
    {
        tv.time = hta::TimePoint(hta::Duration(1596471357000000000ll + i * 10000000ll));
        tv.value = r_dist(r_gen);
    }

    void SetUp(benchmark::State& st) override
    {
        data.resize(st.range(0));
        for (int64_t i = 0; i < st.range(0); i++)
        {
            randomize(data[i], i);
        }
    }

    void TearDown([[maybe_unused]] benchmark::State& st) override
    {
        assert(st.range(0) == int64_t(data.size()));
        auto r = client.DeleteObject(
            Aws::S3::Model::DeleteObjectRequest().WithKey(key).WithBucket(bucket));
        assert(r.IsSuccess());
    }

    Aws::S3::S3Client client;
    // TODO configurable
    std::string bucket = "hta-bench";
    std::string key;

    std::mt19937 r_gen{ 42 };
    std::normal_distribution<> r_dist{ 140, 50 };

    std::vector<T> data;
};

template <class T>
class S3ReadFixture : public S3WriteFixture<T>
{
public:
    S3ReadFixture()
    {
    }

    void SetUp(benchmark::State& st) override
    {
        S3WriteFixture<T>::SetUp(st);
        // TODO Write
    }
};

template <class T>
void write_stringstream(S3WriteFixture<T>& f)
{
    auto data_stream = Aws::MakeShared<Aws::StringStream>(
        "PutObjectInputStream",
        std::stringstream::in | std::stringstream::out | std::stringstream::binary);
    // Not the greatest moment in programming history.
    data_stream->write(reinterpret_cast<char*>(f.data.data()),
                       f.data.size() * sizeof(typename decltype(f.data)::value_type));

    Aws::S3::Model::PutObjectRequest request;
    request.SetBucket(f.bucket);
    request.SetKey(f.key);
    request.SetBody(data_stream);
    auto o = f.client.PutObject(request);
    assert(o.IsSuccess());
}

BENCHMARK_TEMPLATE_F(S3WriteFixture, WriteTimeValue, hta::TimeValue)(benchmark::State& st)
{
    for (auto _ : st)
    {
        write_stringstream(*this);
    }
}

int main()
{
}