#include "../src/storage/s3/init.hpp"

#include <hta/hta.hpp>

#include <aws/s3/S3Client.h>
#include <aws/s3/model/DeleteObjectRequest.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/PutObjectRequest.h>

#include <nitro/env/get.hpp>
#include <nitro/except/exception.hpp>

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
    // TODO the client arguments should be configurable
    S3WriteFixture()
    : client(client_configuration(), Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never,
             false),
      bucket(nitro::env::get("HTA_S3_BUCKET")),
      key("hta-bench-" + key_prefix + "-" + std::to_string(key_count++))
    {
    }

private:
    static Aws::Client::ClientConfiguration client_configuration()
    {
        hta::storage::s3::AwsInit::instance();
        Aws::Client::ClientConfiguration config;
        try
        {
            auto verify = nitro::env::get("HTA_S3_VERIFY_SSL", nitro::env::no_default);
            // TODO replace with nitro foo
            std::transform(verify.begin(), verify.end(), verify.begin(), ::tolower);
            if (verify == "off" or verify == "false" or verify == "no")
            {
                config.verifySSL = false;
            }
        }
        catch (const nitro::except::exception&)
        {
        }
        try
        {
            config.endpointOverride = nitro::env::get("HTA_S3_ENDPOINT", nitro::env::no_default);
            std::cout << "using endpointOverride: " << config.endpointOverride << std::endl;
        }
        catch (const nitro::except::exception&)
        {
        }
        return config;
    }

    void randomize(hta::TimeValue& tv, size_t i)
    {
        tv.time = hta::TimePoint(hta::Duration(1596471357000000000ll + i * 10000000ll));
        tv.value = r_dist(r_gen);
    }

public:
    void SetUp(benchmark::State& st) override
    {
        //        std::cout << "S3WriteFixture::SetUp" << std::endl;
        data.resize(st.range(0));
        for (int64_t i = 0; i < st.range(0); i++)
        {
            randomize(data[i], i);
        }
    }

    void TearDown([[maybe_unused]] benchmark::State& st) override
    {
        //        std::cout << "S3WriteFixture::TearDown" << std::endl;

        assert(st.range(0) == int64_t(data.size()));
        auto r = client.DeleteObject(
            Aws::S3::Model::DeleteObjectRequest().WithKey(key).WithBucket(bucket));
        assert(r.IsSuccess());
    }

    Aws::S3::S3Client client;
    std::string bucket;
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
void write_stringstream(S3WriteFixture<T>& f, benchmark::State& st)
{
    auto data_stream = Aws::MakeShared<Aws::StringStream>(
        "PutObjectInputStream",
        std::stringstream::in | std::stringstream::out | std::stringstream::binary);
    // Not the greatest moment in programming history.
    auto bytes = f.data.size() * sizeof(typename decltype(f.data)::value_type);
    data_stream->write(reinterpret_cast<char*>(f.data.data()), bytes);

    Aws::S3::Model::PutObjectRequest request;
    request.SetBucket(f.bucket);
    request.SetKey(f.key);
    request.SetBody(data_stream);

    //    std::cout << "writing " << bytes << " bytes to object " << f.key << "@" << f.bucket
    //              << std::endl;

    auto o = f.client.PutObject(request);
    if (!o.IsSuccess())
    {
        std::cerr << "Error writing object: " << o.GetError().GetMessage() << "\n";
        throw std::runtime_error("S3 error");
    }

    st.SetBytesProcessed(bytes + st.bytes_processed());
}

BENCHMARK_TEMPLATE_DEFINE_F(S3WriteFixture, WriteTimeValue, hta::TimeValue)(benchmark::State& st)
{
    for (auto _ : st)
    {
        write_stringstream(*this, st);
    }
}
BENCHMARK_REGISTER_F(S3WriteFixture, WriteTimeValue)
    ->UseRealTime()
    ->RangeMultiplier(2)
    ->Range(1ll, 1ll << 20);

BENCHMARK_MAIN();