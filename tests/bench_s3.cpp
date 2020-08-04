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
#include <streambuf>
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
        auto err = o.GetError();
        std::cerr << "Error writing object: " << err.GetExceptionName() << ": " << err.GetMessage()
                  << std::endl;
        throw std::runtime_error("S3 error");
    }

    st.SetBytesProcessed(bytes + st.bytes_processed());
}

// Evil functions are inspired by https://github.com/aws/aws-sdk-cpp/issues/64
// Apparently there is no proper way to get/put from S3 with zero copy.
template <class T>
void write_evil(S3WriteFixture<T>& f, benchmark::State& st)
{
    // Q: Why the fuck is this a shared pointer to a stream?
    // A: Because request.SetBody only accepts shared pointers.
    auto data_stream = Aws::MakeShared<Aws::StringStream>("PleaseBeNiceToMe");

    // Not the greatest moment in programming history.

    auto bytes = f.data.size() * sizeof(typename decltype(f.data)::value_type);
    data_stream->rdbuf()->pubsetbuf(reinterpret_cast<char*>(f.data.data()), bytes);
    data_stream->rdbuf()->pubseekpos(bytes);
    data_stream->seekg(0);

    Aws::S3::Model::PutObjectRequest request;
    request.SetBucket(f.bucket);
    request.SetKey(f.key);
    request.SetBody(data_stream);

    //    std::cout << "writing " << bytes << " bytes to object " << f.key << "@" << f.bucket
    //              << std::endl;

    auto o = f.client.PutObject(request);
    if (!o.IsSuccess())
    {
        auto err = o.GetError();
        std::cerr << "Error writing object: " << err.GetExceptionName() << ": " << err.GetMessage()
                  << std::endl;
        throw std::runtime_error("S3 error");
    }

    st.SetBytesProcessed(bytes + st.bytes_processed());
}

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
        write_stringstream(*this, st);
        read_data.resize(st.range(0));
    }

    void check() const
    {
        if (read_data != this->data)
        {
            std::cerr << "read data does not match original\n";
            throw std::runtime_error("data does not match");
        }
    }

    std::vector<T> read_data;
};

template <class T>
void read_seek(S3ReadFixture<T>& f, [[maybe_unused]] benchmark::State& st)
{
    Aws::S3::Model::GetObjectOutcome get_object_outcome =
        f.client.GetObject(Aws::S3::Model::GetObjectRequest().WithBucket(f.bucket).WithKey(f.key));

    if (!get_object_outcome.IsSuccess())
    {
        auto err = get_object_outcome.GetError();
        std::cerr << "Error reading object: " << err.GetExceptionName() << ": " << err.GetMessage()
                  << std::endl;
        throw std::runtime_error("S3 error");
    }
    auto& retrieved_file = get_object_outcome.GetResultWithOwnership().GetBody();

    retrieved_file.seekg(0, std::ios::end);
    auto expected_bytes = f.data.size() * sizeof(typename decltype(f.data)::value_type);
    auto bytes = std::size_t(retrieved_file.tellg());
    if (bytes != expected_bytes)
    {
        std::cerr << "object has wrong size, expected: " << expected_bytes << ", actual: " << bytes
                  << std::endl;
        throw std::runtime_error("S3 size error");
    }
    retrieved_file.seekg(0, std::ios::beg);
    retrieved_file.read(reinterpret_cast<char*>(f.read_data.data()), bytes);
    f.check();
}

template <class T>
void read_evil(S3ReadFixture<T>& f, [[maybe_unused]] benchmark::State& st)
{
    auto expected_bytes = f.data.size() * sizeof(typename decltype(f.data)::value_type);

    Aws::S3::Model::GetObjectRequest request;
    request.SetBucket(f.bucket);
    request.SetKey(f.key);
    // Maybe it will like me better if I lie?
    request.SetResponseStreamFactory([&f, expected_bytes]() {
        std::unique_ptr<Aws::StringStream> stream(
            Aws::New<Aws::StringStream>("S3TheBestInterfaceInTheWorld"));
        stream->rdbuf()->pubsetbuf(reinterpret_cast<char*>(f.read_data.data()), expected_bytes);
        return stream.release();
    });
    Aws::S3::Model::GetObjectOutcome get_object_outcome = f.client.GetObject(request);
    if (!get_object_outcome.IsSuccess())
    {
        auto err = get_object_outcome.GetError();
        std::cerr << "Error reading object: " << err.GetExceptionName() << ": " << err.GetMessage()
                  << std::endl;
        throw std::runtime_error("S3 error");
    }

    f.check();
}

BENCHMARK_TEMPLATE_DEFINE_F(S3ReadFixture, ReadSeekTimeValue, hta::TimeValue)(benchmark::State& st)
{
    for (auto _ : st)
    {
        read_seek(*this, st);
    }
}
BENCHMARK_REGISTER_F(S3ReadFixture, ReadSeekTimeValue)
    ->UseRealTime()
    ->MeasureProcessCPUTime()
    ->RangeMultiplier(2)
    ->Range(1ll, 1ll << 20);

BENCHMARK_TEMPLATE_DEFINE_F(S3ReadFixture, ReadEvilTimeValue, hta::TimeValue)(benchmark::State& st)
{
    for (auto _ : st)
    {
        read_evil(*this, st);
    }
}
BENCHMARK_REGISTER_F(S3ReadFixture, ReadEvilTimeValue)
    ->UseRealTime()
    ->MeasureProcessCPUTime()
    ->RangeMultiplier(2)
    ->Range(1ll, 1ll << 20);

BENCHMARK_TEMPLATE_DEFINE_F(S3WriteFixture, WriteStringStreamTimeValue, hta::TimeValue)
(benchmark::State& st)
{
    for (auto _ : st)
    {
        write_stringstream(*this, st);
    }
}
BENCHMARK_REGISTER_F(S3WriteFixture, WriteStringStreamTimeValue)
    ->UseRealTime()
    ->MeasureProcessCPUTime()
    ->RangeMultiplier(2)
    ->Range(1ll, 1ll << 20);

BENCHMARK_TEMPLATE_DEFINE_F(S3WriteFixture, WriteEvilTimeValue, hta::TimeValue)
(benchmark::State& st)
{
    for (auto _ : st)
    {
        write_evil(*this, st);
    }
}
BENCHMARK_REGISTER_F(S3WriteFixture, WriteEvilTimeValue)
    ->UseRealTime()
    ->MeasureProcessCPUTime()
    ->RangeMultiplier(2)
    ->Range(1ll, 1ll << 20);

BENCHMARK_MAIN();