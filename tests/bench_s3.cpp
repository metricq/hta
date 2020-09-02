#include "../src/storage/s3/init.hpp"

#include <hta/hta.hpp>

#include <aws/core/utils/Array.h>
#include <aws/core/utils/memory/AWSMemory.h>
#include <aws/core/utils/stream/PreallocatedStreamBuf.h>
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

static std::string key_prefix =
    std::to_string(std::chrono::duration_cast<std::chrono::seconds>(
                       std::chrono::system_clock::now().time_since_epoch())
                       .count());
static int key_count = 0;

class PreallocatedIOStream : public Aws::IOStream
{
public:
    PreallocatedIOStream(std::byte* memory, std::size_t size)
    : Aws::IOStream(new Aws::Utils::Stream::PreallocatedStreamBuf(
          reinterpret_cast<unsigned char*>(memory), size))
    {
    }

    ~PreallocatedIOStream()
    {
        delete rdbuf();
    }
};

template <class T>
class S3WriteFixture : public benchmark::Fixture
{
public:
    // TODO the client arguments should be configurable
    S3WriteFixture()
    : client(client_configuration(), Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never,
             false),
      bucket(nitro::env::get("HTA_S3_BUCKET")),
      key_("hta-bench-" + key_prefix + "-" + std::to_string(key_count++) + "-")
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
    Aws::Utils::Array<T>& write_buffer()
    {
        return *write_buffer_;
    }

    std::string key() const
    {
        return key_;
    }

    void SetUp(benchmark::State& st) override
    {
        assert(!write_buffer_);
        write_buffer_ = std::make_unique<Aws::Utils::Array<T>>(st.range(0));
        for (int64_t elem = 0; elem < st.range(0); elem++)
        {
            randomize(write_buffer()[elem], elem);
        }
    }

    void TearDown([[maybe_unused]] benchmark::State& st) override
    {
        auto r = client.DeleteObject(
            Aws::S3::Model::DeleteObjectRequest().WithKey(key()).WithBucket(bucket));
        assert(r.IsSuccess());
        write_buffer_.reset();
    }

    Aws::S3::S3Client client;
    std::string bucket;

    std::mt19937 r_gen{ 42 };
    std::normal_distribution<> r_dist{ 140, 50 };

private:
    std::string key_;

protected:
    std::unique_ptr<Aws::Utils::Array<T>> write_buffer_;
};

template <class T>
void write_stringstream(S3WriteFixture<T>& f, [[maybe_unused]] benchmark::State& st)
{
    assert(int64_t(f.write_buffer().GetLength()) == st.range(0));
    auto bytes = f.write_buffer().GetLength() * sizeof(T);

    auto data_stream = Aws::MakeShared<Aws::StringStream>(
        "PutObjectInputStream",
        std::stringstream::in | std::stringstream::out | std::stringstream::binary);
    // Not the greatest moment in programming history.
    data_stream->write(reinterpret_cast<char*>(f.write_buffer().GetUnderlyingData()), bytes);

    Aws::S3::Model::PutObjectRequest request;
    request.SetBucket(f.bucket);
    request.SetKey(f.key());
    request.SetBody(data_stream);

    //    std::cout << "writing " << bytes << " bytes to object " << f.key() << "@" << f.bucket
    //              << std::endl;

    auto o = f.client.PutObject(request);
    if (!o.IsSuccess())
    {
        auto err = o.GetError();
        std::cerr << "Error writing object: " << err.GetExceptionName() << ": " << err.GetMessage()
                  << std::endl;
        throw std::runtime_error("S3 error");
    }
}

// Evil functions are inspired by https://github.com/aws/aws-sdk-cpp/issues/64
// Apparently there is no proper way to get/put from S3 with zero copy.
template <class T>
void write_evil(S3WriteFixture<T>& f, [[maybe_unused]] benchmark::State& st)
{
    assert(int64_t(f.write_buffer().GetLength()) == st.range(0));
    auto bytes = f.write_buffer().GetLength() * sizeof(T);

    // Q: Why the fuck is this a shared pointer to a stream?
    // A: Because request.SetBody only accepts shared pointers.
    auto data_stream = Aws::MakeShared<Aws::StringStream>("PleaseBeNiceToMe");

    data_stream->rdbuf()->pubsetbuf(reinterpret_cast<char*>(f.write_buffer().GetUnderlyingData()),
                                    bytes);
    data_stream->rdbuf()->pubseekpos(bytes);
    data_stream->seekg(0);

    Aws::S3::Model::PutObjectRequest request;
    request.SetBucket(f.bucket);
    request.SetKey(f.key());
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
}

// This is why we need to endure Aws::Array
// https://github.com/aws/aws-sdk-cpp/issues/785
template <class T>
void write_preallocatedstreambuf(S3WriteFixture<T>& f, [[maybe_unused]] benchmark::State& st)
{
    assert(int64_t(f.write_buffer().GetLength()) == st.range(0));
    auto bytes = f.write_buffer().GetLength() * sizeof(T);

    auto preallocated_stream = Aws::MakeShared<PreallocatedIOStream>(
        "", reinterpret_cast<std::byte*>(f.write_buffer().GetUnderlyingData()), bytes);

    Aws::S3::Model::PutObjectRequest request;
    request.SetBucket(f.bucket);
    request.SetKey(f.key());
    request.SetBody(preallocated_stream);

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
        read_buffer_ = std::make_unique<Aws::Utils::Array<T>>(st.range(0));
    }

    void TearDown([[maybe_unused]] benchmark::State& st) override
    {
        // Checking just once for now so it doesn't impact performance
        check();
        read_buffer_.reset();
        S3WriteFixture<T>::TearDown(st);
    }

    void check()
    {
        if (read_buffer() != this->write_buffer())
        {
            std::cerr << "read data does not match original\n";
            throw std::runtime_error("data does not match");
        }
    }

    Aws::Utils::Array<T>& read_buffer()
    {
        return *read_buffer_;
    }

private:
    std::unique_ptr<Aws::Utils::Array<T>> read_buffer_;
};

template <class T>
void read_seek(S3ReadFixture<T>& f, [[maybe_unused]] benchmark::State& st)
{
    assert(f.read_buffer().GetLength() == std::size_t(st.range(0)));

    Aws::S3::Model::GetObjectOutcome get_object_outcome = f.client.GetObject(
        Aws::S3::Model::GetObjectRequest().WithBucket(f.bucket).WithKey(f.key()));

    if (!get_object_outcome.IsSuccess())
    {
        auto err = get_object_outcome.GetError();
        std::cerr << "Error reading object: " << f.key() << "@" << f.bucket << ": "
                  << err.GetExceptionName() << ": " << err.GetMessage() << std::endl;
        throw std::runtime_error("S3 error");
    }
    auto& retrieved_file = get_object_outcome.GetResultWithOwnership().GetBody();

    retrieved_file.seekg(0, std::ios::end);
    auto expected_bytes = f.read_buffer().GetLength() * sizeof(T);
    auto bytes = std::size_t(retrieved_file.tellg());
    if (bytes != expected_bytes)
    {
        std::cerr << "object has wrong size, expected: " << expected_bytes << ", actual: " << bytes
                  << std::endl;
        throw std::runtime_error("S3 size error");
    }
    retrieved_file.seekg(0, std::ios::beg);
    retrieved_file.read(reinterpret_cast<char*>(f.read_buffer().GetUnderlyingData()), bytes);
}

template <class T>
void read_evil(S3ReadFixture<T>& f, [[maybe_unused]] benchmark::State& st)
{
    assert(f.read_buffer().GetLength() == std::size_t(st.range(0)));

    auto bytes = f.read_buffer().GetLength() * sizeof(T);

    Aws::S3::Model::GetObjectRequest request;
    request.SetBucket(f.bucket);
    request.SetKey(f.key());
    // Maybe it will like me better if I lie?
    request.SetResponseStreamFactory([&f, bytes]() {
        std::unique_ptr<Aws::StringStream> stream(
            Aws::New<Aws::StringStream>("S3TheBestInterfaceInTheWorld"));
        stream->rdbuf()->pubsetbuf(reinterpret_cast<char*>(f.read_buffer().GetUnderlyingData()),
                                   bytes);
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
}

template <class T>
void read_preallocatedstreambuf(S3ReadFixture<T>& f, [[maybe_unused]] benchmark::State& st)
{
    assert(f.read_buffer().GetLength() == std::size_t(st.range(0)));

    auto bytes = f.read_buffer().GetLength() * sizeof(T);
    Aws::S3::Model::GetObjectRequest request;
    request.SetBucket(f.bucket);
    request.SetKey(f.key());

    request.SetResponseStreamFactory([&f, bytes]() {
        return Aws::New<PreallocatedIOStream>(
            "", reinterpret_cast<std::byte*>(f.read_buffer().GetUnderlyingData()), bytes);
    });
    Aws::S3::Model::GetObjectOutcome get_object_outcome = f.client.GetObject(request);
    if (!get_object_outcome.IsSuccess())
    {
        auto err = get_object_outcome.GetError();
        std::cerr << "Error reading object: " << err.GetExceptionName() << ": " << err.GetMessage()
                  << std::endl;
        throw std::runtime_error("S3 error");
    }
}

template <class T>
class S3ReadRangeFixture : public S3WriteFixture<T>
{
public:
    S3ReadRangeFixture()
    {
    }

    void SetUp(benchmark::State& st) override
    {
        S3WriteFixture<T>::SetUp(st);
        write_stringstream(*this, st);
        read_buffer_ = std::make_unique<Aws::Utils::Array<T>>(st.range(1));
    }

    void TearDown([[maybe_unused]] benchmark::State& st) override
    {
        read_buffer_.reset();
        S3WriteFixture<T>::TearDown(st);
    }

    Aws::Utils::Array<T>& read_buffer()
    {
        return *read_buffer_;
    }

private:
    std::unique_ptr<Aws::Utils::Array<T>> read_buffer_;
};

template <class T>
void read_range(S3ReadRangeFixture<T>& f, [[maybe_unused]] benchmark::State& st)
{
    assert(f.read_buffer().GetLength() == std::size_t(st.range(1)));

    Aws::S3::Model::GetObjectRequest request;
    request.SetBucket(f.bucket);
    request.SetKey(f.key());
    assert(st.range(0) >= st.range(1));
    auto range_size = st.range(1);
    auto bytes = range_size * sizeof(T);
    auto dist = std::uniform_int_distribution<int64_t>(0, st.range(0) - range_size);
    auto range_begin = dist(f.r_gen);
    assert(range_begin >= 0);
    assert(range_begin < int64_t(f.write_buffer().GetLength()));
    auto range_end = range_begin + range_size;
    assert(range_end > range_begin);
    assert(range_end <= int64_t(f.write_buffer().GetLength()));

    // Because a high-level API would totally ask you to build that string yourself.
    // Oh wait, it's not a high-level API
    auto range_str = std::string("bytes=") + std::to_string(range_begin * sizeof(T)) + "-" +
                     std::to_string(range_end * sizeof(T));
    request.SetRange(range_str);

    request.SetResponseStreamFactory([&f, bytes]() {
        return Aws::New<PreallocatedIOStream>(
            "", reinterpret_cast<std::byte*>(f.read_buffer().GetUnderlyingData()), bytes);
    });
    Aws::S3::Model::GetObjectOutcome get_object_outcome = f.client.GetObject(request);
    if (!get_object_outcome.IsSuccess())
    {
        auto err = get_object_outcome.GetError();
        std::cerr << "Error reading object: " << err.GetExceptionName() << ": " << err.GetMessage()
                  << std::endl;
        throw std::runtime_error("S3 error");
    }

    // check

    for (int64_t i = 0; i < st.range(1); i++)
    {
        if (f.read_buffer()[i] != f.write_buffer()[i + range_begin])
        {
            std::cerr << "Read data doesn't match\n";
            throw std::runtime_error("read data doesn't match.");
        }
    }
}

constexpr auto range_multiplier = 2;
constexpr auto range_min = 1ll;
constexpr auto range_max = 1ll << 27;

BENCHMARK_TEMPLATE_DEFINE_F(S3ReadFixture, ReadSeekTimeValue, hta::TimeValue)(benchmark::State& st)
{
    for (auto _ : st)
    {
        read_seek(*this, st);
    }
    st.SetBytesProcessed(st.range(0) * sizeof(hta::TimeValue) * st.iterations());
}
BENCHMARK_REGISTER_F(S3ReadFixture, ReadSeekTimeValue)
    ->UseRealTime()
    ->MeasureProcessCPUTime()
    ->Unit(benchmark::kMillisecond)
    ->RangeMultiplier(range_multiplier)
    ->Range(range_min, range_max);

BENCHMARK_TEMPLATE_DEFINE_F(S3ReadFixture, ReadEvilTimeValue, hta::TimeValue)(benchmark::State& st)
{
    for (auto _ : st)
    {
        read_evil(*this, st);
    }
    st.SetBytesProcessed(st.range(0) * sizeof(hta::TimeValue) * st.iterations());
}
BENCHMARK_REGISTER_F(S3ReadFixture, ReadEvilTimeValue)
    ->UseRealTime()
    ->MeasureProcessCPUTime()
    ->Unit(benchmark::kMillisecond)
    ->RangeMultiplier(range_multiplier)
    ->Range(range_min, range_max);

BENCHMARK_TEMPLATE_DEFINE_F(S3ReadFixture, ReadPreallocTimeValue, hta::TimeValue)
(benchmark::State& st)
{
    for (auto _ : st)
    {
        read_preallocatedstreambuf(*this, st);
    }
    st.SetBytesProcessed(st.range(0) * sizeof(hta::TimeValue) * st.iterations());
}
BENCHMARK_REGISTER_F(S3ReadFixture, ReadPreallocTimeValue)
    ->UseRealTime()
    ->MeasureProcessCPUTime()
    ->Unit(benchmark::kMillisecond)
    ->RangeMultiplier(range_multiplier)
    ->Range(range_min, range_max);

BENCHMARK_TEMPLATE_DEFINE_F(S3WriteFixture, WriteStringStreamTimeValue, hta::TimeValue)
(benchmark::State& st)
{
    for (auto _ : st)
    {
        write_stringstream(*this, st);
    }
    st.SetBytesProcessed(st.range(0) * sizeof(hta::TimeValue) * st.iterations());
}
BENCHMARK_REGISTER_F(S3WriteFixture, WriteStringStreamTimeValue)
    ->UseRealTime()
    ->MeasureProcessCPUTime()
    ->Unit(benchmark::kMillisecond)
    ->RangeMultiplier(range_multiplier)
    ->Range(range_min, range_max);

BENCHMARK_TEMPLATE_DEFINE_F(S3WriteFixture, WriteEvilTimeValue, hta::TimeValue)
(benchmark::State& st)
{
    for (auto _ : st)
    {
        write_evil(*this, st);
    }
    st.SetBytesProcessed(st.range(0) * sizeof(hta::TimeValue) * st.iterations());
}
BENCHMARK_REGISTER_F(S3WriteFixture, WriteEvilTimeValue)
    ->UseRealTime()
    ->MeasureProcessCPUTime()
    ->Unit(benchmark::kMillisecond)
    ->RangeMultiplier(range_multiplier)
    ->Range(range_min, range_max);

BENCHMARK_TEMPLATE_DEFINE_F(S3WriteFixture, WritePreallocTimeValue, hta::TimeValue)
(benchmark::State& st)
{
    for (auto _ : st)
    {
        write_preallocatedstreambuf(*this, st);
    }
    st.SetBytesProcessed(st.range(0) * sizeof(hta::TimeValue) * st.iterations());
}
BENCHMARK_REGISTER_F(S3WriteFixture, WritePreallocTimeValue)
    ->UseRealTime()
    ->MeasureProcessCPUTime()
    ->Unit(benchmark::kMillisecond)
    ->RangeMultiplier(range_multiplier)
    ->Range(range_min, range_max);

BENCHMARK_TEMPLATE_DEFINE_F(S3ReadRangeFixture, ReadRangeTimeValue, hta::TimeValue)
(benchmark::State& st)
{
    for (auto _ : st)
    {
        read_range(*this, st);
    }
    st.SetBytesProcessed(st.range(1) * sizeof(hta::TimeValue) * st.iterations());
}

static void range_arguments(benchmark::internal::Benchmark* b)
{
    for (int64_t size = range_min; size <= range_max; size *= range_multiplier)
    {
        for (int64_t read_size : { 1, 64, 65, 128 })
        {
            if (read_size >= size)
            {
                continue;
            }
            b->Args({ size, read_size });
        }
    }
}
BENCHMARK_REGISTER_F(S3ReadRangeFixture, ReadRangeTimeValue)
    ->UseRealTime()
    ->MeasureProcessCPUTime()
    ->Unit(benchmark::kMillisecond)
    ->RangeMultiplier(range_multiplier)
    ->Apply(range_arguments);

BENCHMARK_MAIN();