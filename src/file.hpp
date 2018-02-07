#pragma once

#include <algorithm>
#include <array>
#if __has_include(<filesystem>)
#include <filesystem>
#else
#include <experimental/filesystem>
namespace std
{
using namespace experimental;
}
#endif
#include <fstream>
#include <ios>
#include <string>
#include <type_traits>

#include <cassert>
#include <cstdint>

using namespace std::literals::string_literals;

namespace hta
{

namespace FileOpenTag
{
    class Create
    {
    };
    class Read
    {
    };
    class ReadWrite
    {
    };
}

template <class HeaderType>
class File
{
private:
    // NEVER EVER CHANGE THESE TWO LINES
    static constexpr std::array<char, 8> magic_bytes = {
        'H', 'T', 'A', 0x1a, 0xc5, 0x2c, 0xcc, 0x1d
    };
    static constexpr uint64_t byte_order_mark = 0xf8f9fafbfcfdfeff;
    static_assert(std::is_pod_v<HeaderType>, "HeaderType must be a POD.");

    File(const std::filesystem::path& filename, std::ios_base::openmode mode) : filename_(filename)
    {
        stream_.exceptions(std::ios::badbit | std::ios::failbit);
        stream_.open(filename, mode | std::ios_base::binary);
    }

public:
    File(FileOpenTag::Create, const std::filesystem::path& filename, const HeaderType& header)
    : File(filename, std::ios_base::trunc | std::ios_base::out)
    {
        stream_.write(magic_bytes.data(), magic_bytes.size());
        write_pod(byte_order_mark);
        uint64_t header_size = sizeof(header);
        write_pod(header_size);
        write_pod(header);
        data_begin_ = header_begin_ + header_size;
        assert(stream_.tellg() == data_begin_);
    }

    File(FileOpenTag::Read, const std::filesystem::path& filename)
    : File(filename, std::ios_base::in)
    {
        read_prefix();
    }

    File(FileOpenTag::ReadWrite, const std::filesystem::path& filename)
    : File(filename, std::ios_base::in | std::ios_base::out)
    {
        read_prefix();
    }

    HeaderType read_header()
    {
        HeaderType header;
        auto read_size = std::min(sizeof(header), data_begin_ - header_begin_);
        stream_.read(reinterpret_cast<char*>(&header), read_size);
        return header;
    }

private:
    void read_prefix()
    {
        std::array<char, 8> read_magic_bytes;
        read_pod(read_magic_bytes);
        if (!std::equal(read_magic_bytes.begin(), read_magic_bytes.end(), magic_bytes.begin()))
        {
            throw std::runtime_error("Magic bytes of file do not match. Invalid or corrupt file:"s +
                                     filename_.string());
        }
        uint64_t read_bom;
        read_pod(read_bom);
        if (read_bom != byte_order_mark)
        {
            throw std::runtime_error("Byte order mark does not match. File written with "
                                     "different endianess: "s +
                                     filename_.string());
        }

        uint64_t header_size;
        read_pod(header_size);
        data_begin_ = header_begin_ + header_size;
    }

    template <typename T>
    void write_pod(const T& thing)
    {
        static_assert(std::is_pod_v<T>, "T must be a POD.");
        stream_.write(reinterpret_cast<char*>(&thing), sizeof(thing));
    }

    template <typename T>
    void read_pod(T& thing)
    {
        static_assert(std::is_pod_v<T>, "T must be a POD.");
        stream_.read(reinterpret_cast<char*>(&thing), sizeof(thing));
    }

private:
    std::fstream stream_;
    std::filesystem::path filename_;
    static constexpr std::size_t header_begin_ = magic_bytes.size() + sizeof(byte_order_mark);
    std::size_t data_begin_;
};
}
