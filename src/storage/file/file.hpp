#pragma once

#include <algorithm>
#include <array>
#include <fstream>
#include <ios>
#include <string>
#include <type_traits>

#include <my_filesystem.hpp>

#include <cassert>
#include <cstdint>

using namespace std::literals::string_literals;

namespace hta::storage::file
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
        write_preamble(header);
    }

    File(FileOpenTag::Read, const std::filesystem::path& filename)
    : File(filename, std::ios_base::in)
    {
        read_preamble();
    }

    File(FileOpenTag::ReadWrite, const std::filesystem::path& filename, const HeaderType& header)
    : File(filename, std::ios_base::in | std::ios_base::out | std::ios_base::app)
    {
        // We need to use append mode otherwise it won't use O_CREAT.
        // Meanwhile app doesn't event seek to the end... oh my.
        stream_.seekp(0, stream_.end);
        if (stream_.tellp() == 0)
        {
            write_preamble(header);
        }
        else
        {
            stream_.seekg(0);
            read_preamble();
        }
    }

    HeaderType read_header()
    {
        HeaderType header;
        auto read_size = std::min(sizeof(header), data_begin_ - header_begin_);
        stream_.seekg(header_begin_);
        stream_.read(reinterpret_cast<char*>(&header), read_size);
        return header;
    }

    template <typename T>
    void write(const T& thing)
    {
        static_assert(std::is_trivially_copyable_v<T>, "T must be a POD.");
        stream_.write(reinterpret_cast<const char*>(&thing), sizeof(thing));
    }

    template <typename T>
    void read(T& thing, size_t pos)
    {
        stream_.seekg(data_begin_ + pos);
        read(thing);
    }

    void flush()
    {
        stream_.flush();
    }

    uint64_t size()
    {
        stream_.seekg(0, stream_.end);
        return stream_.tellg() - data_begin_;
    }

private:
    template <typename T>
    void read(T& thing)
    {
        static_assert(std::is_trivially_copyable_v<T>, "T must be a POD.");
        stream_.read(reinterpret_cast<char*>(&thing), sizeof(thing));
    }

    template <typename T>
    void read(std::vector<T>& vec)
    {
        static_assert(std::is_trivially_copyable_v<T>, "T must be a POD.");
        stream_.read(reinterpret_cast<char*>(vec.data()), sizeof(T) * vec.size());
    }

    void write_preamble(const HeaderType& header)
    {
        assert(stream_.tellp() == 0);
        stream_.write(magic_bytes.data(), magic_bytes.size());
        write(byte_order_mark);
        uint64_t header_size = sizeof(header);
        write(header_size);
        write(header);
        data_begin_ = header_begin_ + header_size;
        assert(stream_.tellp() == data_begin_);
    }

    void read_preamble()
    {
        std::array<char, 8> read_magic_bytes;
        read(read_magic_bytes);
        if (!std::equal(read_magic_bytes.begin(), read_magic_bytes.end(), magic_bytes.begin()))
        {
            throw std::runtime_error("Magic bytes of file do not match. Invalid or corrupt file:"s +
                                     filename_.string());
        }
        uint64_t read_bom;
        read(read_bom);
        if (read_bom != byte_order_mark)
        {
            throw std::runtime_error("Byte order mark does not match. File written with "
                                     "different endianess: "s +
                                     filename_.string());
        }

        uint64_t header_size;
        read(header_size);
        data_begin_ = header_begin_ + header_size;
    }

private:
    std::fstream stream_;
    std::filesystem::path filename_;
    static constexpr std::size_t header_begin_ =
        magic_bytes.size() + sizeof(byte_order_mark) + sizeof(uint64_t);
    // Currently unsupported by code, but used in file format for future use
    static constexpr uint64_t alignment_ = 1;
    std::size_t data_begin_;
};
}
