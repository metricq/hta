#pragma once

#include <algorithm>
#include <array>
#include <fstream>
#include <ios>
#include <string>
#include <type_traits>

#include <hta/filesystem.hpp>

#include <cassert>
#include <cstdint>

using namespace std::literals::string_literals;

namespace hta::storage::file
{

namespace FileOpenTag
{
    class Write
    {
    };
    class Read
    {
    };
    class ReadWrite
    {
    };
}

template <class HeaderType, class T>
class File
{
private:
    // NEVER EVER CHANGE THESE TWO LINES
    static constexpr std::array<char, 8> magic_bytes = { 'H',        'T',  'A',        0x1a,
                                                         char(0xc5), 0x2c, char(0xcc), 0x1d };
    static constexpr uint64_t byte_order_mark = 0xf8f9fafbfcfdfeff;
    static_assert(std::is_pod_v<HeaderType>, "HeaderType must be a POD.");

    File(const std::filesystem::path& filename, std::ios_base::openmode mode) : filename_(filename)
    {
        stream_.exceptions(std::ios::badbit | std::ios::failbit);
        stream_.open(filename, mode | std::ios_base::binary);
    }

public:
    using pos_type = std::fstream::pos_type;
    using value_type = T;
    using size_type = std::size_t;

    static_assert(std::is_trivially_copyable_v<T>, "T must be a trivially copyable.");
    static constexpr size_type value_size = sizeof(T);

    File(FileOpenTag::Write, const std::filesystem::path& filename, const HeaderType& header)
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
            // TODO check compatibility of headers.
        }
    }

    const HeaderType& header() const
    {
        return header_;
    }

    void write(const value_type& thing)
    {
        stream_.write(reinterpret_cast<const char*>(&thing), value_size);
    }

    value_type read(pos_type pos)
    {
        stream_.seekg(data_begin_ + static_cast<pos_type>(pos * value_size));
        return read<value_type>();
    }

    void read(std::vector<value_type>& vector, pos_type pos)
    {
        stream_.seekg(data_begin_ + static_cast<pos_type>(pos * value_size));
        read(vector);
    }

    void flush()
    {
        stream_.flush();
    }

    size_type size()
    {
        stream_.seekg(0, stream_.end);
        auto size_bytes = ssize_t(stream_.tellg()) - ssize_t(data_begin_);
        assert(size_bytes >= 0);
        assert(0 == size_bytes % value_size);
        return size_bytes / value_size;
    }

private:
    template <typename TT>
    void write(const TT& thing)
    {
        static_assert(std::is_trivially_copyable_v<TT>, "Type must be a trivially copyable.");
        stream_.write(reinterpret_cast<const char*>(&thing), sizeof(thing));
    }

    template <typename TT>
    TT read()
    {
        static_assert(std::is_trivially_copyable_v<TT>, "Type must be a trivially copyable.");
        TT thing;
        stream_.read(reinterpret_cast<char*>(&thing), sizeof(thing));
        return thing;
    }

    void read(std::vector<value_type>& vec)
    {
        static_assert(std::is_trivially_copyable_v<T>, "T must be a POD.");
        stream_.exceptions(std::ios::badbit);
        stream_.read(reinterpret_cast<char*>(vec.data()), value_size * vec.size());
        if (stream_.eof())
        {
            // how much did we actually read...
            auto count = stream_.gcount();
            assert(0 == count % value_size);
            assert(count / value_size <= vec.size());
            vec.resize(count / value_size);
            stream_.clear();
        }
        stream_.exceptions(std::ios::badbit | std::ios::failbit);
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
        flush();
        size();
        header_ = header;
    }

    void read_preamble()
    {
        const auto read_magic_bytes = read<std::array<char, 8>>();
        if (!std::equal(read_magic_bytes.begin(), read_magic_bytes.end(), magic_bytes.begin()))
        {
            throw std::runtime_error("Magic bytes of file do not match. Invalid or corrupt file:"s +
                                     filename_.string());
        }
        const auto read_bom = read<uint64_t>();
        if (read_bom != byte_order_mark)
        {
            throw std::runtime_error("Byte order mark does not match. File written with "
                                     "different endianess: "s +
                                     filename_.string());
        }

        const auto header_size = read<uint64_t>();
        data_begin_ = header_begin_ + header_size;

        auto read_size =
            std::min<size_t>(sizeof(header_), data_begin_ - static_cast<pos_type>(header_begin_));
        stream_.seekg(header_begin_);
        stream_.read(reinterpret_cast<char*>(&header_), read_size);
    }

private:
    std::fstream stream_;
    std::filesystem::path filename_;
    // Apparently pos_type doesn't like to be constexpr m(
    static constexpr size_type header_begin_ =
        magic_bytes.size() + sizeof(byte_order_mark) + sizeof(uint64_t);
    // Currently unsupported by code, but used in file format for future use
    static constexpr uint64_t alignment_ = 1;
    pos_type data_begin_;
    HeaderType header_;
};
}
