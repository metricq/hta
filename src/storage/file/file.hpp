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
#pragma once

#include "../directory.hpp"

#include <hta/exception.hpp>
#include <hta/filesystem.hpp>

#include <algorithm>
#include <array>
#include <fstream>
#include <ios>
#include <string>
#include <type_traits>

#include <cassert>
#include <cerrno>
#include <cstdint>
#include <cstring>

using namespace std::literals::string_literals;

namespace hta::storage::file
{
namespace FileOpenTag
{
    struct Read
    {
        static constexpr Mode mode = Mode::read;
    };
    struct Write
    {
        static constexpr Mode mode = Mode::write;
    };
    struct ReadWrite
    {
        static constexpr Mode mode = Mode::read_write;
    };
} // namespace FileOpenTag

class Exception : public hta::Exception
{
public:
    Exception(const std::string& message, const std::filesystem::path& filename, int error_number)
    : hta::Exception(message + ": " + std::string(filename) + ": " + std::strerror(error_number)),
      filename_(filename), errno_(error_number)
    {
    }
    /**
     * errno
     */
    int error_number() const
    {
        return errno_;
    }
    const std::filesystem::path& filename() const
    {
        return filename_;
    }

private:
    std::filesystem::path filename_;
    int errno_;
};

template <class HeaderType, class T>
class File
{
private:
    // NEVER EVER CHANGE THESE TWO LINES
    static constexpr std::array<char, 8> magic_bytes = { 'H',        'T',  'A',        0x1a,
                                                         char(0xc5), 0x2c, char(0xcc), 0x1d };
    static constexpr uint64_t byte_order_mark = 0xf8f9fafbfcfdfeff;
    static_assert(std::is_pod_v<HeaderType>, "HeaderType must be a POD.");

public:
    using pos_type = std::fstream::pos_type;
    using value_type = T;
    using size_type = std::size_t;

    static_assert(std::is_trivially_copyable_v<T>, "T must be a trivially copyable.");
    static constexpr size_type value_size = sizeof(T);

private:
    File(const std::filesystem::path& filename, std::ios_base::openmode mode)
    : filename_(filename), stream_(filename, mode | std::ios_base::binary)
    {
        check_stream("open file");
    }

public:
    File(FileOpenTag::Read, const std::filesystem::path& filename)
    : File(filename, std::ios_base::in)
    {
        read_preamble();
    }

    File(FileOpenTag::Write, const std::filesystem::path& filename, const HeaderType& header)
    : File(filename, std::ios_base::trunc | std::ios_base::out)
    {
        write_preamble(header);
    }

    File(FileOpenTag::ReadWrite, const std::filesystem::path& filename, const HeaderType& header)
    : File(filename, std::ios_base::in | std::ios_base::out | std::ios_base::app)
    {
        // We need to use append mode otherwise it won't use O_CREAT.
        // Meanwhile app doesn't event seek to the end... oh my.
        stream_.seekp(0, std::fstream::end);
        check_stream("seekp to end");
        if (stream_.tellp() == 0)
        {
            write_preamble(header);
        }
        else
        {
            stream_.seekg(0);
            check_stream("seekg to begin");
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
        check_stream("write");
    }

    value_type read(pos_type pos)
    {
        stream_.seekg(data_begin_ + static_cast<pos_type>(pos * value_size));
        check_stream("seekg for read");
        return read<value_type>();
    }

    void read(std::vector<value_type>& vector, pos_type pos)
    {
        stream_.seekg(data_begin_ + static_cast<pos_type>(pos * value_size));
        check_stream("seekg for vector read");
        read(vector);
    }

    void flush()
    {
        stream_.flush();
        check_stream("flush");
    }

    size_type size()
    {
        stream_.seekg(0, std::fstream::end);
        check_stream("seekg to end for size");
        auto size_bytes = ssize_t(stream_.tellg()) - ssize_t(data_begin_);
        assert(size_bytes >= 0);
        // A file that is written in the background may have incomple records
        // during flush. It is useful to allow reading such a file.
        // assert(0 == size_bytes % value_size);
        return size_bytes / value_size;
    }

private:
    template <typename TT>
    void write(const TT& thing)
    {
        static_assert(std::is_trivially_copyable_v<TT>, "Type must be a trivially copyable.");
        stream_.write(reinterpret_cast<const char*>(&thing), sizeof(thing));
        check_stream("write");
    }

    template <typename TT>
    TT read()
    {
        static_assert(std::is_trivially_copyable_v<TT>, "Type must be a trivially copyable.");
        TT thing;
        stream_.read(reinterpret_cast<char*>(&thing), sizeof(thing));
        check_stream("read");
        return thing;
    }

    void read(std::vector<value_type>& vec)
    {
        static_assert(std::is_trivially_copyable_v<T>, "T must be a POD.");
        stream_.read(reinterpret_cast<char*>(vec.data()), value_size * vec.size());
        if (stream_.rdstate() == (std::ios_base::failbit | std::ios_base::eofbit))
        {
            // how much did we actually read...
            auto count = stream_.gcount();
            assert(0 == count % value_size);
            assert(count / value_size <= vec.size());
            vec.resize(count / value_size);
            stream_.clear();
        }
        else
        {
            check_stream("read vector");
        }
    }

    void write_preamble(const HeaderType& header)
    {
        assert(stream_.tellp() == 0);
        stream_.write(magic_bytes.data(), magic_bytes.size());
        check_stream("write magic bytes");
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
        check_stream("seekg header");
        stream_.read(reinterpret_cast<char*>(&header_), read_size);
        check_stream("read header");
        header_.restore(read_size);
    }

    // Get the char* because we might not need it and don't want to construct a temporary string
    // every time...
    // TODO use the fancy make_exception and variable template parameters to build
    // better error messages but lazy
    void check_stream(const char* message)
    {
        if (stream_.fail())
        {
            raise_stream_error(std::string("failed to ") + message);
        }
    }

    [[noreturn]] void raise_stream_error(const std::string& message) {
        throw Exception(message, filename_, errno);
    }

    private : std::filesystem::path filename_;
    std::fstream stream_;
    // Apparently pos_type doesn't like to be constexpr m(
    static constexpr size_type header_begin_ =
        magic_bytes.size() + sizeof(byte_order_mark) + sizeof(uint64_t);
    // Currently unsupported by code, but used in file format for future use
    static constexpr uint64_t alignment_ = 1;
    pos_type data_begin_;
    HeaderType header_;
};
} // namespace hta::storage::file
