#pragma once

#include <array>
#include <cstdint>

namespace hta::storage::file::format
{
// Shared on-disk preamble. Never change these values for the existing format.
inline constexpr std::array<char, 8> magic = { 'H',        'T',  'A',        0x1a,
                                               char(0xc5), 0x2c, char(0xcc), 0x1d };
inline constexpr uint64_t bom = 0xf8f9fafbfcfdfeff;
inline constexpr uint64_t header_begin = magic.size() + sizeof(bom) + sizeof(uint64_t);
} // namespace hta::storage::file::format
