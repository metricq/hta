# Copyright (c) 2019, ZIH, Technische Universitaet Dresden, Federal Republic of Germany
#
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without modification,
# are permitted provided that the following conditions are met:
#
#     * Redistributions of source code must retain the above copyright notice,
#       this list of conditions and the following disclaimer.
#     * Redistributions in binary form must reproduce the above copyright notice,
#       this list of conditions and the following disclaimer in the documentation
#       and/or other materials provided with the distribution.
#     * Neither the name of metricq nor the names of its contributors
#       may be used to endorse or promote products derived from this software
#       without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
# "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
# LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
# A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR
# CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
# EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
# PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
# PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
# LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
# NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
# SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

include(FindPackageHandleStandardArgs)
include(CheckCXXSymbolExists)

if (StdFilesystem_LIBRARY)
    set(StdFilesystem_FIND_QUIETLY TRUE)
endif()

# detect the stdlib we are using
CHECK_CXX_SYMBOL_EXISTS(_LIBCPP_VERSION ciso646 HAS_LIBCXX)
CHECK_CXX_SYMBOL_EXISTS(__GLIBCXX__ ciso646 HAS_LIBSTDCXX)

# detect if the headers are present
set(CMAKE_REQUIRED_FLAGS "-std=c++17")
CHECK_CXX_SYMBOL_EXISTS(std::filesystem::status_known filesystem HAS_STD_FILESYSTEM)
CHECK_CXX_SYMBOL_EXISTS(std::experimental::filesystem::status_known experimental/filesystem HAS_EXPERIMENTAL_STD_FILESYSTEM)
unset(CMAKE_REQUIRED_LIBRARIES)

# check if we need to link an additional library
if(HAS_STD_FILESYSTEM)
    CHECK_CXX_SYMBOL_EXISTS(std::filesystem::exists filesystem NEEDS_STD_FILESYSTEM_LIBRARY)
elseif(HAS_EXPERIMENTAL_STD_FILESYSTEM)
    CHECK_CXX_SYMBOL_EXISTS(std::experimental::filesystem::exists filesystem _DONT_NEED_STD_FILESYSTEM_LIBRARY)
endif()
unset(CMAKE_REQUIRED_FLAGS)

# if we only have it in experimental, we still have it
if(HAS_EXPERIMENTAL_STD_FILESYSTEM)
    set(HAS_STD_FILESYSTEM TRUE)
endif()

if(NOT _DONT_NEED_STD_FILESYSTEM_LIBRARY)
    if(HAS_LIBSTDCXX)
        set(StdFSLibName "stdc++fs")
        find_library(StdFilesystem_LIBRARY NAMES ${StdFSLibName} HINTS ENV LD_LIBRARY_PATH ENV LIBRARY_PATH ENV DYLD_LIBRARY_PATH)
    elseif(HAS_LIBCXX)
        set(StdFSLibName "c++fs")
        find_library(StdFilesystem_LIBRARY NAMES ${StdFSLibName} HINTS ENV LD_LIBRARY_PATH ENV LIBRARY_PATH ENV DYLD_LIBRARY_PATH)
    else()
        message(WARNING "Couldn't detect your C++ standard library, but also couldn't link without an additional library. You'll probably receive linker errors.")
    endif()

    find_package_handle_standard_args(StdFilesystem
        "Coudln't determine a proper setup for std::filesystem. Please use a fully C++17 compliant compiler and stdandard library."
        StdFilesystem_LIBRARY
        HAS_STD_FILESYSTEM
    )
else()
    set(OUTPUT_MESSAGE "Compiler integrated")
    find_package_handle_standard_args(StdFilesystem
        "Coudln't determine a proper setup for std::filesystem. Please use a fully C++17 compliant compiler and stdandard library."
        OUTPUT_MESSAGE
        HAS_STD_FILESYSTEM
    )
endif()

# if we have found it, let's define the std::filesystem target
if(StdFilesystem_FOUND)
    add_library(std::filesystem INTERFACE IMPORTED)
    target_compile_features(std::filesystem INTERFACE cxx_std_17)

    if(NOT _DONT_NEED_STD_FILESYSTEM_LIBRARY)
        target_link_libraries(std::filesystem INTERFACE ${StdFilesystem_LIBRARY})
    endif()

    if(HAS_EXPERIMENTAL_STD_FILESYSTEM)
        set_target_properties(std::filesystem PROPERTIES INTERFACE_COMPILE_DEFINITIONS HAS_EXPERIMENTAL_STD_FILESYSTEM)
    else()
        set_target_properties(std::filesystem PROPERTIES INTERFACE_COMPILE_DEFINITIONS HAS_STD_FILESYSTEM)
    endif()

    mark_as_advanced(StdFilesystem_LIBRARY)
endif()
