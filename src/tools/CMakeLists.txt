include(FetchContent)

set(NITRO_BUILD_TESTING FALSE)
FetchContent_Declare(Nitro
    GIT_REPOSITORY https://github.com/tud-zih-energy/nitro.git
    GIT_TAG b90ddac02c19d6f16709afe1fd1ab1affd6367e8
    )
FetchContent_MakeAvailable(Nitro)

find_package(Boost COMPONENTS program_options system timer)

if(Boost_FOUND)
    add_executable(hta_check check.cpp)
    target_link_libraries(hta_check PRIVATE hta::hta Boost::program_options Boost::system)

    add_executable(hta_dummy dummy.cpp)
    target_link_libraries(hta_dummy PRIVATE hta::hta)

    add_executable(hta_dump dump.cpp)
    target_link_libraries(hta_dump PRIVATE hta::hta Boost::program_options Boost::system)

    add_executable(hta_throttle throttle.cpp)
    target_link_libraries(hta_throttle PRIVATE hta::hta)

    add_executable(hta_clone clone.cpp)
    target_link_libraries(hta_clone PRIVATE hta::hta)
else()
    message(STATUS "Will not build hta_dummy, hta_dump, hta_throttle missing boost")
endif()

add_executable(hta_repair repair.cpp)
target_link_libraries(hta_repair PRIVATE hta::hta Nitro::options)

