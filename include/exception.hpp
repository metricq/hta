#pragma once

#include <stdexcept>
#include <string>
#include <sstream>

namespace hta {

struct Exception : std::runtime_error
{
    explicit Exception(const std::string& arg) : std::runtime_error(arg) {}
};

namespace detail {

template <typename Arg, typename ...Args>
class make_exception
{
public:
    void operator()(std::stringstream& msg, Arg arg, Args ... args)
    {
        msg << arg;
        make_exception<Args...>()(msg, args...);
    }
};

template <typename Arg>
class make_exception<Arg>
{
public:
    void operator()(std::stringstream& msg, Arg arg)
    {
        msg << arg;
    }
};
}

template<typename ...Args>
inline void throw_exception(Args... args)
{
    std::stringstream msg;

    detail::make_exception<Args...>()(msg, args...);

    throw Exception(msg.str());
}
}
