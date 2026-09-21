// Test-only LD_PRELOAD shim: inject ENOSPC/short writes or abrupt exit into fsck.
#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <dlfcn.h>
#include <unistd.h>

extern "C" int flock(int fd, int operation)
{
    using Lock = int (*)(int, int);
    static auto real_lock = reinterpret_cast<Lock>(dlsym(RTLD_NEXT, "flock"));
    if (std::getenv("FSCK_TEST_FLOCK_ERROR"))
    {
        errno = ENOLCK;
        return -1;
    }
    return real_lock(fd, operation);
}

extern "C" ssize_t pwrite(int fd, const void* buffer, size_t count, off_t offset)
{
    using Write = ssize_t (*)(int, const void*, size_t, off_t);
    static auto real_write = reinterpret_cast<Write>(dlsym(RTLD_NEXT, "pwrite"));
    auto suffix = std::getenv("FSCK_TEST_FAIL_SUFFIX");
    char link[64], path[4096];
    std::snprintf(link, sizeof(link), "/proc/self/fd/%d", fd);
    auto length = ::readlink(link, path, sizeof(path) - 1);
    if (suffix && length >= 0)
    {
        path[length] = 0;
        auto n = std::strlen(suffix);
        if (static_cast<size_t>(length) >= n && std::strcmp(path + length - n, suffix) == 0)
        {
            static bool written = false;
            if (!written && count > 1)
            {
                written = true;
                return real_write(fd, buffer, count / 2, offset);
            }
            if (std::getenv("FSCK_TEST_CRASH"))
                ::_exit(99);
            errno = ENOSPC;
            return -1;
        }
    }
    return real_write(fd, buffer, count, offset);
}
