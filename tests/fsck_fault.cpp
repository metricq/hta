// Test-only LD_PRELOAD shim: inject ENOSPC/short writes or abrupt exit into fsck.
#include <atomic>
#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <dlfcn.h>
#include <fcntl.h>
#include <string>
#include <unistd.h>

// Deterministic test rendezvous, outside production code. Workers announce the
// metric they reached, then wait until the test releases them. No timing guesses.
static void gate(int fd, bool installing)
{
    const char* directory = std::getenv("FSCK_TEST_GATE_DIR");
    if (!directory)
        return;
    const bool gate_install = std::getenv("FSCK_TEST_GATE_INSTALL") != nullptr;
    if (gate_install != installing)
        return;
    char link[64], buffer[4096];
    std::snprintf(link, sizeof(link), "/proc/self/fd/%d", fd);
    auto length = ::readlink(link, buffer, sizeof(buffer) - 1);
    if (length < 0)
        return;
    buffer[length] = 0;
    const std::string path(buffer);
    const std::string suffix = installing ? "/1000.hta" : "/raw.hta";
    if (path.size() < suffix.size() ||
        path.compare(path.size() - suffix.size(), suffix.size(), suffix))
        return;
    const auto parent = path.substr(0, path.size() - suffix.size());
    const auto metric = parent.substr(parent.rfind('/') + 1);
    thread_local std::string previous;
    if (previous == parent)
        return;
    previous = parent;
    const auto ready = std::string(directory) + "/ready-" + metric;
    int marker = ::open(ready.c_str(), O_WRONLY | O_CREAT | O_EXCL, 0600);
    if (marker >= 0)
        ::close(marker);
    const auto release = std::string(directory) + "/release";
    while (::access(release.c_str(), F_OK) != 0)
        ::usleep(10000);
}

extern "C" ssize_t pread(int fd, void* buffer, size_t count, off_t offset)
{
    using Read = ssize_t (*)(int, void*, size_t, off_t);
    static auto real_read = reinterpret_cast<Read>(dlsym(RTLD_NEXT, "pread"));
    gate(fd, false);
    return real_read(fd, buffer, count, offset);
}

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
    gate(fd, true);
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
            static std::atomic<bool> written{ false };
            if (count > 1 && !written.exchange(true))
            {
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
