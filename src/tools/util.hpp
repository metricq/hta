#include <cstdio>

#define PBSTR "||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||"
#define PBWIDTH 60

void print_progress(double percentage)
{
    int val = (int)(percentage * 100);
    int lpad = (int)(percentage * PBWIDTH);
    int rpad = PBWIDTH - lpad;
    std::printf("\r%3d%% [%.*s%*s]", val, lpad, PBSTR, rpad, "");
    std::fflush(stdout);
}