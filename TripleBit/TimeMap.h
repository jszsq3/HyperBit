#ifndef TIMEMAP_H_
#define TIMEMAP_H_

#include "TripleBit.h"
#include "MMapBuffer.h"
using namespace std;
class TimeMap{
public:
    string dir;
    map<string, TimeStamp> t_to_ts;
    int size;
    MMapBuffer *time_mmap;
    uchar * writer;
public:
    TimeMap();
    TimeMap(string dir);
    ~TimeMap();
    static TimeMap *load(const string dir);
    static string get_now_t();
    static void transferTimetoStamp(TimeStamp& stamp, const char* stime);
    Status findTime(TimeStamp& stamp, const char* stime);
    Status insertTime(TimeStamp& stamp, const char* stime);
};
#endif