#ifndef STATISTICSINFO_H
#define STATISTICSINFO_H

#include "TripleBit.h"
#include "MemoryBuffer.h"
#include "MMapBuffer.h"
#include "BitmapBuffer.h"

// class StatBuffer{
// public:
    

// };

struct StatMeta{
    size_t used_space;
};

struct StatDataMeta{
    size_t used_space;
    ID min,max;
    enum StaType{ONLYS, ONLYP, ONLYO, SANDP, SANDO, OANDP};
    StaType type;
};

class StatisticsInfo{
public:
    uchar *startptr;
    MMapBuffer *st_buf;
    string path;
    enum StaType{ONLYS, ONLYP, ONLYO, SANDP, SANDO, OANDP};
    StaType type;
public:
    // void flush();
};

class StatisticInfo_One : public StatisticsInfo{
public:
    map<ID,size_t> temp_map;
public:
    StatisticInfo_One();
    StatisticInfo_One(StaType type, string path);
    void write(ID first, int count);
    void flush();
};

class StatisticInfo_Two : public StatisticsInfo{
public:
    map<ID,map<ID,size_t>> temp_map;//SP;OP
public:
    StatisticInfo_Two();
    StatisticInfo_Two(StaType type, string path);
    void write(ID first, ID second, int count);
    void flush();
};

#endif