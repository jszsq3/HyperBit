#include "Statisticsinfo.h"

void StatisticsInfo::flush(){
    switch(this->type){
        case ONLYS:
        case ONLYO:
            cout<<this->type<<":只有一个"<<endl;
            breakl
        case SANDP:
        case OANDP:
            cout<<this->type<<":只有2个"<<endl;
        default:break;
    }
}

StatisticInfo_Two::StatisticInfo_Two(StaType type, string dir){
    path = dir;
    this->type = type;
    string filename = dir+"/StatisticInfo_"+toStr(type);
    st_buf = new MMapBuffer(filename.c_str(), INIT_PAGE_COUNT * MemoryBuffer::pagesize);
    startptr = st_buf->get_address();
}
StatisticInfo_One::StatisticInfo_One(StaType type, string dir){
    path = dir;
    this->type = type;
    string filename = dir+"/StatisticInfo_"+toStr(type);
    st_buf = new MMapBuffer(filename.c_str(), INIT_PAGE_COUNT * MemoryBuffer::pagesize);
    startptr = st_buf->get_address();
}