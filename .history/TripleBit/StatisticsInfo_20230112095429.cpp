#include "Statisticsinfo.h"

// void StatisticsInfo::flush(){
    
// }

void StatisticInfo_One::flush(){
    StatMeta *startmeta = (StatMeta*)startptr;
    StatDataMeta *fir
    if(startmeta->used_space==0){

    }
    for(auto &id : temp_map){
        
    }
}

void StatisticInfo_Two::flush(){
    for(auto &id1 : temp_map){
        for(auto &id2:id1.second){
            
        }
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
    st_buf = new MMapBuffer(filename.c_str(), 1 * MemoryBuffer::pagesize);
    startptr = st_buf->get_address();
}