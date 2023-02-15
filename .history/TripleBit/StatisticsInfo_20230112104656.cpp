#include "Statisticsinfo.h"

// void StatisticsInfo::flush(){
    
// }

void StatisticInfo_One::flush(){
    size_t len = sizeof(ID) + sizeof(size_t);
    StatMeta *startmeta = (StatMeta*)startptr;
    uchar *start = startptr;
    StatDataMeta *datameta;
    if(startmeta->used_space==0){
        startmeta->used_page = 0;
        startmeta->used_space = sizeof(StatMeta);
        start = start + sizeof(StatMeta);
    }else{
        start = start + sizeof(StatMeta) + (startmeta->used_page-1) * MemoryBuffer::pagesize;
    }
    datameta = (StatDataMeta*)start;
    for(auto &id : temp_map){
        //如果超出了，申请空间
        if(startmeta->used_space+len>st_buf->getSize()){

        }
        if(datameta->used_space==0){//如果是第一块
            datameta->used_space = sizeof(StatDataMeta)
        }else if(datameta->used_space+len>MemoryBuffer::pagesize){//如果块没空间了

        }
        
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