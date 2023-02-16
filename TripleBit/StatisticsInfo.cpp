#include "Statisticsinfo.h"

// void StatisticsInfo::flush(){
    
// }

void StatisticInfo_One::flush(){
    size_t len = sizeof(ID) + sizeof(size_t);
    StatMeta *startmeta = (StatMeta*)startptr;
    uchar *start = startptr+sizeof(StatMeta);
    StatDataMeta *datameta;
    
    if(startmeta->used_space==0){//插入第一个数据
        startmeta->used_page = 1;
        startmeta->used_space = sizeof(StatMeta)+sizeof(StatDataMeta);
        startmeta->type = (StatMeta::StaType)type;
    }else{
        start = start + (startmeta->used_page-1) * MemoryBuffer::pagesize;
    }
    datameta = (StatDataMeta*)start;
    for(auto &id : temp_map){
        //如果超出，申请空间
        if(startmeta->used_space+len>st_buf->getSize()){
            st_buf->resize(MemoryBuffer::pagesize);
            startptr = st_buf->get_address();
            startmeta = (StatMeta*)startptr;
            start = startptr + sizeof(StatMeta) + (startmeta->used_page-1) * MemoryBuffer::pagesize;
            datameta = (StatDataMeta*)start;
        }
        if(datameta->used_space==0){
            //第一块
            datameta->used_space = sizeof(StatDataMeta);
            datameta->min = UINT_MAX;
        }
        //如果块超出了，下一块空间
        if(datameta->used_space+len>MemoryBuffer::pagesize){
            startmeta->used_page++;
            startmeta->used_space += sizeof(StatDataMeta);
            if(startmeta->used_space+len>st_buf->getSize()){
                st_buf->resize(MemoryBuffer::pagesize);
                startptr = st_buf->get_address();
                StatMeta *startmeta = (StatMeta*)startptr;
                
            }
            // start = start + sizeof(StatMeta) + (startmeta->used_page-1) * MemoryBuffer::pagesize;
            start = startptr + sizeof(StatMeta) + (startmeta->used_page-1) * MemoryBuffer::pagesize;
            datameta = (StatDataMeta*)start;
            datameta->used_space = sizeof(StatDataMeta);
            datameta->min = UINT_MAX;
        }
        //写
        uchar *writer = start+datameta->used_space;
        // cout<<writer-st_buf->get_address()<<endl;
        *(ID*)writer = id.first;
        writer += sizeof(ID);
        *(size_t*)writer = id.second;
        writer+=sizeof(size_t);
        datameta->used_space += len;
        startmeta->used_space+=len;
        datameta->max = max(datameta->max,id.first);
        datameta->min = min(datameta->min,id.first);
        
    }
    st_buf->flush();
}

void StatisticInfo_One::print(){
    
    ofstream fout("sta_print1");
    StatMeta *stameta = (StatMeta*)st_buf->get_address();
    uchar *start = st_buf->get_address()+sizeof(StatMeta), *end = start+stameta->used_space;
    StatDataMeta *datameta = (StatDataMeta*)start; 
    fout<<"stameta->type : "<<stameta->type<<endl;
    fout<<"stameta->used_page : "<<stameta->used_page<<endl;
    fout<<"stameta->used_space : "<<stameta->used_space<<endl;
    fout<<"=========================="<<endl;
    while(start<end){
        uchar *reader = (uchar*)(start+sizeof(StatDataMeta));
        uchar *limit = (uchar*)(start+datameta->used_space);
        fout<<"datameta->used_space : "<<datameta->used_space<<endl;
        fout<<"datameta->max : "<<datameta->max<<endl;
        fout<<"datameta->min : "<<datameta->min<<endl;
        while(reader<limit){
            fout<<*(ID*)reader<<" ";
            reader+=sizeof(ID);
            fout<<*(size_t*)reader<<endl;
            reader+=sizeof(size_t);
        }
        start = start+MemoryBuffer::pagesize;
        datameta = (StatDataMeta*)start;
    }
}

void StatisticInfo_Two::flush(){
    size_t len = 2*sizeof(ID) + sizeof(size_t);
    StatMeta *startmeta = (StatMeta*)startptr;
    uchar *start = startptr+sizeof(StatMeta);//第一个数据块开始的位置
    StatDataMeta *datameta;
    if(startmeta->used_space==0){//插入第一个数据
        startmeta->used_page = 1;
        startmeta->used_space = sizeof(StatMeta)+sizeof(StatDataMeta);
        startmeta->type = (StatMeta::StaType)type;
    }else{
        start = start + (startmeta->used_page-1) * MemoryBuffer::pagesize;//最后一个数据块开始的位置
    }
    datameta = (StatDataMeta*)start;
    for(auto &id1 : temp_map){
        for(auto &id2:id1.second){
            
                    //如果超出，申请空间
            if(startmeta->used_space+len>st_buf->getSize()){
                startptr = st_buf->resize(INIT_PAGE_COUNT*MemoryBuffer::pagesize);
                
                startmeta = (StatMeta*)startptr;
                start = startptr+sizeof(StatMeta) + (startmeta->used_page-1) * MemoryBuffer::pagesize;
                datameta = (StatDataMeta*)start;
                
            }
            
            if(datameta->used_space==0){
                //第一块
                datameta->used_space = sizeof(StatDataMeta);
                datameta->min = UINT_MAX;
            }
            //如果块超出了，下一块空间
            
            if(datameta->used_space+len>MemoryBuffer::pagesize){
                startmeta->used_page++;
                startmeta->used_space += sizeof(StatDataMeta);
                
                if(startmeta->used_space+len>st_buf->getSize()){
                    st_buf->resize(INIT_PAGE_COUNT*MemoryBuffer::pagesize);
                    startptr = st_buf->get_address();
                    startmeta = (StatMeta*)startptr;
                    
                }
                
                start = startptr+sizeof(StatMeta) + (startmeta->used_page-1) * MemoryBuffer::pagesize;
                datameta = (StatDataMeta*)start;
                datameta->used_space = sizeof(StatDataMeta);
                datameta->min = UINT_MAX;
            }
            //写
            
            // cout<<start+datameta->used_space-startptr<<endl;
            uchar *writer = start+datameta->used_space;
            *(ID*)writer = id1.first;
            writer += sizeof(ID);
            *(ID*)writer = id2.first;
            writer += sizeof(ID);
            *(size_t*)writer = id2.second;
            writer+=sizeof(size_t);
            datameta->used_space += len;
            startmeta->used_space+=len;
            datameta->max = max(datameta->max,id1.first);
            datameta->min = min(datameta->min,id1.first);
        }
    }
    st_buf->flush();
}

void StatisticInfo_Two::print(){
    ofstream fout("sta_print"+toStr(type));
    StatMeta *stameta = (StatMeta*)st_buf->get_address();
    uchar *start = st_buf->get_address()+sizeof(StatMeta), *end = start+stameta->used_space;
    StatDataMeta *datameta = (StatDataMeta*)start; 

    fout<<"stameta->type : "<<stameta->type<<endl;
    fout<<"stameta->used_page : "<<stameta->used_page<<endl;
    fout<<"stameta->used_space : "<<stameta->used_space<<endl;
    fout<<"=========================="<<endl;
    while(start<end){
        uchar *reader = (uchar*)(start+sizeof(StatDataMeta));
        uchar *limit = (uchar*)(start+datameta->used_space);
        fout<<"datameta->used_space : "<<datameta->used_space<<endl;
        fout<<"datameta->max : "<<datameta->max<<endl;
        fout<<"datameta->min : "<<datameta->min<<endl;
        while(reader<limit){
            fout<<*(ID*)reader<<" ";
            reader+=sizeof(ID);
            fout<<*(ID*)reader<<" ";
            reader+=sizeof(ID);
            fout<<*(size_t*)reader<<endl;
            reader+=sizeof(size_t);
        }
        start = start+MemoryBuffer::pagesize;
        datameta = (StatDataMeta*)start;
    }
}

void StatisticsInfo::load(StaType type, string dir){
    path  = dir;
    this->type = type;
    string filename = dir+"/StatisticInfo_"+toStr(type);
    st_buf = new MMapBuffer(filename.c_str());
    startptr = st_buf->get_address();
}

StatisticInfo_Two::StatisticInfo_Two(StaType type, string dir){
    path = dir;
    this->type = type;
    string filename = dir+"/StatisticInfo_"+toStr(type);
    st_buf = new MMapBuffer(filename.c_str(), INIT_PAGE_COUNT * MemoryBuffer::pagesize+sizeof(StatMeta));
    startptr = st_buf->get_address();
}
StatisticInfo_One::StatisticInfo_One(StaType type, string dir){
    path = dir;
    this->type = type;
    string filename = dir+"/StatisticInfo_"+toStr(type);
    st_buf = new MMapBuffer(filename.c_str(), 1 * MemoryBuffer::pagesize+sizeof(StatMeta));
    startptr = st_buf->get_address();
}