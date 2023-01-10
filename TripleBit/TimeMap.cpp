#include "TimeMap.h"

TimeMap::TimeMap(string dir) : dir(dir){
    string filename(dir);
	filename.append("/TimeMap");
	time_mmap = new MMapBuffer(filename.c_str(), INIT_PAGE_COUNT * MemoryBuffer::pagesize);
    writer = time_mmap->get_address();
    size = 0;
    *(int*)writer = size;
    writer += sizeof(int);
}
TimeMap::TimeMap(){

}
TimeMap::~TimeMap(){
    
}

TimeMap* TimeMap::load(const string dir){
    TimeMap* timemap = new TimeMap(); 
    timemap->dir = dir;
    string filename(dir);
    filename.append("/TimeMap");
    timemap->time_mmap = new MMapBuffer(filename.c_str());
    timemap->writer = timemap->time_mmap->get_address();
    timemap->size = *(int*)timemap->writer;
    timemap->writer += sizeof(int);
    for(int i=0;i<timemap->size;i++){

        char stime[11];
        TimeStamp ts;

        int l=0;
        while(*(char*)timemap->writer!='\0'){
            stime[l] = *(char*)timemap->writer;
            l++;
            timemap->writer += sizeof(char);
        }
        stime[10] = '\0';
        timemap->writer += sizeof(char);
        ts = *(TimeStamp*)timemap->writer;
        timemap->writer += sizeof(TimeStamp);
        timemap->t_to_ts[stime] = ts;
    }
    // for(auto &i:timemap->t_to_ts){
    //     cout<<i.first<<" "<<i.second<<endl;
    // }
    return timemap;
}

string TimeMap::get_now_t(){
    time_t t = time(nullptr);
    struct tm* now = localtime(&t);
    std::stringstream tstr;
    tstr<<now->tm_year+1900<<"-"<<now->tm_mon+1<<"-"<<now->tm_mday;
    return tstr.str();
}

void TimeMap::transferTimetoStamp(TimeStamp& stamp, const char* stime){
	struct tm timeinfo;
	strptime(stime,"%Y-%m-%d", &timeinfo);
	timeinfo.tm_hour = 0;
	timeinfo.tm_min = 0;
	timeinfo.tm_sec = 0;
	time_t timestamp = mktime(&timeinfo);
	stamp = (int)timestamp;
}
Status TimeMap::findTime(TimeStamp& stamp, const char* stime){
    map<string, TimeStamp>::iterator iter;
    iter = t_to_ts.find(stime);
    if(iter != t_to_ts.end()){
        stamp = iter->second;
        return OK;
    }
    return NOT_FIND;

}
Status TimeMap::insertTime(TimeStamp& stamp, const char* stime){
    TimeMap::transferTimetoStamp(stamp, stime);
    size++;
    *(int*)time_mmap->get_address() = size;
    t_to_ts[stime] = stamp;
    while(*stime){
        *(char*)writer = *stime;
        writer += sizeof(char);
        stime++;
    }
    *(char*)writer = '\0';
    writer += sizeof(char);

    *(TimeStamp*)writer = stamp;
    writer += sizeof(TimeStamp);
    return OK;
}