//
// Created by 臧少琦 on 2022/3/13.
//

#ifndef HYPERBIT1_LOGMAPBUFFER_H
#define HYPERBIT1_LOGMAPBUFFER_H

#include "TripleBit.h"
#include "BitmapBuffer.h"
#include "MemoryBuffer.h"

class MemoryBuffer;
class MMapBuffer;
class LogChunkManager;
class BitmapBuffer;
struct LogChunkMeta;
class LogmapBuffer;

/*
LogChunkManager
*/
enum UpdateOp{
    INSERT,
    DELETE
};
class LogChunkManager{
public:
    LogmapBuffer *logmap;
    LogChunkMeta* meta;
    ID pid;
    int first_page_no;//第一个块的块号
    int final_page_no;//最后一个块的块号
    map<TimeStamp, int> time_to_page;
public:
    LogChunkManager();
    LogChunkManager(LogmapBuffer *logmap);
    ~LogChunkManager();
    void init_logChunkManager(LogmapBuffer *logmap, LogChunkMeta* meta, ID pid, int first_page_no, int final_page_no);
    void set_final_page_no(int no){final_page_no = no;}
    void setPID(ID in){pid = in;};
    // LogChunkManager* getLogManager(ID predicateID);
    LogmapBuffer* getLogmapBuffer(){return logmap;}
    void insertLog(ID subjectID, ID objectID, TimeStamp ts);
    void findObjectBySubjectAndPredicate(ID subjectID, TimeStamp basis_ts, TimeStamp target_ts, set<vector<ID>> &results, vector<ID> &max_min, vector<ID> &tmp_max_min);
    void findSubjectByObjectAndPredicate(ID objectID, TimeStamp basis_ts, TimeStamp target_ts, set<vector<ID>> &results, vector<ID> &max_min, vector<ID> &tmp_max_min);
    void findSubjectAndObjectByPredicate(TimeStamp basis_ts, TimeStamp target_ts, set<vector<ID>> &results, vector<ID> &max_min1, vector<ID> &max_min2, vector<ID> &tmp_max_min1, vector<ID> &tmp_max_min2);
    LogChunkMeta* get_Meta();
    static void readOPIDIDTS(uchar *&reader, UpdateOp &op, ID &subjectID, ID &objectID, TimeStamp &ts);
};


struct LogChunkMeta{
    int page_no;
    int next_page_no;
    ID pid;
    size_t used_space;
    TimeStamp min_ts;
    TimeStamp max_ts;
};

/*
LogmapBuffer
*/
class LogmapBuffer
{
public:
    const string dir;
    uchar *mmapaddres;
    map<ID, LogChunkManager *> log_pre_managers; //每个Logchunkmanager对应相应的pid的每个Chunk
    MMapBuffer *templogbuffer;
    size_t usedPage;
    map<TimeStamp, size_t> records_count;
    int number_of_sp;
    map<TimeStamp, size_t> dp;
    vector<TimeStamp> sequence_of_sp;
    mutex mutex_logmap;
    // vector<BitmapBuffer *> snapshots;
public:
    LogmapBuffer()
    {
        usedPage = 0;
        number_of_sp = DEFAULT_SP_COUNT;
    };

    LogmapBuffer(const string dir);
    LogmapBuffer(MMapBuffer *image, const string dir);
    ~LogmapBuffer();

    uchar* getAddress(){return mmapaddres;}

    static LogmapBuffer *load(MMapBuffer *image, const string dir);

    LogChunkManager* getLogManager(ID predicateID);
    void insertLogManager(ID predicateID);

    LogChunkMeta* get_Page(int page_no){
        return (LogChunkMeta*)(getAddress()+(page_no-1)*MemoryBuffer::pagesize);
    }
    void flush();
    void insertLog(ID subjectID, ID predicateID, ID objectID, TimeStamp ts);
    void SnapShotSequence();
    // TimeStamp findBreCentre(TimeStamp t1, TimeStamp t2);
    // void InitSnapshot();
    void save();
    void init_for_query();
};


#endif // HYPERBIT1_LOGMAPBUFFER_H
