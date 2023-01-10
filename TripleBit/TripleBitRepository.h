/*
 * TripleBitRespository.h
 *
 *  Created on: May 13, 2010
 *      Author: root
 */

#ifndef TRIPLEBITRESPOSITORY_H_
#define TRIPLEBITRESPOSITORY_H_

class TripleBitBuilder;

class PredicateTable;

class URITable;

class TimeTable;

class URIStatisticsBuffer;

class BitmapBuffer;

class EntityIDBuffer;

class MMapBuffer;

class TripleBitWorker;

class PartitionMaster;

class transQueueSW;

class TasksQueueWP;

class ResultBuffer;

class IndexForTT;

class QueryInfo;


#include "IRepository.h"
#include "TripleBit.h"
#include "StatisticsBuffer.h"
#include "TripleBitWorker.h"
#include "ThreadPool.h"
#include "LogmapBuffer.h"
#include "TimeMap.h"
#include "SPARQLParser.h"


class TripleBitRepository : public IRepository
{
public:
    map<string, map<string, vector<string>>> shixu;

public:
    PredicateTable *preTable;
    URITable *UriTable;
    // BitmapBuffer *bitmapBuffer;
    TimeMap *timemap;
    vector<BitmapBuffer *> snapshots;
    LogmapBuffer *logmapBuffer;
    STInfo *stInfoBySP;
    STInfo *stInfoByOP;
    //	StatisticsBuffer *spStatisBuffer, *opStatisBuffer;
    vector<MMapBuffer *> predicateImages;
    vector<MMapBuffer *> bitmapImage_s;
    vector<MMapBuffer *> bitmapImage_o;
    EntityIDBuffer *buffer;
    int pos;
    string dataBasePath;

    //	something about shared memory
    size_t workerNum, partitionNum;
    transQueueSW *transQueSW;
    vector<TasksQueueWP *> tasksQueueWP;
    vector<ResultBuffer *> resultWP;
    vector<boost::mutex *> tasksQueueWPMutex;
    boost::mutex *uriMutex;

    map<ID, TripleBitWorker *> tripleBitWorker;
    map<ID, PartitionMaster *> partitionMaster;
    MMapBuffer *logmapImage;
    MMapBuffer *bitmapPredicateImage;
    vector<string> resultSet;
    vector<string>::iterator resBegin;
    vector<string>::iterator resEnd;

    //	some thing about QueryTest
    IndexForTT *indexForTT;

public:
    //	vector<boost::mutex> tasksQueueMutex;

public:
    TripleBitRepository();

    virtual ~TripleBitRepository();

    // SO(id,string)transform
    bool find_soid_by_string(SOID &soid, const string &str);

    bool find_soid_by_string_update(SOID &soid, const string &str);

    bool find_string_by_soid(string &str, SOID &soid);

    // P(id,string)transform
    bool find_pid_by_string(PID &pid, const string &str);

    bool find_pid_by_string_update(PID &pid, const string &str);

    bool find_string_by_pid(string &str, ID &pid);

    // create a Repository specific in the path .
    static TripleBitRepository *create(const string &path);

    // create a Repository for update(without thread)
    static TripleBitRepository *create_for_update(const string &path);

    // Get some statistics information
    int get_predicate_count(PID pid);

    int get_subject_count(ID subjectID);

    int get_object_count(double object, char objType);

    int get_subject_predicate_count(ID subjectID, ID predicateID);

    int get_object_predicate_count(double object, ID predicateID, char objType);

    int get_subject_object_count(ID subjectID, double object, char objType = STRING);

    PredicateTable *getPredicateTable() const
    {
        return preTable;
    }

    URITable *getURITable() const
    {
        return UriTable;
    }

    // BitmapBuffer *getBitmapBuffer() const
    // {
    //     return bitmapBuffer;
    // }

    transQueueSW *getTransQueueSW()
    {
        return transQueSW;
    }

    STInfo *getSTInfoBySP() const
    {
        return stInfoBySP;
    }

    STInfo *getSTInfoByOP() const
    {
        return stInfoByOP;
    }

    //	StatisticsBuffer* getSpStatisBuffer() const {
    //		return spStatisBuffer;
    //	}
    //	StatisticsBuffer* getOpStatisBuffer() const {
    //		return opStatisBuffer;
    //	}
    vector<TasksQueueWP *> getTasksQueueWP()
    {
        return tasksQueueWP;
    }

    vector<ResultBuffer *> getResultWP()
    {
        return resultWP;
    }

    vector<boost::mutex *> getTasksQueueWPMutex()
    {
        return tasksQueueWPMutex;
    }

    boost::mutex *getUriMutex()
    {
        return uriMutex;
    }

    size_t getWorkerNum()
    {
        return workerNum;
    }

    size_t getPartitionNum()
    {
        return partitionNum;
    }

    string getDataBasePath()
    {
        return dataBasePath;
    }

    //	StatisticsBuffer* getStatisticsBuffer(StatisticsType type) {
    //		switch (type) {
    //		case SUBJECTPREDICATE_STATIS:
    //			return spStatisBuffer;
    //		case OBJECTPREDICATE_STATIS:
    //			return opStatisBuffer;
    //		}
    //
    //		return NULL;
    //	}
    // scan the database;
    Status getSubjectByObjectPredicate(double object, ID pod, char objType = STRING);

    ID next();

    // lookup string id;
    bool lookup(const string &str, ID &id);

    Status nextResult(string &str);

    Status execute(string query);

    size_t getResultSize() const
    {
        return resultSet.size();
    }

    void tripleBitWorkerInit(int i);

    Status sharedMemoryInit();

    Status sharedMemoryDestroy();

    Status sharedMemoryTransQueueSWInit();

    Status sharedMemoryTransQueueSWDestroy();

    Status sharedMemoryTasksQueueWPInit();

    Status sharedMemoryTasksQueueWPDestroy();

    Status sharedMemoryResultWPInit();

    Status sharedMemoryResultWPDestroy();

    Status tempMMapDestroy();

    void endForWorker();

    void workerComplete();

    void cmd_line(FILE *fin, FILE *fout);

    void cmd_line_sm(FILE *fin, FILE *fout, const string query_path);

    void cmd_line_sm(FILE *fin, FILE *fout, const string query_path, const string query);

    void cmd_line_cold(FILE *fin, FILE *fout, const string cmd);

    void cmd_line_warm(FILE *fin, FILE *fout, const string cmd);

    void insertData(const string &query_path);
    // void insertData(const string &query_path,string time);
    bool N3Parse(const char *filename, TempFile &rawFacts);

    void updateEdge(const string &query_path, const int updatepos);

    void updateNode(const string &update_path);
    void testquery(const string querfile);
    void query_with_single_time(QueryInfo* worker, string date);
    void query_with_double_time(QueryInfo* worker, string start_date, string end_date);

    Status getIDbyTriple(string *triple, ID *triple_id);

    static int colNo;

    void print();
    void parse(string &line, string &subject, string &predicate, string &object);
    void NTriplesParse(const char *subject, const char *predicate, const char *object, TempFile &facts);
    void print_all_data(int mode);
    void resolveTriples(TempFile &rawFacts, TimeStamp ts);
    static inline void loadTriple(const uchar*& reader, ID& v1, ID& v2, ID& v3) {
		TempFile::readIDIDID(reader, v1, v2, v3);
	}


};
class QueryInfo{
public:
    int proj_size;//变量个数
    SPARQLParser::PatternGroup patterns;
    vector<set<vector<ID>>> mid_rs;//存放中间结果集,每个查询模式的结果
    vector<vector<int>> proj_appear_nodes;//每一个变量在哪些模式中出现
    vector<vector<int>> projection_sim;//这个模式有哪些变量
    vector<vector<ID>> max_min;
public:
    QueryInfo(int pro_size, int pattern_size, SPARQLParser::PatternGroup patterns);
    void join(set<vector<ID>> &ans);
};


#endif /* TRIPLEBITRESPOSITORY_H_ */
