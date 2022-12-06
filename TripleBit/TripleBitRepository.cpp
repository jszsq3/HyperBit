/*
 * TripleBitRespository.cpp
 *
 *  Created on: May 13, 2010
 *      Author: root
 */

#include "TripleBitRepository.h"

#include <sys/time.h>

#include <boost/thread/thread.hpp>

#include "BitmapBuffer.h"
#include "EntityIDBuffer.h"
#include "MMapBuffer.h"
#include "MemoryBuffer.h"
#include "OSFile.h"
#include "PredicateTable.h"
#include "StatisticsBuffer.h"
#include "TempMMapBuffer.h"
#include "ThreadPool.h"
#include "TripleBitBuilder.h"
#include "URITable.h"
#include "comm/IndexForTT.h"
#include "comm/ResultBuffer.h"
#include "comm/TasksQueueWP.h"
#include "comm/TransQueueSW.h"
#include "util/Properties.h"

// #define ZSQDEBUG
//#define MYDEBUG
int TripleBitRepository::colNo = INT_MAX - 1;

TripleBitRepository::TripleBitRepository()
{
  this->UriTable = NULL;
  this->preTable = NULL;
  // this->bitmapBuffer = NULL;
  buffer = NULL;
  timemap = NULL;
  this->transQueSW = NULL;

  //	spStatisBuffer = NULL;
  //	opStatisBuffer = NULL;

  // bitmapImage_s = NULL;
  // bitmapImage_o = NULL;
  // bitmapImage = NULL;
  bitmapPredicateImage = NULL;
}

TripleBitRepository::~TripleBitRepository()
{
  TempMMapBuffer::deleteInstance();
  if (tripleBitWorker.size() == workerNum)
  {
    for (size_t i = 1; i <= workerNum; ++i)
    {
      if (tripleBitWorker[i] != NULL)
        delete tripleBitWorker[i];
      tripleBitWorker[i] = NULL;
    }
  }
  if (buffer != NULL)
    delete buffer;
  buffer = NULL;

  if (UriTable != NULL)
    delete UriTable;
  UriTable = NULL;

  if (preTable != NULL)
    delete preTable;
  preTable = NULL;

  sharedMemoryDestroy();

  // if (bitmapBuffer != NULL)
  //   delete bitmapBuffer;
  // bitmapBuffer = NULL;

  if (stInfoBySP != NULL)
  {
    delete stInfoBySP;
    stInfoBySP = NULL;
  }

  if (stInfoByOP != NULL)
  {
    delete stInfoByOP;
    stInfoByOP = NULL;
  }
  //	if (spStatisBuffer != NULL)
  //		delete spStatisBuffer;
  //	spStatisBuffer = NULL;
  //
  //	if (opStatisBuffer != NULL)
  //		delete opStatisBuffer;
  //	opStatisBuffer = NULL;

  // if (bitmapImage_s != NULL)
  // {
  //   bitmapImage_s->close();
  //   delete bitmapImage_s;
  //   bitmapImage_s = NULL;
  // }
  // if (bitmapImage_o != NULL)
  // {
  //   bitmapImage_o->close();
  //   delete bitmapImage_o;
  //   bitmapImage_o = NULL;
  // }
  // if (bitmapImage != NULL)
  // {
  //   bitmapImage->close();
  //   delete bitmapImage;
  //   bitmapImage = NULL;
  // }

  if (bitmapPredicateImage != NULL)
  {
    bitmapPredicateImage->close();
    delete bitmapPredicateImage;
    bitmapPredicateImage = NULL;
  }

  if (indexForTT != NULL)
    delete indexForTT;
  indexForTT = NULL;

  if (uriMutex != NULL)
  {
    delete uriMutex;
    uriMutex = NULL;
  }
}

bool TripleBitRepository::find_pid_by_string(PID &pid, const string &str)
{
  if (preTable->getIDByPredicate(str.c_str(), pid) != OK)
    return false;
  return true;
}

bool TripleBitRepository::find_pid_by_string_update(PID &pid,
                                                    const string &str)
{
  if (preTable->getIDByPredicate(str.c_str(), pid) != OK)
  {
    if (preTable->insertTable(str.c_str(), pid) != OK)
      return false;
  }
  return true;
}

bool TripleBitRepository::find_soid_by_string(SOID &soid, const string &str)
{
  if (UriTable->getIdByURI(str.c_str(), soid) != URI_FOUND)
    return false;
  return true;
}

bool TripleBitRepository::find_soid_by_string_update(SOID &soid,
                                                     const string &str)
{
  if (UriTable->getIdByURI(str.c_str(), soid) != URI_FOUND)
  {
    if (UriTable->insertTable(str.c_str(), soid) != OK)
      return false;
  }
  return true;
}

bool TripleBitRepository::find_string_by_pid(string &str, PID &pid)
{
  if (preTable->getPredicateByID(str, pid) == OK)
    return true;
  return false;
}

bool TripleBitRepository::find_string_by_soid(string &str, SOID &soid)
{
  if (UriTable->getURIById(str, soid) == URI_NOT_FOUND)
    return false;

  return true;
}

int TripleBitRepository::get_predicate_count(PID pid)
{
  // int count1 = bitmapBuffer->getChunkManager(pid, ORDERBYS)->getTripleCount();
  // int count2 = bitmapBuffer->getChunkManager(pid, ORDERBYO)->getTripleCount();

  // return count1 + count2;
}

bool TripleBitRepository::lookup(const string &str, ID &id)
{
  if (preTable->getIDByPredicate(str.c_str(), id) != OK &&
      UriTable->getIdByURI(str.c_str(), id) != URI_FOUND)
    return false;

  return true;
}

int TripleBitRepository::get_object_count(double object, char objType)
{
  longlong count;
  //	spStatisBuffer->getStatisBySO(object, count, objType);
  stInfoByOP->get(object, objType, count);
  return count;
}

int TripleBitRepository::get_subject_count(ID subjectID)
{
  longlong count;
  //	opStatisBuffer->getStatisBySO(subjectID, count);
  stInfoBySP->get(subjectID, STRING, count);
  return count;
}

int TripleBitRepository::get_subject_predicate_count(ID subjectID,
                                                     ID predicateID)
{
  longlong count;
  stInfoBySP->get(subjectID, STRING, predicateID, count);
  //	spStatisBuffer->getStatis(subjectID, predicateID, count);
  return count;
}

int TripleBitRepository::get_object_predicate_count(double object,
                                                    ID predicateID,
                                                    char objType)
{
  longlong count;
  stInfoByOP->get(object, objType, predicateID, count);
  //	opStatisBuffer->getStatis(object, predicateID, count, objType);
  return count;
}

int TripleBitRepository::get_subject_object_count(ID subjectID, double object,
                                                  char objType)
{
  return 1;
}

Status TripleBitRepository::getSubjectByObjectPredicate(double object, ID pod,
                                                        char objType)
{
  pos = 0;
  return OK;
}

ID TripleBitRepository::next()
{
  ID id;
  Status s = buffer->getID(id, pos);
  if (s != OK)
    return 0;

  pos++;
  return id;
}

//�������ݿ�
TripleBitRepository *TripleBitRepository::create(const string &path)
{ // path = database/

  TripleBitRepository *repo = new TripleBitRepository();
  repo->dataBasePath = path;

  repo->UriTable = URITable::load(path);
  repo->preTable = PredicateTable::load(path);
  repo->timemap = TimeMap::load(path);
  string filename = path + "tmplogbuffer";

  repo->logmapImage = new MMapBuffer(filename.c_str(),0);
  // cout<<repo->logmapImage->getSize()<<endl;
  // LogChunkMeta *tmp = (LogChunkMeta*)repo->logmapImage->get_address();
  repo->logmapBuffer = LogmapBuffer::load(repo->logmapImage, path);
  string sp_filename = path;
  repo->predicateImages.resize(DEFAULT_SP_COUNT);
  repo->bitmapImage_s.resize(DEFAULT_SP_COUNT);
  repo->bitmapImage_o.resize(DEFAULT_SP_COUNT);
  repo->snapshots.resize(DEFAULT_SP_COUNT);
  ifstream a_usedPagesfile((path+"./properties").c_str());
  vector<vector<size_t>> usedPages(DEFAULT_SP_COUNT, vector<size_t>(2,0));
  if(!a_usedPagesfile){
    
  }else{
    for(int i = 0;i<DEFAULT_SP_COUNT;i++){
      a_usedPagesfile>>usedPages[i][0];
      a_usedPagesfile>>usedPages[i][1];
    }
  }
  for(int i = 0; i<DEFAULT_SP_COUNT;i++){
    string bitFilename = path+"BitmapBuffer";
    string predicateFile(bitFilename);
    string bitmapFile_s(bitFilename);
    string bitmapFile_o(bitFilename);
    string append_s(bitFilename);
    string append_o(bitFilename);
    predicateFile.append("_predicate"+toStr(i));
    bitmapFile_s.append("_obs"+toStr(i));
    bitmapFile_o.append("_obo"+toStr(i));
    append_s.append("_append_S"+toStr(i));
    append_o.append("_append_O"+toStr(i));
    repo->predicateImages[i] = new MMapBuffer(predicateFile.c_str(), 0);
    repo->bitmapImage_s[i] = new MMapBuffer(bitmapFile_s.c_str(), 0);
    repo->bitmapImage_o[i] = new MMapBuffer(bitmapFile_o.c_str(), 0);
    repo->snapshots[i] = BitmapBuffer::load_so(repo->bitmapImage_s[i], repo->bitmapImage_o[i], repo->predicateImages[i]);
    repo->snapshots[i]->appendByS = new MMapBuffer(append_s.c_str(),MemoryBuffer::pagesize);
    repo->snapshots[i]->appendByO = new MMapBuffer(append_o.c_str(),MemoryBuffer::pagesize);
    repo->snapshots[i]->usedPageByaS = usedPages[i][0];
    repo->snapshots[i]->usedPageByaO = usedPages[i][1];
  }

  repo->buffer = new EntityIDBuffer();
  // cout<<repo->logmapBuffer->usedPage<<endl;

//   repo->bitmapImage =
//       new MMapBuffer(filename.c_str(), 0); 
//   string predicateFile(
//       filename); 
//   predicateFile.append("_predicate");

//   string tempMMap(filename);
//   tempMMap.append(
//       "_temp"); 
//   TempMMapBuffer::create(tempMMap.c_str(), repo->bitmapImage->getSize());

//   std::map<std::string, std::string> properties;
//   Properties::parse(path, properties);
//   string usedPage = properties["usedpage"];
//   if (!usedPage.empty())
//   {
//     TempMMapBuffer::getInstance().setUsedPage(stoull(usedPage));
//   }
//   else
//   {
//     TempMMapBuffer::getInstance().setUsedPage(0);
//   }

//   repo->bitmapPredicateImage = new MMapBuffer(predicateFile.c_str(), 0);
//   repo->bitmapBuffer =
//       BitmapBuffer::load(repo->bitmapImage, repo->bitmapPredicateImage);

//   timeval start, end;
//   string statFilename = path + "/statistic_sp";
//   gettimeofday(&start, NULL);
//   repo->stInfoBySP = new STInfo(statFilename);
//   statFilename = path + "/statistic_op";
//   repo->stInfoByOP = new STInfo(statFilename);
//   gettimeofday(&end, NULL);


//   repo->buffer = new EntityIDBuffer();

//   repo->partitionNum = repo->bitmapPredicateImage->get_length() /
//                        ((sizeof(ID) + sizeof(SOType) + sizeof(size_t) * 2) *
//                         2); // numbers of predicate

//   repo->workerNum = WORKERNUM;
//   repo->indexForTT = new IndexForTT(WORKERNUM);

// #ifdef DEBUG
//   cout << "partitionNumber: " << repo->partitionNum << endl;
// #endif

//   repo->sharedMemoryInit();

//   gettimeofday(&start, NULL);
//   for (size_t i = 1; i <= repo->workerNum; ++i)
//   {
//     repo->tripleBitWorker[i] = new TripleBitWorker(repo, i);
//   }
//   gettimeofday(&end, NULL);

//   for (size_t i = 1; i <= repo->workerNum; ++i)
//   {
//     boost::thread thrd(boost::thread(
//         boost::bind<void>(&TripleBitRepository::tripleBitWorkerInit, repo, i)));
//   }


  return repo;
}

TripleBitRepository *TripleBitRepository::create_for_update(
    const string &path)
{ // path = database/

//   TripleBitRepository *repo = new TripleBitRepository();
//   repo->dataBasePath = path;

//   repo->UriTable = URITable::load(path);
//   repo->preTable = PredicateTable::load(path);
//   string filename = path + "BitmapBuffer";
//   string filename_s = path + "BitmapBuffer_obs"; // filename=database/BitmapBuffer_obs
//   string filename_o = path + "BitmapBuffer_obo"; // filename=database/BitmapBuffer_obo
  

//   //读取bitmapimage
//   repo->bitmapImage_s = new MMapBuffer(filename_s.c_str(), 0); // filename=database/BitmapBuffer_obs
//   repo->bitmapImage_o = new MMapBuffer(filename_o.c_str(), 0); // filename=database/BitmapBuffer_obo
  

//   string predicateFile(filename); // predicateFile = database/BitmapBuffer_predicate
//   predicateFile.append("_predicate");

//   string tempMMap(filename_s);
//   // tempMMap.append("_temp_s"); // tempMMap=database/BitmapBuffer_temp
//   TempMMapBuffer::create_so(0, tempMMap.c_str(), MemoryBuffer::pagesize);
//   tempMMap = filename_o;
//   // tempMMap.append("_temp_o"); // tempMMap=database/BitmapBuffer_temp
//   TempMMapBuffer::create_so(1, tempMMap.c_str(), MemoryBuffer::pagesize);

//   std::map<std::string, std::string> properties;
//   Properties::parse(path, properties);
//   string usedPage = properties["usedpage"];
//   if (!usedPage.empty())
//   {
//     TempMMapBuffer::getInstance_s().setUsedPage(stoull(usedPage) / 2);
//     TempMMapBuffer::getInstance_o().setUsedPage(stoull(usedPage) / 2);
//   }
//   else
//   {
//     TempMMapBuffer::getInstance_s().setUsedPage(1);
//     TempMMapBuffer::getInstance_o().setUsedPage(1);
//   }

//   repo->bitmapPredicateImage = new MMapBuffer(predicateFile.c_str(), 0);
//   //加载原始块
//   repo->bitmapBuffer = BitmapBuffer::load_so(repo->bitmapImage_s, repo->bitmapImage_o, repo->bitmapPredicateImage);

//   // repo->bitmapBuffer->print(2);

//   //	filename = path + "/statIndex";
//   //	MMapBuffer* indexBufferFile = MMapBuffer::create(filename.c_str(), 0);
//   //	uchar* indexBuffer = indexBufferFile->get_address();

//   //	string statFilename = path + "/subjectpredicate_statis";
//   timeval start, end;
//   string statFilename = path + "/statistic_sp";
//   gettimeofday(&start, NULL);
//   repo->stInfoBySP = new STInfo(statFilename);
//   statFilename = path + "/statistic_op";
//   repo->stInfoByOP = new STInfo(statFilename);
//   gettimeofday(&end, NULL);
//   // cout << "load statistic " << ((end.tv_sec - start.tv_sec) * 1000000 +
//   // end.tv_usec - start.tv_usec) / 1000000.0
//   // 		<< "s" << endl;

//   //	repo->spStatisBuffer = StatisticsBuffer::load(SUBJECTPREDICATE_STATIS,
//   // statFilename, indexBuffer); 	statFilename = path + "/objectpredicate_statis";
//   //	repo->opStatisBuffer = StatisticsBuffer::load(OBJECTPREDICATE_STATIS,
//   // statFilename, indexBuffer);

//   repo->buffer = new EntityIDBuffer();

//   repo->partitionNum = repo->bitmapPredicateImage->get_length() / ((sizeof(ID) + sizeof(SOType) + sizeof(size_t) * 2) * 2); // numbers of predicate

//   repo->workerNum = WORKERNUM;
//   repo->indexForTT = new IndexForTT(WORKERNUM);

// #ifdef DEBUG
//   cout << "partitionNumber: " << repo->partitionNum << endl;
// #endif

//   repo->sharedMemoryInit();

//   gettimeofday(&start, NULL);
//   for (size_t i = 1; i <= repo->workerNum; ++i)
//   {
//     repo->tripleBitWorker[i] = new TripleBitWorker(repo, i);
//   }
//   gettimeofday(&end, NULL);
//   // cout << "load tripleBitWorker " << ((end.tv_sec - start.tv_sec) * 1000000 +
//   // end.tv_usec - start.tv_usec) / 1000000.0
//   // 		<< "s" << endl;

//   // for (size_t i = 1; i <= repo->workerNum; ++i)
//   // {
//   //   boost::thread thrd(boost::thread(boost::bind<void>(&TripleBitRepository::tripleBitWorkerInit, repo, i)));
//   // }
//   //	delete indexBufferFile;
//   //	indexBufferFile = NULL;
//   // cout << "load complete!" << endl;

//   return repo;
}

void TripleBitRepository::tripleBitWorkerInit(int i)
{
  tripleBitWorker[i]->Work();
}

Status TripleBitRepository::sharedMemoryInit()
{
  // Init the transQueueSW shared Memory
  sharedMemoryTransQueueSWInit();

  // Init the tasksQueueWP shared memory
  sharedMemoryTasksQueueWPInit();

  // Init the resultWP shared memory
  sharedMemoryResultWPInit();

  uriMutex = new boost::mutex();

  return OK;
}

Status TripleBitRepository::sharedMemoryDestroy()
{
  // Destroy the transQueueSW shared Memory
  sharedMemoryTransQueueSWDestroy();
#ifdef MYDEBUG
  cout << "shared memory TransQueueSW destoried" << endl;
#endif

  // Destroy the tasksQueueWP shared memory
  sharedMemoryTasksQueueWPDestroy();
#ifdef MYDEBUG
  cout << "shared memory TasksQueueWP destoried" << endl;
#endif

  // Destroy the ResultWP shared memory
  sharedMemoryResultWPDestroy();
#ifdef MYDEBUG
  cout << "shared memory ResultWP destoried" << endl;
#endif

  return OK;
}

Status TripleBitRepository::sharedMemoryTransQueueSWInit()
{
  transQueSW = new transQueueSW();
#ifdef DEBUG
  cout << "transQueSW(Master) Address: " << transQueSW << endl;
#endif
  if (transQueSW == NULL)
  {
    cout << "TransQueueSW Init Failed!" << endl;
    return ERROR_UNKOWN;
  }
  return OK;
}

Status TripleBitRepository::sharedMemoryTransQueueSWDestroy()
{
  if (transQueSW != NULL)
  {
    delete transQueSW;
    transQueSW = NULL;
  }
  return OK;
}

Status TripleBitRepository::sharedMemoryTasksQueueWPInit()
{
  for (size_t partitionID = 1; partitionID <= this->partitionNum;
       ++partitionID)
  {
    TasksQueueWP *tasksQueue = new TasksQueueWP();
    if (tasksQueue == NULL)
    {
      cout << "TasksQueueWP Init Failed!" << endl;
      return ERROR_UNKOWN;
    }
    this->tasksQueueWP.push_back(tasksQueue);

    boost::mutex *wpMutex = new boost::mutex();
    this->tasksQueueWPMutex.push_back(wpMutex);
  }
  return OK;
}

Status TripleBitRepository::sharedMemoryTasksQueueWPDestroy()
{
  vector<TasksQueueWP *>::iterator iter = this->tasksQueueWP.begin(),
                                   limit = this->tasksQueueWP.end();
  for (; iter != limit; ++iter)
  {
    delete *iter;
    *iter = NULL;
  }
  this->tasksQueueWP.clear();
  this->tasksQueueWP.swap(this->tasksQueueWP);
  for (vector<boost::mutex *>::iterator iter = tasksQueueWPMutex.begin();
       iter != tasksQueueWPMutex.end(); iter++)
  {
    delete *iter;
    *iter = NULL;
  }
  return OK;
}

Status TripleBitRepository::sharedMemoryResultWPInit()
{
  for (size_t workerID = 1; workerID <= this->workerNum; ++workerID)
  {
    for (size_t partitionID = 1; partitionID <= this->partitionNum;
         ++partitionID)
    {
      ResultBuffer *resBuffer = new ResultBuffer();
      if (resBuffer == NULL)
      {
        cout << "ResultBufferWP Init Failed!" << endl;
        return ERROR_UNKOWN;
      }

      this->resultWP.push_back(resBuffer);
    }
  }
  return OK;
}

Status TripleBitRepository::sharedMemoryResultWPDestroy()
{
  vector<ResultBuffer *>::iterator iter = this->resultWP.begin(),
                                   limit = this->resultWP.end();
  for (; iter != limit; ++iter)
  {
    delete *iter;
  }
  this->resultWP.clear();
  this->resultWP.swap(this->resultWP);
  return OK;
}

Status TripleBitRepository::tempMMapDestroy()
{
  // endPartitionMaster();
  // bitmapBuffer->endUpdate(bitmapPredicateImage, bitmapImage);
  TempMMapBuffer::deleteInstance();
  return OK;
}

static void getQuery(string &queryStr, const char *filename)
{
  ifstream f;
  f.open(filename);

  queryStr.clear();

  if (f.fail() == true)
  {
    MessageEngine::showMessage("open query file error!",
                               MessageEngine::WARNING);
    return;
  }

  char line[250];
  while (!f.eof())
  {
    f.getline(line, 250);
    queryStr.append(line);
    queryStr.append(" ");
  }

  f.close();
}

Status TripleBitRepository::nextResult(string &str)
{
  if (resBegin != resEnd)
  {
    str = *resBegin;
    resBegin++;
    return OK;
  }
  return ERROR;
}

Status TripleBitRepository::execute(string queryStr)
{
  resultSet.resize(0);
  return OK;
}

void TripleBitRepository::endForWorker()
{
  string queryStr("exit");
  for (size_t i = 1; i <= workerNum; ++i)
  {
    transQueSW->EnQueue(queryStr);
  }
  indexForTT->wait();
}

void TripleBitRepository::workerComplete() { indexForTT->completeOneTriple(); }

extern char *QUERY_PATH;

void TripleBitRepository::cmd_line(FILE *fin, FILE *fout)
{
  char cmd[256];
  while (true)
  {
    fflush(fin);
    fprintf(fout, ">>>");
    fscanf(fin, "%s", cmd);
    resultSet.resize(0);
    if (strcmp(cmd, "dp") == 0 || strcmp(cmd, "dumppredicate") == 0)
    {
      getPredicateTable()->dump();
    }
    else if (strcmp(cmd, "query") == 0)
    {
    }
    else if (strcmp(cmd, "source") == 0)
    {
      string queryStr;
      ::getQuery(queryStr, string(QUERY_PATH).append("queryLUBM6").c_str());

      if (queryStr.length() == 0)
        continue;
      execute(queryStr);
    }
    else if (strcmp(cmd, "exit") == 0)
    {
      break;
    }
    else
    {
      string queryStr;
      ::getQuery(queryStr, string(QUERY_PATH).append(cmd).c_str());

      if (queryStr.length() == 0)
        continue;
      execute(queryStr);
    }
  }
}

void TripleBitRepository::cmd_line_sm(FILE *fin, FILE *fout,
                                      const string query_path)
{
  ThreadPool::createAllPool();
  char cmd[256];
  while (true)
  {
    fflush(fin);
    fprintf(fout, ">>>");
    fscanf(fin, "%s", cmd);
    resultSet.resize(0);
    if (strcmp(cmd, "dp") == 0 || strcmp(cmd, "dumppredicate") == 0)
    {
      getPredicateTable()->dump();
    }
    else if (strcmp(cmd, "query") == 0)
    {
    }
    else if (strcmp(cmd, "source") == 0)
    {
      string queryStr;
      ::getQuery(queryStr, string(query_path).append("queryLUBM6").c_str());
      if (queryStr.length() == 0)
        continue;
    }
    else if (strcmp(cmd, "exit") == 0)
    {
      endForWorker();
      break;
    }
    else
    {
      string queryStr;
      ::getQuery(queryStr, string(query_path).append(cmd).c_str());

      if (queryStr.length() == 0)
        continue;
      transQueSW->EnQueue(queryStr);
    }
  }

  std::map<std::string, std::string> properties;
  properties["usedpage"] =
      to_string(TempMMapBuffer::getInstance().getUsedPage());
  string db = DATABASE_PATH;
  Properties::persist(db, properties);
  tempMMapDestroy();
  ThreadPool::deleteAllPool();
}

void TripleBitRepository::query_with_single_time(QueryInfo* worker, string date){
#ifdef ZSQDEBUG
  cout<<date<<endl;
#endif
  TimeStamp target_ts;
  BitmapBuffer *basis_sp;
  target_ts = timemap->t_to_ts[date];
    /*选择合适的快照*/
  int optimal_sp = -1;
  size_t min = INT_MAX;
    for(int  i = 0 ;i<logmapBuffer->sequence_of_sp.size(); i++){
      TimeStamp tmpts = logmapBuffer->sequence_of_sp[i];
      if(abs((long)logmapBuffer->dp[target_ts]-(long)logmapBuffer->dp[tmpts])<min){
        min = abs((long)logmapBuffer->dp[target_ts]-(long)logmapBuffer->dp[tmpts]);
        optimal_sp = i;
      }
    }
#ifdef ZSQDEBUG
   cout<<"optimal_sp:"<<optimal_sp<<endl;
#endif
  basis_sp = snapshots[optimal_sp];
  int index_p = 0;
  for(auto &pattern:worker->patterns.patterns){
    
    int querytype = 0;//110,011,010
    if(pattern.subject.type==pattern.subject.Constant){
      if(UriTable->getIdByURI(pattern.subject.value.c_str(),pattern.subject.id)!=URI_FOUND){
        cout<<"subject不存在"<<endl;
        return;
      }
      querytype = querytype|4;
    }else{
      worker->projection_sim[index_p].push_back(pattern.subject.id);
      worker->proj_appear_nodes[pattern.subject.id].push_back(index_p);
    }
    if(pattern.predicate.type==pattern.predicate.Constant){
      preTable->getIDByPredicate(pattern.predicate.value.c_str(),pattern.predicate.id);
      querytype = querytype|2;
    }
    if(pattern.object.type==pattern.object.Constant){
      if(UriTable->getIdByURI(pattern.object.value.c_str(),pattern.object.id)!=URI_FOUND){
        cout<<"object不存在"<<endl;
        return;
      }
      querytype = querytype|1;
    }else{
      worker->projection_sim[index_p].push_back(pattern.object.id);
      worker->proj_appear_nodes[pattern.object.id].push_back(index_p);
    }
    if(querytype==6){//find o by sp
      int vid = pattern.object.id;
      vector<ID> tmp_max_min({UINT_MAX,1});
      basis_sp->predicate_managers[0][pattern.predicate.id]->findObjectBySubjectAndPredicate(pattern.subject.id, worker->mid_rs[index_p], worker->max_min[vid-1], tmp_max_min);
      //scan日志
      logmapBuffer->log_pre_managers[pattern.predicate.id]->findObjectBySubjectAndPredicate(pattern.subject.id, logmapBuffer->sequence_of_sp[optimal_sp], target_ts, worker->mid_rs[index_p], worker->max_min[vid-1], tmp_max_min);
      worker->max_min[vid-1][0] = tmp_max_min[0]>worker->max_min[vid-1][0]?tmp_max_min[0]:worker->max_min[vid-1][0];
      worker->max_min[vid-1][1] = tmp_max_min[1]<worker->max_min[vid-1][1]?tmp_max_min[1]:worker->max_min[vid-1][1];
    }else if(querytype==3){//find s by po
      
      int vid = pattern.subject.id;
      vector<ID> tmp_max_min({UINT_MAX,1});
      basis_sp->predicate_managers[1][pattern.predicate.id]->findSubjectByObjectAndPredicate(pattern.object.id, worker->mid_rs[index_p], worker->max_min[vid-1], tmp_max_min); 
      //scan日志
      logmapBuffer->log_pre_managers[pattern.predicate.id]->findSubjectByObjectAndPredicate(pattern.object.id, logmapBuffer->sequence_of_sp[optimal_sp], target_ts, worker->mid_rs[index_p], worker->max_min[vid-1], tmp_max_min);
      worker->max_min[vid-1][0] = tmp_max_min[0]>worker->max_min[vid-1][0]?tmp_max_min[0]:worker->max_min[vid-1][0];
      worker->max_min[vid-1][1] = tmp_max_min[1]<worker->max_min[vid-1][1]?tmp_max_min[1]:worker->max_min[vid-1][1];
    }else if(querytype==2){
      int vid1 = pattern.subject.id;
      int vid2 = pattern.object.id;
      vector<ID> tmp_max_min1({UINT_MAX,1});
      vector<ID> tmp_max_min2({UINT_MAX,1});
      basis_sp->predicate_managers[1][pattern.predicate.id]->findSubjectAndObjectByPredicate(worker->mid_rs[index_p], worker->max_min[vid1-1],worker->max_min[vid2-1], tmp_max_min1, tmp_max_min2); 
      //scan日志
      logmapBuffer->log_pre_managers[pattern.predicate.id]->findSubjectAndObjectByPredicate(logmapBuffer->sequence_of_sp[optimal_sp], target_ts, worker->mid_rs[index_p], worker->max_min[vid1-1],worker->max_min[vid2-1], tmp_max_min1, tmp_max_min2);
      worker->max_min[vid1-1][0] = tmp_max_min1[0]>worker->max_min[vid1-1][0]?tmp_max_min1[0]:worker->max_min[vid1-1][0];
      worker->max_min[vid1-1][1] = tmp_max_min1[1]<worker->max_min[vid1-1][1]?tmp_max_min1[1]:worker->max_min[vid1-1][1];
      worker->max_min[vid2-1][0] = tmp_max_min2[0]>worker->max_min[vid2-1][0]?tmp_max_min2[0]:worker->max_min[vid2-1][0];
      worker->max_min[vid2-1][1] = tmp_max_min2[1]<worker->max_min[vid2-1][1]?tmp_max_min2[1]:worker->max_min[vid2-1][1];
    }
    index_p++;
  }
}

void TripleBitRepository::query_with_double_time(QueryInfo* worker, string start_date, string end_date){
  TimeStamp target_ts_s;
  TimeStamp target_ts_e;
  BitmapBuffer *basis_sp;
  target_ts_s = timemap->t_to_ts[start_date];
  target_ts_e = timemap->t_to_ts[end_date];
    /*选择合适的快照*/
  int optimal_sp = -1;
  size_t min = INT_MAX;
    for(int  i = 0 ;i<logmapBuffer->sequence_of_sp.size(); i++){
      TimeStamp tmpts = logmapBuffer->sequence_of_sp[i];
      if(abs((long)logmapBuffer->dp[target_ts_s]-(long)logmapBuffer->dp[tmpts])<min){
        min = abs((long)logmapBuffer->dp[target_ts_s]-(long)logmapBuffer->dp[tmpts]);
        optimal_sp = i;
      }
    }
#ifdef ZSQDEBUG
   cout<<"optimal_sp:"<<optimal_sp<<endl;
#endif
  basis_sp = snapshots[optimal_sp];
  int index_p = 0;
  for(auto &pattern:worker->patterns.patterns){
    
    int querytype = 0;//110,011,010
    if(pattern.subject.type==pattern.subject.Constant){
      if(UriTable->getIdByURI(pattern.subject.value.c_str(),pattern.subject.id)!=URI_FOUND){
        cout<<"subject不存在"<<endl;
        return;
      }
      querytype = querytype|4;
    }else{
      worker->projection_sim[index_p].push_back(pattern.subject.id);
      worker->proj_appear_nodes[pattern.subject.id].push_back(index_p);
    }
    if(pattern.predicate.type==pattern.predicate.Constant){
      preTable->getIDByPredicate(pattern.predicate.value.c_str(),pattern.predicate.id);
      querytype = querytype|2;
    }
    if(pattern.object.type==pattern.object.Constant){
      if(UriTable->getIdByURI(pattern.object.value.c_str(),pattern.object.id)!=URI_FOUND){
        cout<<"object不存在"<<endl;
        return;
      }
      querytype = querytype|1;
    }else{
      worker->projection_sim[index_p].push_back(pattern.object.id);
      worker->proj_appear_nodes[pattern.object.id].push_back(index_p);
    }
    if(querytype==6){//find o by sp
      int vid = pattern.object.id;
      vector<ID> tmp_max_min({UINT_MAX,1});
      basis_sp->predicate_managers[0][pattern.predicate.id]->findObjectBySubjectAndPredicate(pattern.subject.id, worker->mid_rs[index_p], worker->max_min[vid-1], tmp_max_min);
      //scan日志
      logmapBuffer->log_pre_managers[pattern.predicate.id]->findObjectBySubjectAndPredicate(pattern.subject.id, logmapBuffer->sequence_of_sp[optimal_sp], target_ts_s, worker->mid_rs[index_p], worker->max_min[vid-1], tmp_max_min);
      logmapBuffer->log_pre_managers[pattern.predicate.id]->findObjectBySubjectAndPredicate(pattern.subject.id, target_ts_s, target_ts_e, worker->mid_rs[index_p], worker->max_min[vid-1], tmp_max_min);
      worker->max_min[vid-1][0] = tmp_max_min[0]>worker->max_min[vid-1][0]?tmp_max_min[0]:worker->max_min[vid-1][0];
      worker->max_min[vid-1][1] = tmp_max_min[1]<worker->max_min[vid-1][1]?tmp_max_min[1]:worker->max_min[vid-1][1];
    }else if(querytype==3){//find s by po
      
      int vid = pattern.subject.id;
      vector<ID> tmp_max_min({UINT_MAX,1});
      basis_sp->predicate_managers[1][pattern.predicate.id]->findSubjectByObjectAndPredicate(pattern.object.id, worker->mid_rs[index_p], worker->max_min[vid-1], tmp_max_min); 
      //scan日志
      logmapBuffer->log_pre_managers[pattern.predicate.id]->findSubjectByObjectAndPredicate(pattern.object.id, logmapBuffer->sequence_of_sp[optimal_sp], target_ts_s, worker->mid_rs[index_p], worker->max_min[vid-1], tmp_max_min);
      logmapBuffer->log_pre_managers[pattern.predicate.id]->findSubjectByObjectAndPredicate(pattern.object.id, target_ts_s, target_ts_e, worker->mid_rs[index_p], worker->max_min[vid-1], tmp_max_min);
      worker->max_min[vid-1][0] = tmp_max_min[0]>worker->max_min[vid-1][0]?tmp_max_min[0]:worker->max_min[vid-1][0];
      worker->max_min[vid-1][1] = tmp_max_min[1]<worker->max_min[vid-1][1]?tmp_max_min[1]:worker->max_min[vid-1][1];
    }else if(querytype==2){
      int vid1 = pattern.subject.id;
      int vid2 = pattern.object.id;
      vector<ID> tmp_max_min1({UINT_MAX,1});
      vector<ID> tmp_max_min2({UINT_MAX,1});
      basis_sp->predicate_managers[1][pattern.predicate.id]->findSubjectAndObjectByPredicate(worker->mid_rs[index_p], worker->max_min[vid1-1],worker->max_min[vid2-1], tmp_max_min1, tmp_max_min2); 
      //scan日志
      logmapBuffer->log_pre_managers[pattern.predicate.id]->findSubjectAndObjectByPredicate(logmapBuffer->sequence_of_sp[optimal_sp], target_ts_s, worker->mid_rs[index_p], worker->max_min[vid1-1],worker->max_min[vid2-1], tmp_max_min1, tmp_max_min2);
      logmapBuffer->log_pre_managers[pattern.predicate.id]->findSubjectAndObjectByPredicate(target_ts_s, target_ts_e, worker->mid_rs[index_p], worker->max_min[vid1-1],worker->max_min[vid2-1], tmp_max_min1, tmp_max_min2);
      worker->max_min[vid1-1][0] = tmp_max_min1[0]>worker->max_min[vid1-1][0]?tmp_max_min1[0]:worker->max_min[vid1-1][0];
      worker->max_min[vid1-1][1] = tmp_max_min1[1]<worker->max_min[vid1-1][1]?tmp_max_min1[1]:worker->max_min[vid1-1][1];
      worker->max_min[vid2-1][0] = tmp_max_min2[0]>worker->max_min[vid2-1][0]?tmp_max_min2[0]:worker->max_min[vid2-1][0];
      worker->max_min[vid2-1][1] = tmp_max_min2[1]<worker->max_min[vid2-1][1]?tmp_max_min2[1]:worker->max_min[vid2-1][1];
    }
    index_p++;
  }
}



void TripleBitRepository::testquery(const string queryfile){

  string queryStr;
  ::getQuery(queryStr, queryfile.c_str());
#ifdef ZSQDEBUG
  cout<<queryStr<<endl;
#endif
  SPARQLLexer *lexer = new SPARQLLexer(queryStr);
	SPARQLParser *parser = new SPARQLParser(*lexer);
  parser->parse();
  auto patterns = parser->getPatterns();
  QueryInfo *queryworker = new QueryInfo(parser->projectionEnd()-parser->projectionBegin(), patterns.patterns.size(), patterns);

  if(patterns.t_filter.size()==1){
    query_with_single_time(queryworker, patterns.t_filter[0]);

  }else{
    query_with_double_time(queryworker, patterns.t_filter[0],patterns.t_filter[1]);
  }

  //join
  
  set<vector<ID>> mmid_ans;
  queryworker->join(mmid_ans);
  
  // 打印结果
  for(auto &i:mmid_ans){
    for(auto &j:i){
      string uri;
      UriTable->getURIById(uri,j);
      cout<<uri<<"\t";
    }
    cout<<endl;
  }
  cout<<"共 "<<mmid_ans.size()<<" 条结果"<<endl;
  
}

void TripleBitRepository::cmd_line_sm(FILE *fin, FILE *fout,
                                      const string query_path,
                                      const string query)
{
  ThreadPool::createAllPool();
  //����ʱ������
  // ShiXu::parse(this->dataBasePath, this->shixu);
  char *cmd = strdup(query.c_str());
  resultSet.resize(0);
  if (strcmp(cmd, "dp") == 0 || strcmp(cmd, "dumppredicate") == 0)
  {
    getPredicateTable()->dump();
  }
  else if (strcmp(cmd, "query") == 0)
  {
  }
  else if (strcmp(cmd, "source") == 0)
  {
    string queryStr;
    ::getQuery(queryStr, string(query_path).append("queryLUBM6").c_str());
  }
  else if (strcmp(cmd, "exit") == 0)
  {
    endForWorker();
  }
  else
  {
    string queryStr;
    ::getQuery(queryStr, string(query_path).append(cmd).c_str());
    transQueSW->EnQueue(queryStr);
  }
  endForWorker();

  std::map<std::string, std::string> properties;
  properties["usedpage"] =
      to_string(TempMMapBuffer::getInstance().getUsedPage());
  string db = DATABASE_PATH;
  Properties::persist(db, properties);

  tempMMapDestroy();
  ThreadPool::deleteAllPool();
}

extern char *QUERY_PATH;

void TripleBitRepository::cmd_line_cold(FILE *fin, FILE *fout,
                                        const string cmd)
{
  string queryStr;
  getQuery(queryStr, string(QUERY_PATH).append(cmd).c_str());
  if (queryStr.length() == 0)
  {
    cout << "queryStr.length() == 0!" << endl;
    return;
  }
  cout << "DataBase: " << DATABASE_PATH << " Query:" << cmd << endl;
  transQueSW->EnQueue(queryStr);
  endForWorker();
  tempMMapDestroy();
}

extern char *QUERY_PATH;

void TripleBitRepository::cmd_line_warm(FILE *fin, FILE *fout,
                                        const string cmd)
{
  string queryStr;
  getQuery(queryStr, string(QUERY_PATH).append(cmd).c_str());
  if (queryStr.length() == 0)
  {
    cout << "queryStr.length() == 0" << endl;
    return;
  }
  cout << "DataBase: " << DATABASE_PATH << " Query:" << cmd << endl;
  for (int i = 0; i < 10; i++)
  {
    transQueSW->EnQueue(queryStr);
  }
  endForWorker();
  tempMMapDestroy();
}
void TripleBitRepository::parse(string &line, string &subject, string &predicate,
                                string &object)
{
  int l, r;
  // subject
  l = line.find_first_of('<') + 1;
  r = line.find_first_of('>');
  subject = line.substr(l, r - l);
  line = line.substr(r + 1);
  // predicate
  l = line.find_first_of('<') + 1;
  r = line.find_first_of('>');
  predicate = line.substr(l, r - l);
  line = line.substr(r + 1);

  // object
  if (line.find_first_of('\"') != string::npos)
  {
    l = line.find_first_of('\"') + 1;
    r = line.find_last_of('\"');
    object = line.substr(l, r - l);
    line = line.substr(r + 1);
  }
  else
  {
    l = line.find_first_of('<') + 1;
    r = line.find_first_of('>');
    object = line.substr(l, r - l);
    line = line.substr(r + 1);
  }
  

}

void TripleBitRepository::NTriplesParse(const char *subject, const char *predicate, const char *object, TempFile &facts)
{
  ID subjectID, predicateID, objectID;
  if (preTable->getIDByPredicate(predicate, predicateID) == PREDICATE_NOT_BE_FINDED)
  {
    preTable->insertTable(predicate, predicateID);
    // cout << predicateID << "  :  " << predicate << endl;
  }
  if (UriTable->getIdByURI(subject, subjectID) == URI_NOT_FOUND)
  {
    UriTable->insertTable(subject, subjectID);
    // cout << subjectID << "  :  " << subject << endl;
  }
  if (UriTable->getIdByURI(object, objectID) == URI_NOT_FOUND)
  {
    UriTable->insertTable(object, objectID);
    // cout << objectID << "  :  " << object << endl;
  }
  facts.writeIDIDID(subjectID,predicateID,objectID);
}

void TripleBitRepository::insertData(const string &query_path)
{
  TempFile rawFacts("./test");

  ifstream in((char *)query_path.c_str());
  if (!in.is_open())
  {
    cerr << "Unable to open " << query_path << endl;
    return ;
  }
  if (!N3Parse(query_path.c_str(), rawFacts))
  {
    in.close();
    return ;
  }

  in.close();
  delete UriTable;
  UriTable = NULL;
  delete preTable;
  preTable = NULL;

  rawFacts.flush();

  cout << "paser over" << endl;
  string nowtime = TimeMap::get_now_t();
  TimeStamp now_ts;
  timemap->insertTime(now_ts, nowtime.c_str());
  resolveTriples(rawFacts, now_ts);

  rawFacts.discard();
  timemap->time_mmap->flush();
  logmapBuffer->save();
}

void TripleBitRepository::resolveTriples(TempFile &rawFacts, TimeStamp ts){
  ID subjectID, lastSubjectID, predicateID, lastPredicateID, objectID,
      lastObjectID;
  MemoryMappedFile mappedIn;
  assert(mappedIn.open(rawFacts.getFile().c_str()));
  const uchar *reader = mappedIn.getBegin(), *limit = mappedIn.getEnd();
  loadTriple(reader, subjectID, predicateID, objectID);
  logmapBuffer->insertLog(subjectID, predicateID, objectID, ts);
  logmapBuffer->records_count[ts]++;
  while(reader<limit){
    loadTriple(reader, subjectID, predicateID, objectID);
    logmapBuffer->records_count[ts]++;
    logmapBuffer->insertLog(subjectID, predicateID, objectID, ts);
  }

  logmapBuffer->SnapShotSequence();
#ifdef ZSQDEBUG
  ofstream out("./testforinsert");
#endif

  for(int i = 0; i< snapshots.size();i++){
    TimeStamp current_ts = logmapBuffer->sequence_of_sp[i], target_ts = logmapBuffer->sequence_of_sp[i+logmapBuffer->number_of_sp];

    for(auto &j:logmapBuffer->log_pre_managers){
      LogChunkMeta *log_meta = j.second->get_Meta();
      while(true){
        if(log_meta->min_ts>target_ts){
          break;
        }else if(log_meta->max_ts<=current_ts){
          if(log_meta->next_page_no==-1){
            break;
          }
          log_meta = logmapBuffer->get_Page(log_meta->next_page_no);
          continue;
        }else{
          UpdateOp op;
          ID subjectID,objectID;
          TimeStamp ts;
          uchar *reader = (uchar*)log_meta, *limit = reader+log_meta->used_space;
          reader += sizeof(LogChunkMeta);
          while(reader<limit){
            LogChunkManager::readOPIDIDTS(reader, op, subjectID, objectID, ts);
            if(ts>current_ts&&ts<=target_ts){
              snapshots[i]->insert_new_data(subjectID,j.first, objectID);
              
            }
          }
          if(log_meta->next_page_no==-1){
            break;
          }
          log_meta = logmapBuffer->get_Page(log_meta->next_page_no);
        }
      }
    }
  }
#ifdef ZSQDEBUG
  out.close();
#endif
  logmapBuffer->sequence_of_sp.assign(logmapBuffer->sequence_of_sp.begin()+3,logmapBuffer->sequence_of_sp.end());

}

bool TripleBitRepository::N3Parse(const char *filename, TempFile &rawFacts){
  cerr << "Parsing " << filename << "..." << endl;
  MMapBuffer *slice = new MMapBuffer(filename);
  const uchar *reader = (const uchar *)slice->getBuffer(), *tempReader;
  const uchar *limit = (const uchar *)(slice->getBuffer() + slice->getSize());
  char buffer[512];
  string line, subject, predicate, object;
  //读取时间
  tempReader = reader;
  memset(buffer, 0, sizeof(buffer));
  while (reader < limit)
  {
    tempReader = reader;
    memset(buffer, 0, sizeof(buffer));
    while (tempReader < limit && *tempReader != '\n')
    {
      tempReader++;
    }
    if (tempReader != reader)
    {
      memcpy(buffer, reader, tempReader - reader);
      // cout<<buffer<<endl;
      line = buffer;
      parse(line, subject, predicate, object);
      if (subject.length() && predicate.length() && object.length())
      {
        
        NTriplesParse(subject.c_str(), predicate.c_str(), object.c_str(), 
                      rawFacts);
      }
    }
    tempReader++;
    reader = tempReader;
  }

  return true;
}

// void TripleBitRepository::insertData(const string &query_path,string timestr)
// {

// }

void TripleBitRepository::updateEdge(const string &update_path,
                                     const int updatepos)
{
  // TempFile rawFacts("./test");
  // //读取原始边，获取其ID
  // ifstream update_file(update_path);
  // if (!update_file.is_open())
  // {
  //   cout << "open falied" << endl;
  //   return;
  // }
  // string T_t[3];
  // string T_o[3];
  // update_file >> T_t[0] >> T_t[1] >> T_t[2];
  // string S(T_t[0].begin() + 1, T_t[0].end() - 1);
  // string P(T_t[1].begin() + 1, T_t[1].end() - 1);
  // string O(T_t[2].begin() + 1, T_t[2].end() - 1);
  // T_o[0] = S;
  // T_o[1] = P;
  // T_o[2] = O;

  // ID ID_o[3];
  // if (getIDbyTriple(T_o, ID_o) != URI_FOUND)
  // {
  //   cout << "ID_o not found" << endl;
  //   return;
  // }

  // //现将对应的节点插入StringPool中，读取修改边，获取其ID
  // string T_u[3];
  // update_file >> T_t[0] >> T_t[1] >> T_t[2];
  // string S_(T_t[0].begin() + 1, T_t[0].end() - 1);
  // string P_(T_t[1].begin() + 1, T_t[1].end() - 1);
  // string O_(T_t[2].begin() + 1, T_t[2].end() - 1);
  // T_u[0] = S_;
  // T_u[1] = P_;
  // T_u[2] = O_;
  // //若StringPool中没有要更新的数据，将其插入
  // if (updatepos != 1)
  // {
  //   //要插入的是S或者O
  //   ID temp_id;
  //   if (UriTable->getIdByURI(T_u[updatepos - 1].c_str(), temp_id) !=
  //       URI_FOUND)
  //   {
  //     UriTable->insertTable(T_u[updatepos - 1].c_str(), temp_id);
  //   }
  // }
  // else
  // {
  //   //要插入的是predicate
  //   ID temp_id;
  //   if (preTable->getIDByPredicate(T_u[updatepos - 1].c_str(), temp_id) != OK)
  //   {
  //     preTable->insertTable(T_u[updatepos - 1].c_str(), temp_id);
  //   }
  // }

  // ID ID_u[3];
  // if (getIDbyTriple(T_u, ID_u) != URI_FOUND)
  // {
  //   cout << "ID_o not found" << endl;
  //   return;
  // }

  // //查询bitmapp中原始边的位置，若不是写入test,若是更改，写入test
  // bitmapBuffer->updateEdge(ID_o, ID_u, updatepos, rawFacts);
  // //排序，建库，持久化
  // rawFacts.discard();
};

void TripleBitRepository::updateNode(const string &update_path)
{
  cout << "update node" << endl;
}

Status TripleBitRepository::getIDbyTriple(string *triple, ID *triple_id)
{
  if (UriTable->getIdByURI(triple[0].c_str(), triple_id[0]) != URI_FOUND)
  {
    return URI_NOT_FOUND;
  };
  if (preTable->getIDByPredicate(triple[1].c_str(), triple_id[1]) != OK)
  {
    return URI_NOT_FOUND;
  };
  if (UriTable->getIdByURI(triple[2].c_str(), triple_id[2]) != URI_FOUND)
  {
    return URI_NOT_FOUND;
  };
  return URI_FOUND;
}

void TripleBitRepository::print() { tripleBitWorker[0]->Print(); }

void TripleBitRepository::print_all_data(int mode)
{
  //只打印orderbyS的数据

  // ofstream out;
  // if (mode == 1)
  // {
  //   out.open("origin_data.txt", ios::out);
  // }
  // else
  // {
  //   out.open("new_data.txt", ios::out);
  // }

  // map<ID, ChunkManager *> chunkmanager_orders;
  // ChunkManagerMeta *chunkmanagermeta;
  // uchar *startPtr, *curPtr, *limit;
  // MetaData *meta, *firstmeta;
  // ID subjectID, objectID;
  // int tmpnextpage;

  // chunkmanager_orders =
  //     this->bitmapBuffer->predicate_managers[0]; //获取orderbys的chunkmanagers

  // map<ID, ChunkManager *>::iterator iter = chunkmanager_orders.begin();

  // for (; iter != chunkmanager_orders.end(); iter++)
  // {
  //   chunkmanagermeta =
  //       iter->second->getChunkManagerMeta(); //获取chunkmanager中的chunk_meta头
  //   uchar *chunkmanagermetaStart = (uchar *)chunkmanagermeta;
  //   int chunkCount = chunkmanagermeta->length / MemoryBuffer::pagesize;
  //   firstmeta = (MetaData *)chunkmanagermeta->startPtr; // chunkmanagermeta结束，meta开始的位置

  //   startPtr = chunkmanagermeta->startPtr; // chunkmanagermeta结束，meta开始的位置
  //   curPtr = startPtr + sizeof(MetaData);  // data开始的位置
  //   limit = chunkmanagermeta->endPtr;      // data结束的位置
  //   // out << "PID:" << iter->first << endl;
  //   for (int i = 1; i <= chunkCount; i++)
  //   {
  //     if (i == 1)
  //     {
  //       meta = firstmeta;
  //     }
  //     else
  //     {

  //       meta = (MetaData *)(chunkmanagermetaStart + (i - 1) * MemoryBuffer::pagesize);
  //     }
  //     tmpnextpage = meta->NextPageNo;
  //     while (tmpnextpage != -1)
  //     {
  //       startPtr = (uchar *)meta;
  //       curPtr = startPtr + sizeof(MetaData);
  //       limit = startPtr + meta->usedSpace;
  //       out << "meta->pageNo:" << meta->pageNo << endl;
  //       out << "meta->NextPageNo:" << meta->NextPageNo << endl;
  //       out << "meta->PID:" << meta->PID << endl;
  //       out << "meta->ChunkID:" << meta->ChunkID << endl;
  //       out << "meta->min:" << meta->min << endl;
  //       out << "meta->max:" << meta->max << endl;
  //       out << "meta->maxTime:" << meta->maxTime << endl;
  //       out << "meta->usedSpace:" << meta->usedSpace << endl;
  //       out << "meta->tripleCount:" << meta->tripleCount << endl;
  //       out << "meta->snapshotNo:" << meta->snapshotNo << endl;
  //       out << "meta->snapshotPageSize:" << meta->snapshotPageSize << endl;
  //       out << "meta->nextsnapshotChunknno:" << meta->nextsnapshotChunknno << endl;
  //       out << "meta->nextlogChunkID:" << meta->nextlogChunkID << endl;
  //       while (curPtr < limit)
  //       {
  //         if (*(ID *)curPtr == 0)
  //         {
  //           curPtr += sizeof(ID);
  //           out << *(ID *)curPtr << endl;
  //           curPtr += sizeof(ID);
  //         }
  //         subjectID = *(ID *)curPtr;
  //         curPtr += sizeof(ID);
  //         objectID = *(ID *)curPtr;
  //         curPtr += sizeof(ID);
  //         out << subjectID << " "
  //             << " " << iter->first << " " << objectID << " " << endl;
  //       }
  //       meta = (MetaData *)(TempMMapBuffer::getInstance().getAddress() +
  //                           (tmpnextpage)*MemoryBuffer::pagesize);
  //       tmpnextpage = meta->NextPageNo;
  //     }

  //     startPtr = (uchar *)meta;
  //     curPtr = startPtr + sizeof(MetaData);
  //     limit = startPtr + meta->usedSpace;
  //     out << "meta->pageNo:" << meta->pageNo << endl;
  //     out << "meta->NextPageNo:" << meta->NextPageNo << endl;
  //     out << "meta->PID:" << meta->PID << endl;
  //     out << "meta->ChunkID:" << meta->ChunkID << endl;
  //     out << "meta->min:" << meta->min << endl;
  //     out << "meta->max:" << meta->max << endl;
  //     out << "meta->maxTime:" << meta->maxTime << endl;
  //     out << "meta->usedSpace:" << meta->usedSpace << endl;
  //     out << "meta->tripleCount:" << meta->tripleCount << endl;
  //     out << "meta->snapshotNo:" << meta->snapshotNo << endl;
  //     out << "meta->snapshotPageSize:" << meta->snapshotPageSize << endl;
  //     out << "meta->nextsnapshotChunknno:" << meta->nextsnapshotChunknno << endl;
  //     out << "meta->nextlogChunkID:" << meta->nextlogChunkID << endl;

  //     while (curPtr < limit)
  //     {
  //       if (*(ID *)curPtr == 0)
  //       {
  //         curPtr += sizeof(ID);
  //         out << *(ID *)curPtr << endl;
  //         curPtr += sizeof(ID);
  //       }
  //       subjectID = *(ID *)curPtr;
  //       curPtr += sizeof(ID);
  //       objectID = *(ID *)curPtr;
  //       curPtr += sizeof(ID);
  //       out << subjectID << " "
  //           << " " << iter->first << " " << objectID << " " << endl;
  //     }
  //   }
  // }
  // out.close();
}

QueryInfo::QueryInfo(int pro_size, int pattersize, SPARQLParser::PatternGroup pattern){
  proj_size = pro_size;
  patterns = pattern;
  mid_rs.resize(pattersize);
  proj_appear_nodes.resize(pro_size+1);
  projection_sim.resize(pattersize);
  max_min = vector<vector<ID>>(pro_size,{1,UINT_MAX});
}

void QueryInfo::join(set<vector<ID>>& mmid_ans){
    int mmid_mode = 0 ;//
    vector<int> pattern_visited(mid_rs.size(),0);
    
    vector<int> proj_vis(proj_size,0);
    for(int i = 1 ; i < proj_appear_nodes.size(); i++){
      //i : 变量ID
      //j : triplenode ID
      for(auto &j:proj_appear_nodes[i]){
        if(pattern_visited[j]==1){
          continue;
        }
        cout<<"pattern: "<<j<<" size: "<<mid_rs[j].size()<<endl;
        set<vector<ID>> tmp;
        //如果单个变量
        if(projection_sim[j].size()==1){
          int vid = projection_sim[j][0];
          if(mmid_mode==0){
            pattern_visited[j]=1;//已访问j的中间结果
            //如果还没有中间结果
            mmid_mode = mmid_mode|(1<<(vid-1));
            proj_vis[vid-1] = 1;
            for(auto &res:mid_rs[j]){
              vector<ID> res_cell(proj_size,0);
              res_cell[vid-1] = res[0];
              tmp.insert(res_cell);
            }
            mmid_ans = tmp;
          }else{
            //如果已经有结果
            if(proj_vis[vid-1]!=0){
              pattern_visited[j]=1;//已访问j的中间结果
              for(auto &cell:mmid_ans){
                if(mid_rs[j].find({cell[vid-1]})!=mid_rs[j].end()){
                  tmp.insert(cell);
                }
              }
              mmid_ans = tmp; 
            }
          }
        }
        //如果2个变量
        else if(projection_sim[j].size()==2){
          int vid1 = projection_sim[j][0];
          int vid2 = projection_sim[j][1];
          if(mmid_mode==0){
            pattern_visited[j]=1;//已访问j的中间结果
            //如果还没有中间结果
            mmid_mode = mmid_mode|(1<<(vid1-1));
            mmid_mode = mmid_mode|(1<<(vid2-1));
            proj_vis[vid1-1] = 1;
            proj_vis[vid2-1] = 1;
            for(auto &res:mid_rs[j]){
              vector<ID> res_cell(proj_size,0);
              res_cell[vid1-1] = res[0];
              res_cell[vid2-1] = res[1];
              tmp.insert(res_cell);
            }
            mmid_ans = tmp;
          }else{
            //如果已经有中间结果集
            if(proj_vis[vid1-1]!=0||proj_vis[vid2-1]!=0){
              pattern_visited[j]=1;//已访问j的中间结果
              //投影有交
              
              if(proj_vis[vid1-1]!=0&&proj_vis[vid2-1]!=0){
                //如果有2个交集
                for(auto &cell:mmid_ans){
                  if(mid_rs[j].find({cell[vid1-1], cell[vid2-1]})!=mid_rs[j].end()){
                    tmp.insert(cell);
                  }
                }
                mmid_ans = tmp; 
              }else{
                //如果有1个交集
                if(proj_vis[vid1-1]!=0){//如果是vid1已经join过
                  proj_vis[vid2-1]=1;
                  map<ID, vector<ID>> tmp_map;
                  for(auto &mid_rs_iter:mid_rs[j]){
                    tmp_map[mid_rs_iter[0]].push_back(mid_rs_iter[1]);
                  }
                  for(auto &cell:mmid_ans){
                    if(tmp_map.find(cell[vid1-1])!=tmp_map.end()){
                      auto tmp_vec = tmp_map[cell[vid1-1]];
                      for(auto tmp_vec_iter:tmp_vec){
                        vector<ID> tmp_cell = cell;
                        tmp_cell[vid2-1] = tmp_vec_iter;
                        tmp.insert(tmp_cell);
                      }
                    }
                  }
                }else if(proj_vis[vid2-1]!=0){//如果是vid2已经join过
                  proj_vis[vid1-1]=1;
                  map<ID, vector<ID>> tmp_map;
                  for(auto &mid_rs_iter:mid_rs[j]){
                    tmp_map[mid_rs_iter[1]].push_back(mid_rs_iter[0]);
                  }
                  for(auto &cell:mmid_ans){
                    if(tmp_map.find(cell[vid2-1])!=tmp_map.end()){
                      auto tmp_vec = tmp_map[cell[vid2-1]];
                      for(auto tmp_vec_iter:tmp_vec){
                        vector<ID> tmp_cell = cell;
                        tmp_cell[vid1-1] = tmp_vec_iter;
                        tmp.insert(tmp_cell);
                      }
                    }
                  }
                }
                mmid_ans = tmp;
              }
              
            }
          }
        }
        cout<<"middle results number : "<<mmid_ans.size()<<endl;
      }
    }
}