/*
 * TripleBitBuilder.cpp
 *
 *  Created on: Apr 6, 2010
 *      Author: root
 */

#include "TripleBitBuilder.h"

#include <pthread.h>
#include <string.h>

#include <ctime>
#include <fstream>

#include "BitmapBuffer.h"
#include "LogmapBuffer.h"
#include "MMapBuffer.h"
#include "MemoryBuffer.h"
#include "PredicateTable.h"
#include "Sorter.h"
#include "StatisticsBuffer.h"
#include "TripleBit.h"
#include "URITable.h"
#include "Statisticsinfo.h"

//#define MYDEBUG

static int getCharPos(const char *data, char ch)
{
  const char *p = data;
  int i = 0;
  while (*p != '\0')
  {
    if (*p == ch)
      return i + 1;
    p++;
    i++;
  }

  return -1;
}

TripleBitBuilder::TripleBitBuilder(string _dir) : dir(_dir)
{
  preTable = new PredicateTable(dir);
  uriTable = new URITable(dir);
  logmap = new LogmapBuffer(dir);
  timemap = new TimeMap(dir);
  staReifTable = new StatementReificationTable();
  InitSnapshot(dir);

}

TripleBitBuilder::TripleBitBuilder()
{
  preTable = NULL;
  uriTable = NULL;
  timemap = NULL;
  logmap = NULL;
  staReifTable = NULL;
}

void TripleBitBuilder::InitSnapshot(const string dir){
    for(int i = 0; i < DEFAULT_SP_COUNT; i++){
        BitmapBuffer *tmp = new BitmapBuffer(dir, i);
        snapshots.push_back(tmp);
    }
}

TripleBitBuilder::~TripleBitBuilder()
{
  // TODO Auto-generated destructor stub
#ifdef TRIPLEBITBUILDER_DEBUG
  cout << "Bit map builder destroyed begin " << endl;
#endif
  // mysql = NULL;
  if (preTable != NULL)
    delete preTable;
  preTable = NULL;

  if (uriTable != NULL)
    delete uriTable;
  uriTable = NULL;
  // delete uriStaBuffer;
  if (staReifTable != NULL)
    delete staReifTable;
  staReifTable = NULL;

  if(timemap != NULL){
    delete timemap;
    timemap = NULL;
  }

  if (logmap != NULL)
  {
    delete logmap;
    logmap = NULL;
  }
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

  //	if (spStatisBuffer != NULL) {
  //		delete spStatisBuffer;
  //	}
  //	spStatisBuffer = NULL;
  //
  //	if (opStatisBuffer != NULL) {
  //		delete opStatisBuffer;
  //	}
  //	opStatisBuffer = NULL;
}

bool TripleBitBuilder::isStatementReification(const char *object)
{
  int pos;

  const char *p;

  if ((pos = getCharPos(object, '#')) != -1)
  {
    p = object + pos;

    if (strcmp(p, "Statement") == 0 || strcmp(p, "subject") == 0 ||
        strcmp(p, "predicate") == 0 || strcmp(p, "object") == 0)
    {
      return true;
    }
  }

  return false;
}

bool lexDate(string &str, double &date)
{
  if (str.empty())
  {
    return false;
  }
  TurtleParser::strim(str);
  if (str.empty() || str.length() != 19)
  {
    return false;
  }
  if (str[0] >= '0' && str[0] <= '9' && str[1] >= '0' && str[1] <= '9' &&
      str[2] >= '0' && str[2] <= '9' && str[3] >= '0' && str[3] <= '9' &&
      str[4] == '-' && str[5] >= '0' && str[5] <= '1' && str[6] >= '0' &&
      str[6] <= '9' && str[7] == '-' && str[8] >= '0' && str[8] <= '3' &&
      str[9] >= '0' && str[9] <= '9' && str[10] == ' ' && str[11] >= '0' &&
      str[11] <= '2' && str[12] >= '0' && str[12] <= '9' && str[13] == ':' &&
      str[14] >= '0' && str[14] <= '5' && str[15] >= '0' && str[15] <= '9' &&
      str[16] == ':' && str[17] >= '0' && str[17] <= '5' && str[18] >= '0' &&
      str[18] <= '9')
  {
    date = (str[0] - '0');
    date = date * 10 + (str[1] - '0');
    date = date * 10 + (str[2] - '0');
    date = date * 10 + (str[3] - '0');
    date = date * 10 + (str[5] - '0');
    date = date * 10 + (str[6] - '0');
    date = date * 10 + (str[8] - '0');
    date = date * 10 + (str[9] - '0');
    date = date * 10 + (str[11] - '0');
    date = date * 10 + (str[12] - '0');
    date = date * 10 + (str[14] - '0');
    date = date * 10 + (str[15] - '0');
    date = date * 10 + (str[17] - '0');
    date = date * 10 + (str[18] - '0');
    return true;
  }
  return false;
}

void TripleBitBuilder::parse(string &line, string &subject, string &predicate,
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

void TripleBitBuilder::parse(string &line, string &subject, string &predicate,
                             string &object, string& time)
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
  // time
  l = line.find_first_of('<') + 1;
  r = line.find_first_of('>');
  time = line.substr(l, r - l);
  line = line.substr(r + 1);
}

void TripleBitBuilder::NTriplesParse(const char *subject, const char *predicate,
                                     const char *object, TempFile &facts)
{
  ID subjectID, predicateID, objectID;
  if (preTable->getIDByPredicate(predicate, predicateID) ==
      PREDICATE_NOT_BE_FINDED)
  {
    preTable->insertTable(predicate, predicateID);
    // cout << predicateID << "  :  " << predicate << endl;
  }
  if (uriTable->getIdByURI(subject, subjectID) == URI_NOT_FOUND)
  {
    uriTable->insertTable(subject, subjectID);
    // cout << subjectID << "  :  " << subject << endl;
  }
  if (uriTable->getIdByURI(object, objectID) == URI_NOT_FOUND)
  {
    uriTable->insertTable(object, objectID);
    // cout << objectID << "  :  " << object << endl;
  }

  facts.writeIDIDID(subjectID, predicateID, objectID);
}

void TripleBitBuilder::NTriplesParse(const char *subject, const char *predicate,
                                     const char *object, const char *str_time, TempFile &facts)
{
  ID subjectID, predicateID, objectID;
  TimeStamp timestamp;
  if (preTable->getIDByPredicate(predicate, predicateID) ==
      PREDICATE_NOT_BE_FINDED)
  {
    preTable->insertTable(predicate, predicateID);
    // cout << predicateID << "  :  " << predicate << endl;
  }
  if (uriTable->getIdByURI(subject, subjectID) == URI_NOT_FOUND)
  {
    uriTable->insertTable(subject, subjectID);
    // cout << subjectID << "  :  " << subject << endl;
  }
  if (uriTable->getIdByURI(object, objectID) == URI_NOT_FOUND)
  {
    uriTable->insertTable(object, objectID);
    // cout << objectID << "  :  " << object << endl;
  }
  if(timemap->findTime(timestamp, str_time)==NOT_FIND){
    timemap->insertTime(timestamp, str_time);
  }
  // cout << str_time << "  :  " << timestamp << endl;
  facts.writeIDIDIDTS(subjectID, predicateID, objectID, timestamp);
}

bool TripleBitBuilder::N3Parse(const char *filename, TempFile &rawFacts)
{
  cerr << "Parsing " << filename << "..." << endl;
  MMapBuffer *slice = new MMapBuffer(filename);
  const uchar *reader = (const uchar *)slice->getBuffer(), *tempReader;
  const uchar *limit = (const uchar *)(slice->getBuffer() + slice->getSize());
  char buffer[512];
  string line, subject, predicate, object, time;
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
      parse(line, subject, predicate, object, time);
      if (subject.length() && predicate.length() && object.length() && time.length())
      {
        NTriplesParse(subject.c_str(), predicate.c_str(), object.c_str(), time.c_str(), 
                      rawFacts);
      }
    }
    tempReader++;
    reader = tempReader;
  }

  return true;
}

const uchar *TripleBitBuilder::skipIdIdId(const uchar *reader)
{
  return TempFile::skipID(TempFile::skipID(TempFile::skipID(reader)));
}
const uchar *TripleBitBuilder::skipIdIdIdTS(const uchar *reader)
{
  return TempFile::skipID(TempFile::skipID(TempFile::skipID(TempFile::skipID(reader))));
}
int TripleBitBuilder::compare213(const uchar *left, const uchar *right)
{
  ID l1, l2, l3, r1, r2, r3;
  loadTriple(left, l1, l2, l3);
  loadTriple(right, r1, r2, r3);
  return cmpTriples(l2, l1, l3, r2, r1, r3);
}
int TripleBitBuilder::compare231(const uchar *left, const uchar *right)
{
  ID l1, l2, l3, r1, r2, r3;
  loadTriple(left, l1, l2, l3);
  loadTriple(right, r1, r2, r3);
  return cmpTriples(l2, l3, l1, r2, r3, r1);
}

int TripleBitBuilder::compare123(const uchar *left, const uchar *right)
{
  ID l1, l2, l3, r1, r2, r3;
  loadTriple(left, l1, l2, l3);
  loadTriple(right, r1, r2, r3);
  return cmpTriples(l1, l2, l3, r1, r2, r3);
}

int TripleBitBuilder::compare321(const uchar *left, const uchar *right)
{
  ID l1, l2, l3, r1, r2, r3;
  loadTriple(left, l1, l2, l3);
  loadTriple(right, r1, r2, r3);
  return cmpTriples(l3, l2, l1, r3, r2, r1);
}
int TripleBitBuilder::compareT(const uchar *left, const uchar *right)
{
  ID l1, l2, l3, r1, r2, r3;
  TimeStamp tl, tr;
  loadQuads(left, l1, l2, l3, tl);
  loadQuads(right, r1, r2, r3, tr);
  return cmpTimeStamp(l1, l2, l3, tl, r1, r2, r3, tr);
}

void print(TempFile &infile, char *outfile)
{
  MemoryMappedFile mappedIn;
  assert(mappedIn.open(infile.getFile().c_str()));
  const uchar *reader = mappedIn.getBegin(), *limit = mappedIn.getEnd();

  // Produce tempfile
  ofstream out(outfile);
  while (reader < limit)
  {
    out << *(ID *)reader << "\t" << *(ID *)(reader + 4) << "\t"
        << *(double *)(reader + 8) << *(char *)(reader + 16) << endl;
    reader += 17;
  }
  mappedIn.close();
  out.close();
}

Status TripleBitBuilder::resolveTriples(TempFile &rawFacts)
{ // rawFacts = <SID,PID,OID,TID>...
  ID subjectID, lastSubjectID, predicateID, lastPredicateID, objectID,
      lastObjectID;
  TimeStamp ts, lastts;
  // string predicates;
  // bool diff = false;
  // size_t count1 = 0;
  /*=========初次建立时按照时间排序 start=========*/
  TempFile sortByTimeStamp("./SortByT");
  cout<<"===Start sort by Time==="<<endl;
  Sorter::sort(rawFacts, sortByTimeStamp, skipIdIdIdTS, compareT);
  cout<<"===End sort by Time==="<<endl;
  sortByTimeStamp.close();
  MemoryMappedFile mappedIn;
  assert(mappedIn.open(sortByTimeStamp.getFile().c_str()));
  const uchar *reader = mappedIn.getBegin(), *limit = mappedIn.getEnd();
  StatisticInfo_One *sta_one_s = new StatisticInfo_One(StatisticsInfo::ONLYS, logmap->dir);//一个统计信息
  StatisticInfo_One *sta_one_o = new StatisticInfo_One(StatisticsInfo::ONLYO, logmap->dir);
  StatisticInfo_Two *sta_two_sp = new StatisticInfo_Two(StatisticsInfo::SANDP, logmap->dir);//一个统计信息
  StatisticInfo_Two *sta_two_op = new StatisticInfo_Two(StatisticsInfo::OANDP, logmap->dir);
  loadQuads(reader, subjectID, predicateID, objectID, ts);
  logmap->insertLog(subjectID, predicateID, objectID, ts);
  sta_one_s->temp_map[subjectID]++;
  sta_one_o->temp_map[objectID]++;
  sta_two_sp->temp_map[subjectID][predicateID]++;
  sta_two_opo->temp_map[objectID][predicateID]++;
  logmap->records_count[ts]++;
  while(reader<limit){
    loadQuads(reader, subjectID, predicateID, objectID, ts);
    logmap->records_count[ts]++;
    logmap->insertLog(subjectID, predicateID, objectID, ts);
  }

  //计算快照位置
  // cout<<"计算快照位置"<<endl;
  logmap->SnapShotSequence();
  /*生成快照*/
  // BitmapBuffer *bitmap = new BitmapBuffer(logmap->dir);
  
  
  
  for(int i = 0; i<logmap->number_of_sp; i++){
    reader = mappedIn.getBegin(); limit = mappedIn.getEnd();
    TempFile rawfacts_sp("./test1");
    cout<<"建立快照 "<<i<<endl;
    loadQuads(reader, subjectID, predicateID, objectID, ts);
    rawfacts_sp.writeIDIDID(subjectID, predicateID, objectID);
    while(ts<=logmap->sequence_of_sp[i]){
      loadQuads(reader, subjectID, predicateID, objectID, ts);
      rawfacts_sp.writeIDIDID(subjectID, predicateID, objectID);
    }
    cout<<i<<" Sort By Subject start"<<endl;
    TempFile sortedByS("./SortByS");
    Sorter::sort(rawfacts_sp, sortedByS, skipIdIdId, compare123);
    MemoryMappedFile mappedIn_sp;
    assert(mappedIn_sp.open(sortedByS.getFile().c_str()));
    const uchar *reader_sp = mappedIn_sp.getBegin(), *limit_sp = mappedIn_sp.getEnd();
    cout<<i<<" build snapshot start"<<endl;
    loadTriple(reader_sp, subjectID, predicateID, objectID);
    lastSubjectID = subjectID; lastPredicateID = predicateID, lastObjectID = objectID;
    snapshots[i]->insertTriple(predicateID, subjectID, objectID, ORDERBYS);

    while(reader_sp<limit_sp){
      loadTriple(reader_sp, subjectID, predicateID, objectID);
      if(lastSubjectID==subjectID && lastPredicateID == predicateID && lastObjectID == objectID){
        continue;
      }else{
        snapshots[i]->insertTriple(predicateID, subjectID, objectID, ORDERBYS);
        lastSubjectID = subjectID; lastPredicateID = predicateID, lastObjectID = objectID;
      }
    }
    mappedIn_sp.close();
    snapshots[i]->flush();


    cout<<i<<" Sort By Object start"<<endl;
    TempFile sortedByO("./SortByO");
    Sorter::sort(rawfacts_sp, sortedByO, skipIdIdId, compare321);
    assert(mappedIn_sp.open(sortedByO.getFile().c_str()));
    cout<<i<<" build snapshot start"<<endl;
    reader_sp = mappedIn_sp.getBegin(); limit_sp = mappedIn_sp.getEnd();
    loadTriple(reader_sp, subjectID, predicateID, objectID);
    lastSubjectID = subjectID; lastPredicateID = predicateID, lastObjectID = objectID;
    snapshots[i]->insertTriple(predicateID, subjectID, objectID, ORDERBYO);
    while(reader_sp<limit_sp){
      loadTriple(reader_sp, subjectID, predicateID, objectID);
      if(lastSubjectID==subjectID && lastPredicateID == predicateID && lastObjectID == objectID){
        continue;
      }else{
        snapshots[i]->insertTriple(predicateID, subjectID, objectID, ORDERBYO);
        lastSubjectID = subjectID; lastPredicateID = predicateID, lastObjectID = objectID;
      }
    }
    mappedIn_sp.close();
    snapshots[i]->flush();
    rawfacts_sp.discard();
    sortedByS.discard();
    sortedByO.discard();
  }
  

  logmap->flush();
  
  rawFacts.discard();
  sortByTimeStamp.discard();
}

Status TripleBitBuilder:: startBuildN3(string fileName)
{
  TempFile rawFacts("./test");

  ifstream in((char *)fileName.c_str());
  if (!in.is_open())
  {
    cerr << "Unable to open " << fileName << endl;
    return ERROR;
  }
  if (!N3Parse(fileName.c_str(), rawFacts))
  {
    in.close();
    return ERROR;
  }

  in.close();
  delete uriTable;
  uriTable = NULL;
  delete preTable;
  preTable = NULL;
  delete staReifTable;
  staReifTable = NULL;

  rawFacts.flush();

  cout << "paser over" << endl;

  //现在三元组信息只存在tempfile ：test.0中,resolveTriples会将数据排序并持久化，
  resolveTriples(rawFacts);

  return OK;
}


Status TripleBitBuilder::endBuild()
{
  timemap->time_mmap->flush();
  logmap->save();
  for(int i = 0; i < snapshots.size(); i++){
    snapshots[i]->save(i);
  }
  return OK;
}

// void TripleBitBuilder::print(int f) { bitmap->print(f); }
