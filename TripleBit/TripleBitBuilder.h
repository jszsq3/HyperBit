/*
 * TripleBitBuilder.h
 *
 *  Created on: Apr 6, 2010
 *      Author: root
 *
 *  build tripleBit table,predicate table, URI table
 */

#ifndef TRIPLEBITBUILDER_H_
#define TRIPLEBITBUILDER_H_

//#define TRIPLEBITBUILDER_DEBUG 1

class PredicateTable;
class URITable;
class TimeTable;
class URIStatisticsBuffer;
class StatementReificationTable;
class FindColumns;
class BitmapBuffer;
class Sorter;
class TempFile;
class StatisticsBuffer;
class LogmapBuffer;

#include "TripleBit.h"

#include <fstream>
#include <pthread.h>
#include <cassert>
#include <cstring>

#include "TurtleParser.hpp"
#include "ThreadPool.h"
#include "TempFile.h"
#include "TimeMap.h"
#include "StatisticsBuffer.h"
//#define MYDEBUG
using namespace std;

class TripleBitBuilder {
private:
    LogmapBuffer* logmap;
	vector<BitmapBuffer *> snapshots;
	PredicateTable* preTable;
	URITable* uriTable;
	TimeMap* timemap;
	vector<string> predicates;
	string dir;
	STInfo *stInfoBySP;
	STInfo *stInfoByOP;
	StatisticsBuffer* statBuffer[4];
	StatementReificationTable* staReifTable;
	FindColumns* columnFinder;
public:
	TripleBitBuilder();
	TripleBitBuilder(const string dir);
	void InitSnapshot(const string dir);
	static const uchar* skipIdIdId(const uchar* reader);
	static const uchar* skipIdIdIdTS(const uchar* reader);
	static int compare213(const uchar* left, const uchar* right);
	static int compare231(const uchar* left, const uchar* right);
	static int compare123(const uchar* left, const uchar* right);
	static int compare321(const uchar* left, const uchar* right);
	static int compareT(const uchar* left, const uchar* right);
	static inline void loadTriple(const uchar*& reader, ID& v1, ID& v2, ID& v3) {
		TempFile::readIDIDID(reader, v1, v2, v3);
	}
	static inline void loadTriple(const uchar* &reader, ID& v1, ID& v2, ID& v3,ID& v4) {
		TempFile::readIDIDID(reader, v1, v2, v3);
		TempFile::readID(reader, v4);
	}
	static inline void loadQuads(const uchar*& reader, ID& v1, ID& v2, ID& v3, TimeStamp& t) {
		TempFile::readIDIDIDTS(reader, v1, v2, v3, t);
	}
	static inline int cmpValue(ID l, ID r) {
		return (l < r) ? -1 : ((l > r) ? 1 : 0);
	}
	static inline int cmpTriples(ID l1, ID l2, ID l3, ID r1, ID r2, ID r3) {
		int c = cmpValue(l1, r1);
		if (c)
			return c;
		c = cmpValue(l2, r2);
		if (c)
			return c;
		return cmpValue(l3, r3);
	}
	static inline int cmpTimeStamp(ID l1, ID l2, ID l3, TimeStamp lt, ID r1, ID r2, ID r3, TimeStamp rt) {
		int c = cmpValue(lt, rt);
		if (c)
			return c;
		c = cmpValue(l1, r1);
		if (c)
			return c;
		c = cmpValue(l2, r2);
		if (c)
			return c;
		return cmpValue(l3, r3);
	}
	Status resolveTriples(TempFile& rawFacts);
	Status startBuildN3(string fileName);
	void parse(string& line, string& subject, string& predicate, string& object);
	void parse(string& line, string& subject, string& predicate, string& object, string& time);
	bool N3Parse(const char* filename, TempFile& rawFacts);
	//Status importFromMySQL(string db, string server, string username, string password);
	void NTriplesParse(const char* subject, const char* predicate, const char* object, TempFile& facts);
	void NTriplesParse(const char* subject, const char* predicate, const char* object, const char* str_time, TempFile& facts);
//	Status buildIndex();
	Status endBuild();
	static bool isStatementReification(const char* object);
	virtual ~TripleBitBuilder();
	void print(int f);
};
#endif /* TRIPLEBITBUILDER_H_ */
