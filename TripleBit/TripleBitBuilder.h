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
class URIStatisticsBuffer;
class StatementReificationTable;
class FindColumns;
class BitmapBuffer;
class Sorter;
class TempFile;
class StatisticsBuffer;

#include "TripleBit.h"

#include <fstream>
#include <pthread.h>
#include <cassert>
#include <cstring>

#include "TurtleParser.hpp"
#include "ThreadPool.h"
#include "TempFile.h"
#include "StatisticsBuffer.h"
//#define MYDEBUG
using namespace std;

class TripleBitBuilder {
private:
	//MySQL* mysql;
	BitmapBuffer* bitmap;
	PredicateTable* preTable;
	URITable* uriTable;
	vector<string> predicates;
	string dir;
	/// statistics buffer;
	STInfo *stInfoBySP;
	STInfo *stInfoByOP;
	StatisticsBuffer* statBuffer[4];
//	StatisticsBuffer *spStatisBuffer, *opStatisBuffer;
	StatementReificationTable* staReifTable;
	FindColumns* columnFinder;
public:
	TripleBitBuilder();
	TripleBitBuilder(const string dir);
	static const uchar* skipIdIdId(const uchar* reader);
	static int compare213(const uchar* left, const uchar* right);
	static int compare231(const uchar* left, const uchar* right);
	static int compare123(const uchar* left, const uchar* right);
	static int compare321(const uchar* left, const uchar* right);
	static inline void loadTriple(const uchar*& reader, ID& v1, ID& v2, ID& v3) {
		TempFile::readIDIDID(reader, v1, v2, v3);

	}
	static inline void loadTriple(const uchar* &reader, ID& v1, ID& v2, ID& v3,ID& v4) {
		TempFile::readIDIDID(reader, v1, v2, v3);
		TempFile::readID(reader, v4);

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
	Status resolveTriples(TempFile& rawFacts);
	Status startBuildN3(string fileName);
	void parse(string& line, string& subject, string& predicate, string& object,string& time);
	bool N3Parse(const char* filename, TempFile& rawFacts);
	//Status importFromMySQL(string db, string server, string username, string password);
	void NTriplesParse(const char* subject, const char* predicate, const char* object, const char* time, TempFile& facts);
//	Status buildIndex();
	Status endBuild();
	static bool isStatementReification(const char* object);
	virtual ~TripleBitBuilder();
	void print(int f);
};
#endif /* TRIPLEBITBUILDER_H_ */
