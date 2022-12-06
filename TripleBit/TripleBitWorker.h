/*
 * TripleBitWorker.h
 *
 *  Created on: 2013-6-28
 *      Author: root
 */

#ifndef TRIPLEBITWORKER_H_
#define TRIPLEBITWORKER_H_

class BitmapBuffer;
class URITable;
class PredicateTable;
class TripleBitRepository;
class TripleBitQueryGraph;
class EntityIDBuffer;
class HashJoin;
class SPARQLLexer;
class SPARQLParser;
class QuerySemanticAnalysis;
class PlanGenerator;
class transQueueSW;
class transaction;
class TasksQueueWP;
class TripleBitWorkerQuery;

#include "TripleBit.h"
#include <boost/thread/thread.hpp>
using namespace boost;

class TripleBitWorker {
private:
	TripleBitRepository* tripleBitRepo;
	PredicateTable* preTable;
	URITable* uriTable;
	BitmapBuffer* bitmapBuffer;
	transQueueSW* transQueSW;

	QuerySemanticAnalysis* semAnalysis;
	PlanGenerator* planGen;
	TripleBitQueryGraph* queryGraph;
	vector<string> resultSet;
	vector<string>::iterator resBegin;
	vector<string>::iterator resEnd;
	vector<TripleNode> insertNodes; //����������

	struct Triple {
		string subject;
		string predicate;
		string object;
		string time;
		Triple(string subject, string predicate, string object):
			subject(subject), predicate(predicate), object(object) {}
	};
	map<ID, vector<Triple*>> unParseTriple;

	ID workerID;

	boost::mutex* uriMutex;

	TripleBitWorkerQuery* workerQuery;
	transaction *trans;

public:
	static triplebit::atomic<size_t> insertTripleCount;
	TripleBitWorker(TripleBitRepository* repo, ID workID);
	Status Execute(string& queryString);
	virtual ~TripleBitWorker();
	void parse(string& line, string& subject, string& predicate, string& object);
	bool N3Parse(const char* fileName);
	Status NTriplesParse(const char* subject, const char* predicate, const char* object, TripleNode& tripleNode);
	void Work();
	void Print();
};

#endif /* TRIPLEBITWORKER_H_ */
