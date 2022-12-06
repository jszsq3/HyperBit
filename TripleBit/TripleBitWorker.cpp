/*
 * TripleBitWorker.cpp
 *
 *  Created on: 2013-6-28
 *      Author: root
 */

#include "SPARQLLexer.h"
#include "SPARQLParser.h"
#include "QuerySemanticAnalysis.h"
#include "PlanGenerator.h"
#include "TripleBitQueryGraph.h"
#include "MemoryBuffer.h"
#include "BitmapBuffer.h"
#include "TripleBitRepository.h"
#include "URITable.h"
#include "PredicateTable.h"
#include "EntityIDBuffer.h"
#include "util/BufferManager.h"
#include "TripleBitWorker.h"
#include "comm/TransQueueSW.h"
#include "TripleBitWorkerQuery.h"
#include "TempBuffer.h"
#include "OSFile.h"
#include "TurtleParser.hpp"

//#define MYDEBUG
//#define TOTAL_TIME

triplebit::atomic<size_t> TripleBitWorker::insertTripleCount(0);
TripleBitWorker::TripleBitWorker(TripleBitRepository* repo, ID workID) {
	tripleBitRepo = repo;
	preTable = repo->getPredicateTable();
	uriTable = repo->getURITable();
	// bitmapBuffer = repo->getBitmapBuffer();
	transQueSW = repo->getTransQueueSW();
	uriMutex = repo->getUriMutex();

	workerID = workID;

	queryGraph = new TripleBitQueryGraph();
	planGen = new PlanGenerator(*repo);
	semAnalysis = new QuerySemanticAnalysis(*repo);
	workerQuery = new TripleBitWorkerQuery(repo, workerID);
}

TripleBitWorker::~TripleBitWorker() {
	if (workerQuery != NULL) {
		delete workerQuery;
		workerQuery = NULL;
	}
	if (semAnalysis != NULL) {
		delete semAnalysis;
		semAnalysis = NULL;
	}
	if (planGen != NULL) {
		delete planGen;
		planGen = NULL;
	}
	if (queryGraph != NULL) {
		delete queryGraph;
		queryGraph = NULL;
	}
	transQueSW = NULL;
	uriMutex = NULL;
	uriTable = NULL;
	preTable = NULL;
	bitmapBuffer = NULL;
	tripleBitRepo = NULL;
}

void TripleBitWorker::Work() {
	while (1) {
		trans = transQueSW->DeQueue();
		string queryString = trans->transInfo;
		if (queryString == "exit") {
			delete trans;
			tripleBitRepo->workerComplete();
			break;
		}
		Execute(queryString);
		delete trans;

	}
}

void TripleBitWorker::parse(string &line, string &subject, string &predicate, string &object) {
	int l, r;

	l = line.find_first_of('<') + 1;
	r = line.find_first_of('>');
	subject = line.substr(l, r - l);
	line = line.substr(r + 1);

	l = line.find_first_of('<') + 1;
	r = line.find_first_of('>');
	predicate = line.substr(l, r - l);
	line = line.substr(r + 1);

	if (line.find_first_of('\"') != string::npos) {
		l = line.find_first_of('\"') + 1;
		r = line.find_last_of('\"');
		object = line.substr(l, r - l);
		line = line.substr(r + 1);
	}
	else {
		l = line.find_first_of('<') + 1;
		r = line.find_first_of('>');
		object = line.substr(l, r - l);
		line = line.substr(r + 1);
	}


}

Status TripleBitWorker::NTriplesParse(const char* subject, const char* predicate, const char* object,TripleNode& tripleNode) {
	ID subjectID, predicateID, objectID,timeID;
	if (preTable->getIDByPredicate(predicate, predicateID) == PREDICATE_NOT_BE_FINDED) {
		preTable->insertTable(predicate, predicateID);
	}
	if (uriTable->getIdByURI(subject, subjectID) == URI_NOT_FOUND) {
		uriTable->insertTable(subject, subjectID);
	}
	if (uriTable->getIdByURI(object, objectID) == URI_NOT_FOUND) {
		uriTable->insertTable(object, objectID);
	}
	tripleNode.subjectID = subjectID;
	tripleNode.predicateID = predicateID;
	tripleNode.objectID = objectID;
	return OK;
}

bool TripleBitWorker::N3Parse(const char* fileName) {
	//	cerr << "Parsing " << fileName << "..." << endl;
	try {
		MMapBuffer* slice = new MMapBuffer(fileName);
		const uchar* reader = (const uchar*)slice->getBuffer(), * tempReader;
		const uchar* limit = (const uchar*)(slice->getBuffer() + slice->getSize());
		char buffer[256];
		string line, subject, predicate, object;
		ID tr_id = 0;
		TripleNode tripleNode;
		while (reader < limit) {
			tempReader = reader;
			memset(buffer, 0, sizeof(buffer));
			while (tempReader < limit && *tempReader != '\n') {
				tempReader++;
			}
			if (tempReader != reader) {
				memcpy(buffer, reader, tempReader - reader);
				tempReader++;
				reader = tempReader;
				line = buffer;
				if (line.empty())
					continue;
				insertTripleCount++;
				parse(line, subject, predicate, object);
				if (subject.length() && predicate.length() && object.length()) {
					Status status = NTriplesParse(subject.c_str(), predicate.c_str(), object.c_str(),tripleNode);
					queryGraph->getQuery().tripleNodes.push_back(tripleNode);
				}
			}
			else {
				reader = tempReader;
				break;
			}

		}
	}
	catch (const TurtleParser::Exception&) {
		return false;
	}
	return true;
}

Status TripleBitWorker::Execute(string& queryString) {
	//����
	if (queryString.find_first_of("insert_path:") == 0) {
		//�������ݼ�·��
		string insert_path = queryString.substr(queryString.find_first_of(":") + 1);

		queryGraph->Clear();
		queryGraph->setOpType(TripleBitQueryGraph::INSERT_DATA);

		//�����������ݼ�
		N3Parse(insert_path.c_str());

		workerQuery->query(queryGraph, resultSet, trans->transTime);
	} 
	//��ѯ
	else {
		SPARQLLexer *lexer = new SPARQLLexer(queryString);
		SPARQLParser *parser = new SPARQLParser(*lexer);
		try {
			parser->parse();
		} catch (const SPARQLParser::ParserException& e) {
			cout << "Parser error: " << e.message << endl;
			return ERR;
		}

		if (parser->getOperationType() == SPARQLParser::QUERY) {
			queryGraph->Clear();
			uriMutex->lock();
			if (!this->semAnalysis->transform(*parser, *queryGraph)) {
				return NOT_FOUND;
			}
			uriMutex->unlock();

			if (queryGraph->knownEmpty() == true) {
				cout << "Empty result" << endl;
				return OK;
			}

			if (queryGraph->isPredicateConst() == false) {
				resultSet.push_back("predicate should be constant");
				return ERR;
			}

			planGen->generatePlan(*queryGraph);
			resultSet.clear();

			workerQuery->query(queryGraph, resultSet, trans->transTime);
		} 
		else {
			queryGraph->Clear();
			uriMutex->lock();
			if (!this->semAnalysis->transform(*parser, *queryGraph)) {
				return ERR;
			}
			uriMutex->unlock();

			workerQuery->query(queryGraph, resultSet, trans->transTime);

			for(uint i=0;i<resultSet.size();i++){
				cout<<i<<"\t"<<resultSet[i]<<endl;
			}
		}
		delete lexer;
		delete parser;
	}
	workerQuery->releaseBuffer();

	return OK;
}

void TripleBitWorker::Print() {
	//workerQuery->chunkManager
	TripleBitQueryGraph::SubQuery& query = queryGraph->getQuery();
	unsigned int i, size, j;
	size = query.tripleNodes.size();
	cout << "join triple size: " << size << endl;

	vector<TripleNode>& triples = query.tripleNodes;
	for (i = 0; i < size; i++) {
		cout << i << " triple: " << endl;
		cout << triples[i].constSubject << " " << triples[i].subjectID << endl;
		cout << triples[i].constPredicate << " " << triples[i].predicateID << endl;
		cout << triples[i].constObject << " " << triples[i].objectID << endl;
		cout << endl;
	}

	size = query.joinVariables.size();
	cout << "join variables size: " << size << endl;
	vector<TripleBitQueryGraph::JoinVariableNodeID>& variables = query.joinVariables;
	for (i = 0; i < size; i++) {
		cout << variables[i] << endl;
	}

	vector<TripleBitQueryGraph::JoinVariableNode>& nodes = query.joinVariableNodes;
	size = nodes.size();
	cout << "join variable nodes size: " << size << endl;
	for (i = 0; i < size; i++) {
		cout << i << "variable nodes" << endl;
		cout << nodes[i].value << endl;
		for (j = 0; j < nodes[i].appear_tpnodes.size(); j++) {
			cout << nodes[i].appear_tpnodes[j].first << " " << nodes[i].appear_tpnodes[j].second << endl;
		}
		cout << endl;
	}

	size = query.joinVariableEdges.size();
	cout << "join variable edges size: " << size << endl;
	vector<TripleBitQueryGraph::JoinVariableNodesEdge>& edge = query.joinVariableEdges;
	for (i = 0; i < size; i++) {
		cout << i << " edge" << endl;
		cout << "From: " << edge[i].from << "To: " << edge[i].to << endl;
	}

	cout << " query type: " << query.joinGraph << endl;
	cout << " root ID: " << query.rootID << endl;
	cout << " query leafs: ";
	size = query.leafNodes.size();
	for (i = 0; i < size; i++) {
		cout << query.leafNodes[i] << " ";
	}
	cout << endl;

	vector<ID>& projection = queryGraph->getProjection();
	cout << "variables need to project: " << endl;
	cout << "variable count: " << projection.size() << endl;
	for (i = 0; i < projection.size(); i++) {
		cout << projection[i] << endl;
	}
	cout << endl;
}
