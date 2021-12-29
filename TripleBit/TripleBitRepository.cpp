/*
 * TripleBitRespository.cpp
 *
 *  Created on: May 13, 2010
 *      Author: root
 */

#include "MemoryBuffer.h"
#include "TripleBitRepository.h"
#include "PredicateTable.h"
#include "URITable.h"
#include "TripleBitBuilder.h"
#include "BitmapBuffer.h"
#include "StatisticsBuffer.h"
#include "EntityIDBuffer.h"
#include "MMapBuffer.h"
#include "ThreadPool.h"
#include "TempMMapBuffer.h"
#include "OSFile.h"
#include <sys/time.h>
#include "comm/TransQueueSW.h"
#include "comm/TasksQueueWP.h"
#include "comm/ResultBuffer.h"
#include "comm/IndexForTT.h"
#include "util/Properties.h"
#include <boost/thread/thread.hpp>

//#define MYDEBUG
int TripleBitRepository::colNo = INT_MAX - 1;

TripleBitRepository::TripleBitRepository() {
	this->UriTable = NULL;
	this->preTable = NULL;
	this->bitmapBuffer = NULL;
	buffer = NULL;
	this->transQueSW = NULL;

//	spStatisBuffer = NULL;
//	opStatisBuffer = NULL;

	bitmapImage = NULL;
	bitmapPredicateImage = NULL;
}

TripleBitRepository::~TripleBitRepository() {
	TempMMapBuffer::deleteInstance();
	if (tripleBitWorker.size() == workerNum) {
		for (size_t i = 1; i <= workerNum; ++i) {
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

	if (bitmapBuffer != NULL)
		delete bitmapBuffer;
	bitmapBuffer = NULL;

	if (stInfoBySP != NULL) {
		delete stInfoBySP;
		stInfoBySP = NULL;
	}

	if (stInfoByOP != NULL) {
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

	if (bitmapImage != NULL) {
		bitmapImage->close();
		delete bitmapImage;
		bitmapImage = NULL;
	}

	if (bitmapPredicateImage != NULL) {
		bitmapPredicateImage->close();
		delete bitmapPredicateImage;
		bitmapPredicateImage = NULL;
	}

	if (indexForTT != NULL)
		delete indexForTT;
	indexForTT = NULL;

	if (uriMutex != NULL) {
		delete uriMutex;
		uriMutex = NULL;
	}
}

bool TripleBitRepository::find_pid_by_string(PID& pid, const string& str) {
	if (preTable->getIDByPredicate(str.c_str(), pid) != OK)
		return false;
	return true;
}

bool TripleBitRepository::find_pid_by_string_update(PID& pid, const string& str) {
	if (preTable->getIDByPredicate(str.c_str(), pid) != OK) {
		if (preTable->insertTable(str.c_str(), pid) != OK)
			return false;
	}
	return true;
}

bool TripleBitRepository::find_soid_by_string(SOID& soid, const string& str) {
	if (UriTable->getIdByURI(str.c_str(), soid) != URI_FOUND)
		return false;
	return true;
}

bool TripleBitRepository::find_soid_by_string_update(SOID& soid, const string& str) {
	if (UriTable->getIdByURI(str.c_str(), soid) != URI_FOUND) {
		if (UriTable->insertTable(str.c_str(), soid) != OK)
			return false;
	}
	return true;
}

bool TripleBitRepository::find_string_by_pid(string& str, PID& pid) {
	if (preTable->getPredicateByID(str, pid) == OK)
		return true;
	return false;
}

bool TripleBitRepository::find_string_by_soid(string& str, SOID& soid) {
	if (UriTable->getURIById(str, soid) == URI_NOT_FOUND)
		return false;

	return true;
}

int TripleBitRepository::get_predicate_count(PID pid) {
	int count1 = bitmapBuffer->getChunkManager(pid, ORDERBYS)->getTripleCount();
	int count2 = bitmapBuffer->getChunkManager(pid, ORDERBYO)->getTripleCount();

	return count1 + count2;
}

bool TripleBitRepository::lookup(const string& str, ID& id) {
	if (preTable->getIDByPredicate(str.c_str(), id) != OK && UriTable->getIdByURI(str.c_str(), id) != URI_FOUND)
		return false;

	return true;
}
int TripleBitRepository::get_object_count(double object, char objType) {
	longlong count;
//	spStatisBuffer->getStatisBySO(object, count, objType);
	stInfoByOP->get(object, objType, count);
	return count;
}

int TripleBitRepository::get_subject_count(ID subjectID) {
	longlong count;
//	opStatisBuffer->getStatisBySO(subjectID, count);
	stInfoBySP->get(subjectID, STRING, count);
	return count;
}

int TripleBitRepository::get_subject_predicate_count(ID subjectID, ID predicateID) {
	longlong count;
	stInfoBySP->get(subjectID, STRING, predicateID, count);
//	spStatisBuffer->getStatis(subjectID, predicateID, count);
	return count;
}

int TripleBitRepository::get_object_predicate_count(double object, ID predicateID, char objType) {
	longlong count;
	stInfoByOP->get(object, objType, predicateID, count);
//	opStatisBuffer->getStatis(object, predicateID, count, objType);
	return count;
}

int TripleBitRepository::get_subject_object_count(ID subjectID, double object, char objType) {
	return 1;
}

Status TripleBitRepository::getSubjectByObjectPredicate(double object, ID pod, char objType) {
	pos = 0;
	return OK;
}

ID TripleBitRepository::next() {
	ID id;
	Status s = buffer->getID(id, pos);
	if (s != OK)
		return 0;

	pos++;
	return id;
}

//�������ݿ�
TripleBitRepository* TripleBitRepository::create(const string& path) { //path = database/

	TripleBitRepository* repo = new TripleBitRepository();
	repo->dataBasePath = path;

	repo->UriTable = URITable::load(path);
	repo->preTable = PredicateTable::load(path);

	string filename = path + "BitmapBuffer";// filename=database/BitmapBuffer

	repo->bitmapImage = new MMapBuffer(filename.c_str(), 0);// filename=database/BitmapBuffer

	string predicateFile(filename); // predicateFile = database/BitmapBuffer_predicate
	predicateFile.append("_predicate");


	string tempMMap(filename); tempMMap.append("_temp"); //tempMMap=database/BitmapBuffer_temp  ������Ų��������
	TempMMapBuffer::create(tempMMap.c_str(), repo->bitmapImage->getSize());

	std::map<std::string, std::string> properties;
	Properties::parse(path, properties);
	string usedPage = properties["usedpage"];
	if (!usedPage.empty()) {
		TempMMapBuffer::getInstance().setUsedPage(stoull(usedPage));
	}
	else {
		TempMMapBuffer::getInstance().setUsedPage(0);
	}


	repo->bitmapPredicateImage = new MMapBuffer(predicateFile.c_str(), 0);
	repo->bitmapBuffer = BitmapBuffer::load(repo->bitmapImage, repo->bitmapPredicateImage);

	//repo->bitmapBuffer->print(2);


	//	filename = path + "/statIndex";
	//	MMapBuffer* indexBufferFile = MMapBuffer::create(filename.c_str(), 0);
	//	uchar* indexBuffer = indexBufferFile->get_address();

	//	string statFilename = path + "/subjectpredicate_statis";
	timeval start, end;
	string statFilename = path + "/statistic_sp";
	gettimeofday(&start, NULL);
	repo->stInfoBySP = new STInfo(statFilename);
	statFilename = path + "/statistic_op";
	repo->stInfoByOP = new STInfo(statFilename);
	gettimeofday(&end, NULL);
	// cout << "load statistic " << ((end.tv_sec - start.tv_sec) * 1000000 + end.tv_usec - start.tv_usec) / 1000000.0
	// 		<< "s" << endl;

//	repo->spStatisBuffer = StatisticsBuffer::load(SUBJECTPREDICATE_STATIS, statFilename, indexBuffer);
//	statFilename = path + "/objectpredicate_statis";
//	repo->opStatisBuffer = StatisticsBuffer::load(OBJECTPREDICATE_STATIS, statFilename, indexBuffer);

	repo->buffer = new EntityIDBuffer();

	repo->partitionNum = repo->bitmapPredicateImage->get_length() / ((sizeof(ID) + sizeof(SOType) + sizeof(size_t) * 2) * 2);	//numbers of predicate

	repo->workerNum = WORKERNUM;
	repo->indexForTT = new IndexForTT(WORKERNUM);

#ifdef DEBUG
	cout << "partitionNumber: " << repo->partitionNum << endl;
#endif

	repo->sharedMemoryInit();

	gettimeofday(&start, NULL);
	for (size_t i = 1; i <= repo->workerNum; ++i) {
		repo->tripleBitWorker[i] = new TripleBitWorker(repo, i);
	}
	gettimeofday(&end, NULL);
	// cout << "load tripleBitWorker " << ((end.tv_sec - start.tv_sec) * 1000000 + end.tv_usec - start.tv_usec) / 1000000.0
	// 		<< "s" << endl;

	for (size_t i = 1; i <= repo->workerNum; ++i) {
		boost::thread thrd(boost::thread(boost::bind<void>(&TripleBitRepository::tripleBitWorkerInit, repo, i)));
	}
	//	delete indexBufferFile;
	//	indexBufferFile = NULL;
		// cout << "load complete!" << endl;

	return repo;
}

void TripleBitRepository::tripleBitWorkerInit(int i) {
	tripleBitWorker[i]->Work();
}

Status TripleBitRepository::sharedMemoryInit() {
	//Init the transQueueSW shared Memory
	sharedMemoryTransQueueSWInit();

	//Init the tasksQueueWP shared memory
	sharedMemoryTasksQueueWPInit();

	//Init the resultWP shared memory
	sharedMemoryResultWPInit();

	uriMutex = new boost::mutex();

	return OK;
}

Status TripleBitRepository::sharedMemoryDestroy() {
	//Destroy the transQueueSW shared Memory
	sharedMemoryTransQueueSWDestroy();
#ifdef MYDEBUG
	cout << "shared memory TransQueueSW destoried" << endl;
#endif

	//Destroy the tasksQueueWP shared memory
	sharedMemoryTasksQueueWPDestroy();
#ifdef MYDEBUG
	cout << "shared memory TasksQueueWP destoried" << endl;
#endif

	//Destroy the ResultWP shared memory
	sharedMemoryResultWPDestroy();
#ifdef MYDEBUG
	cout << "shared memory ResultWP destoried" << endl;
#endif

	return OK;
}

Status TripleBitRepository::sharedMemoryTransQueueSWInit() {
	transQueSW = new transQueueSW();
#ifdef DEBUG
	cout << "transQueSW(Master) Address: " << transQueSW << endl;
#endif
	if (transQueSW == NULL) {
		cout << "TransQueueSW Init Failed!" << endl;
		return ERROR_UNKOWN;
	}
	return OK;
}

Status TripleBitRepository::sharedMemoryTransQueueSWDestroy() {
	if (transQueSW != NULL) {
		delete transQueSW;
		transQueSW = NULL;
	}
	return OK;
}

Status TripleBitRepository::sharedMemoryTasksQueueWPInit() {
	for (size_t partitionID = 1; partitionID <= this->partitionNum; ++partitionID) {
		TasksQueueWP* tasksQueue = new TasksQueueWP();
		if (tasksQueue == NULL) {
			cout << "TasksQueueWP Init Failed!" << endl;
			return ERROR_UNKOWN;
		}
		this->tasksQueueWP.push_back(tasksQueue);

		boost::mutex *wpMutex = new boost::mutex();
		this->tasksQueueWPMutex.push_back(wpMutex);
	}
	return OK;
}

Status TripleBitRepository::sharedMemoryTasksQueueWPDestroy() {
	vector<TasksQueueWP*>::iterator iter = this->tasksQueueWP.begin(), limit = this->tasksQueueWP.end();
	for (; iter != limit; ++iter) {
		delete *iter;
		*iter = NULL;
	}
	this->tasksQueueWP.clear();
	this->tasksQueueWP.swap(this->tasksQueueWP);
	for (vector<boost::mutex *>::iterator iter = tasksQueueWPMutex.begin(); iter != tasksQueueWPMutex.end(); iter++) {
		delete *iter;
		*iter = NULL;
	}
	return OK;
}

Status TripleBitRepository::sharedMemoryResultWPInit() {
	for (size_t workerID = 1; workerID <= this->workerNum; ++workerID) {
		for (size_t partitionID = 1; partitionID <= this->partitionNum; ++partitionID) {
			ResultBuffer* resBuffer = new ResultBuffer();
			if (resBuffer == NULL) {
				cout << "ResultBufferWP Init Failed!" << endl;
				return ERROR_UNKOWN;
			}

			this->resultWP.push_back(resBuffer);
		}
	}
	return OK;
}

Status TripleBitRepository::sharedMemoryResultWPDestroy() {
	vector<ResultBuffer*>::iterator iter = this->resultWP.begin(), limit = this->resultWP.end();
	for (; iter != limit; ++iter) {
		delete *iter;
	}
	this->resultWP.clear();
	this->resultWP.swap(this->resultWP);
	return OK;
}

Status TripleBitRepository::tempMMapDestroy() {
	//endPartitionMaster();
	//bitmapBuffer->endUpdate(bitmapPredicateImage, bitmapImage);
	TempMMapBuffer::deleteInstance();
	return OK;
}

static void getQuery(string& queryStr, const char* filename) {
	ifstream f;
	f.open(filename);

	queryStr.clear();

	if (f.fail() == true) {
		MessageEngine::showMessage("open query file error!", MessageEngine::WARNING);
		return;
	}

	char line[250];
	while (!f.eof()) {
		f.getline(line, 250);
		queryStr.append(line);
		queryStr.append(" ");
	}

	f.close();
}

Status TripleBitRepository::nextResult(string& str) {
	if (resBegin != resEnd) {
		str = *resBegin;
		resBegin++;
		return OK;
	}
	return ERROR;
}

Status TripleBitRepository::execute(string queryStr) {
	resultSet.resize(0);
	return OK;
}

void TripleBitRepository::endForWorker() {
	string queryStr("exit");
	for (size_t i = 1; i <= workerNum; ++i) {
		transQueSW->EnQueue(queryStr);
	}
	indexForTT->wait();
}

void TripleBitRepository::workerComplete() {
	indexForTT->completeOneTriple();
}

extern char* QUERY_PATH;
void TripleBitRepository::cmd_line(FILE* fin, FILE* fout) {
	char cmd[256];
	while (true) {
		fflush(fin);
		fprintf(fout, ">>>");
		fscanf(fin, "%s", cmd);
		resultSet.resize(0);
		if (strcmp(cmd, "dp") == 0 || strcmp(cmd, "dumppredicate") == 0) {
			getPredicateTable()->dump();
		} else if (strcmp(cmd, "query") == 0) {

		} else if (strcmp(cmd, "source") == 0) {

			string queryStr;
			::getQuery(queryStr, string(QUERY_PATH).append("queryLUBM6").c_str());

			if (queryStr.length() == 0)
				continue;
			execute(queryStr);
		} else if (strcmp(cmd, "exit") == 0) {
			break;
		} else {
			string queryStr;
			::getQuery(queryStr, string(QUERY_PATH).append(cmd).c_str());

			if (queryStr.length() == 0)
				continue;
			execute(queryStr);
		}
	}
}

void TripleBitRepository::cmd_line_sm(FILE* fin, FILE *fout, const string query_path) {
	ThreadPool::createAllPool();
	char cmd[256];
	while (true) {
		fflush(fin);
		fprintf(fout, ">>>");
		fscanf(fin, "%s", cmd);
		resultSet.resize(0);
		if (strcmp(cmd, "dp") == 0 || strcmp(cmd, "dumppredicate") == 0) {
			getPredicateTable()->dump();
		} else if (strcmp(cmd, "query") == 0) {
		} else if (strcmp(cmd, "source") == 0) {
			string queryStr;
			::getQuery(queryStr, string(query_path).append("queryLUBM6").c_str());
			if (queryStr.length() == 0)
				continue;
		} else if (strcmp(cmd, "exit") == 0) {
			endForWorker();
			break;
		} else {
			string queryStr;
			::getQuery(queryStr, string(query_path).append(cmd).c_str());

			if (queryStr.length() == 0)
				continue;
			transQueSW->EnQueue(queryStr);
		}
	}

	std::map<std::string, std::string> properties;
	properties["usedpage"] = to_string(TempMMapBuffer::getInstance().getUsedPage());
	string db = DATABASE_PATH;
	Properties::persist(db, properties);
	tempMMapDestroy();
	ThreadPool::deleteAllPool();
}

void TripleBitRepository::cmd_line_sm(FILE* fin, FILE *fout, const string query_path, const string query) {
	ThreadPool::createAllPool();
	//����ʱ������
	//ShiXu::parse(this->dataBasePath, this->shixu);
	char *cmd = strdup(query.c_str());
	resultSet.resize(0);
	if (strcmp(cmd, "dp") == 0 || strcmp(cmd, "dumppredicate") == 0) {
		getPredicateTable()->dump();
	} else if (strcmp(cmd, "query") == 0) {
	} else if (strcmp(cmd, "source") == 0) {
		string queryStr;
		::getQuery(queryStr, string(query_path).append("queryLUBM6").c_str());
	} else if (strcmp(cmd, "exit") == 0) {
		endForWorker();
	} else {
		string queryStr;
		::getQuery(queryStr, string(query_path).append(cmd).c_str());
		transQueSW->EnQueue(queryStr);
	}
	endForWorker();

	std::map<std::string, std::string> properties;
	properties["usedpage"] = to_string(TempMMapBuffer::getInstance().getUsedPage());
	string db = DATABASE_PATH;
	Properties::persist(db, properties);

	tempMMapDestroy();
	ThreadPool::deleteAllPool();
}

extern char* QUERY_PATH;
void TripleBitRepository::cmd_line_cold(FILE *fin, FILE *fout, const string cmd) {
	string queryStr;
	getQuery(queryStr, string(QUERY_PATH).append(cmd).c_str());
	if (queryStr.length() == 0) {
		cout << "queryStr.length() == 0!" << endl;
		return;
	}
	cout << "DataBase: " << DATABASE_PATH << " Query:" << cmd << endl;
	transQueSW->EnQueue(queryStr);
	endForWorker();
	tempMMapDestroy();
}

extern char* QUERY_PATH;
void TripleBitRepository::cmd_line_warm(FILE *fin, FILE *fout, const string cmd) {
	string queryStr;
	getQuery(queryStr, string(QUERY_PATH).append(cmd).c_str());
	if (queryStr.length() == 0) {
		cout << "queryStr.length() == 0" << endl;
		return;
	}
	cout << "DataBase: " << DATABASE_PATH << " Query:" << cmd << endl;
	for (int i = 0; i < 10; i++) {
		transQueSW->EnQueue(queryStr);
	}
	endForWorker();
	tempMMapDestroy();
}

void TripleBitRepository::insertData(const string& query_path) {
	ThreadPool::createAllPool();
	resultSet.resize(0);
	string queryStr("insert_path:");
	transQueSW->EnQueue(queryStr.append(query_path));
	endForWorker();
	std::map<std::string, std::string> properties;
	properties["usedpage"] = to_string(TempMMapBuffer::getInstance().getUsedPage());
	string db = DATABASE_PATH;
	Properties::persist(db, properties);
	//������ݿ�״̬
	bitmapBuffer->print(2);
	tempMMapDestroy();
	ThreadPool::deleteAllPool();
}

void TripleBitRepository::print() {
	tripleBitWorker[0]->Print();
}
