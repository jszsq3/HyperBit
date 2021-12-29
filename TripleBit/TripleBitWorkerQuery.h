/*
 * TripleBitWorkerQuery.h
 *
 *  Created on: 2013-7-1
 *      Author: root
 */

#ifndef TRIPLEBITWORKERQUERY_H_
#define TRIPLEBITWORKERQUERY_H_

class BitmapBuffer;
class URITable;
class PredicateTable;
class TripleBitRepository;
class TripleBitQueryGraph;
class EntityIDBuffer;
class HashJoin;
class TasksQueueWP;
class ResultBuffer;
class PartitionThreadPool;
class IndexForTT;
class SubTrans;
class ChunkTask;

#include "TripleBitQueryGraph.h"
#include "TripleBit.h"
#include "util/HashJoin.h"
#include "util/SortMergeJoin.h"
#include "util/atomic.hpp"
#include "TempBuffer.h"
#include "TempMMapBuffer.h"
#include "comm/TasksQueueChunk.h"
#include "comm/TempBufferQueue.h"
#include "comm/SortChunkTaskQueue.h"
#include "TempFile.h"
#include "Sorter.h"

typedef map<ID, EntityIDBuffer*> EntityIDListType;
typedef map<ID, EntityIDBuffer*>::iterator EntityIDListIterType;

class TripleBitWorkerQuery {
public:
	map<ID, ChunkManager*> chunkManager[2];
private:
	struct PSubTrans {
		ID pid;
		SubTrans *subTrans;
		PSubTrans(ID pid, SubTrans *subTrans) :
			pid(pid), subTrans(subTrans) {
		}
	};
	TripleBitRepository* tripleBitRepo;
	BitmapBuffer* bitmap;
	URITable* uriTable;
	PredicateTable* preTable;
	

	TripleBitQueryGraph* _queryGraph;
	TripleBitQueryGraph::SubQuery* _query;

	EntityIDListType EntityIDList;
	vector<TripleBitQueryGraph::JoinVariableNodeID> idTreeBFS;
	vector<TripleBitQueryGraph::JoinVariableNodeID> leafNode;
	vector<TripleBitQueryGraph::JoinVariableNodeID> varVec;

	ID workerID;

	//something about shared memory
	int partitionNum;
	vector<TasksQueueWP*> tasksQueueWP;
	vector<ResultBuffer*> resultWP;
	vector<boost::mutex*> tasksQueueWPMutex;
	map<ID, map<ID, MidResultBuffer*>> resultSet;

//	TasksQueueWP* tasksQueue;
	map<ID, TasksQueueWP*> tasksQueue;
	map<ID, ResultBuffer*> resultBuffer;
	map<ID, vector<TasksQueueChunk*>> chunkQueue[2];
	map<ID, vector<TempBuffer*>> chunkTempBuffer[2];
	map<ID, vector<const uchar*>> chunkStartPtr[2];
	map<ID, TempBufferQueue*> tempBufferQueue[2];

	//used to classity the InsertData or DeleteData
	map<ID, vector<TripleNode*> > tripleNodeMap;
	vector<ChunkTask*> chunkTasks;
	boost::mutex chunkTasksMutex;

	//used to get the results
	vector<int> varPos;
	vector<int> keyPos;
	vector<int> resultPos;
	vector<int> verifyPos;
	vector<ID> resultVec;
	vector<size_t> bufPreIndexs;
	bool needselect;

	HashJoin hashJoin;
	SortMergeJoin mergeJoin;

	timeval *transactionTime;

	vector<string>* resultPtr;
	vector<SortChunkTask *> sortTask;
	vector<TempBuffer*> tempBuffers;

	struct SortChunkTaskPara {
		TripleBitWorkerQuery *query;
		vector<SortChunkTask *> *sortTask;
		int start, end;
		SortChunkTaskPara(TripleBitWorkerQuery *query, vector<SortChunkTask *> *sortTask, int start, int end) :
			query(query), sortTask(sortTask), start(start), end(end) {
		}
	};

	struct EndForWorkTaskPara {
		TripleBitWorkerQuery *query;
		vector<TempBuffer *> *tempBuffers;
		int start, end;
		EndForWorkTaskPara(TripleBitWorkerQuery *query, vector<TempBuffer *> *tempBuffers, int start, int end) :
			query(query), tempBuffers(tempBuffers), start(start), end(end) {
		}
	};

	struct QueryChunkTaskPara {
		TripleBitWorkerQuery *query;
		ChunkTask *chunkTasks;
		int start, end;
		QueryChunkTaskPara(TripleBitWorkerQuery *query, ChunkTask *chunkTasks, int start, int end) :
			query(query), chunkTasks(chunkTasks), start(start), end(end) {
		}
	};

public:
	static triplebit::atomic<size_t> timeCount;
	static triplebit::atomic<size_t> tripleCount;
public:
	TripleBitWorkerQuery(TripleBitRepository*& repo, ID workID);
	virtual ~TripleBitWorkerQuery();

	void releaseBuffer();
	Status query(TripleBitQueryGraph* queryGraph, vector<string>& resultSet, timeval& transTime);
	void executeSortChunkTask(SortChunkTask *sortChunkTask);
	void executeSortChunkTaskPartition(const vector<SortChunkTask *>&sortTask, int start, int end);

private:

	void taskEnQueue(ChunkTask *chunkTask, TasksQueueChunk *tasksQueue);
	void handleTasksQueueChunk(TasksQueueChunk* tasksQueue);
	void executeEndForWorkTaskPartition(vector<TempBuffer*> &tempBuffers, int start, int end);

	void endForWork();
	void sortDataBase();

	Status excuteInsertData();
	void executeInsertData(const vector<TripleNode*> &tripleNodes);
	void executeInsertChunkData(ChunkTask *chunkTask);
	void combineTempBufferToSource(TempBuffer *buffer);

	Status excuteDeleteData();
	void executeDeleteData(const set<TripleNode*> &tripleNodes);
	bool excuteDeleteChunkData_lab(ChunkTask *chunkTask);
	bool excuteDeleteChunkData(ChunkTask *chunkTask);

	Status excuteDeleteClause();
	bool excuteSubTransDeleteClause(SubTrans* subTran);
	bool executeChunkTaskDeleteClause(ChunkTask *chunkTask);
	void excuteDeleteDataForDeleteClause(timeval &startTime, MidResultBuffer *buffer, const bool soType,
	                                     const ID predicateID, const bool constSubject, const ID subjectID, const double object, const char objType);

	Status excuteUpdate();
	void executeInsertTripleNodeForUpdate(TripleNode &tripleNode);
	void executeInsertChunkTaskForUpdate(ChunkTask *chunkTask);

	Status excuteQuery();
	Status singlePatternWithSingleVariable();
	Status singlePatternWithTwoVariable();
	void executeChunkTaskSQuery(ChunkTask* chunkTask, int start, int end);
	void executeChunkTaskQuery(ChunkTask* chunkTask);

	Status singleVariableJoin();
	Status acyclicJoin();
	Status cyclicJoin();
	void displayAllTriples();


	ResultIDBuffer* findEntityIDByTriple(TripleNode* triple, ID minID, ID maxID);
	void tasksEnQueue(ID predicateID, SubTrans *subTrans);
	void handleSubTransQueue(ID predicateID, TasksQueueWP* tasksQueue);
	void executeQuery(SubTrans *subTransaction);
	void findSubjectByPredicateAndObject(ChunkTask *chunkTask, MidResultBuffer *buffer);
	void findObjectBySubjectAndPredicate(ChunkTask *chunkTask, MidResultBuffer *buffer);
	void findSubjectByPredicate(ChunkTask *chunkTask, MidResultBuffer *buffer);
	void findObjectByPredicate(ChunkTask *chunkTask, MidResultBuffer *buffer);
	void findSubjectAndObjectByPredicate(ChunkTask *chunkTask, MidResultBuffer *buffer);

	void classifyTripleNode();


};

#endif /* TRIPLEBITWORKERQUERY_H_ */
