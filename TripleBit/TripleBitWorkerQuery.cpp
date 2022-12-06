/*
 * TripleBitWorkerQuery.cpp
 *
 *  Created on: 2013-7-1
 *      Author: root
 */

#include "MemoryBuffer.h"
#include "BitmapBuffer.h"
#include "TripleBitWorkerQuery.h"
#include "TripleBitRepository.h"
#include "URITable.h"
#include "PredicateTable.h"
#include "TripleBitQueryGraph.h"
#include "EntityIDBuffer.h"
#include "util/BufferManager.h"
#include "comm/TasksQueueWP.h"
#include "comm/ResultBuffer.h"
#include "comm/IndexForTT.h"
#include "comm/Tasks.h"
#include <algorithm>
#include <math.h>
#include <pthread.h>
#include "util/Timestamp.h"

#define BLOCK_SIZE 1024
//#define PRINT_RESULT
#define COLDCACHE
//#define MYDEBUG

triplebit::atomic<size_t> TripleBitWorkerQuery::timeCount(0);
triplebit::atomic<size_t> TripleBitWorkerQuery::tripleCount(0);

//初始化
TripleBitWorkerQuery::TripleBitWorkerQuery(TripleBitRepository *&repo, ID workID)
{
	tripleBitRepo = repo;
	// bitmap = repo->getBitmapBuffer();
	uriTable = repo->getURITable();
	preTable = repo->getPredicateTable();

	workerID = workID; // 1 ~ WORKERNUM

	tasksQueueWP = repo->getTasksQueueWP();
	resultWP = repo->getResultWP();
	tasksQueueWPMutex = repo->getTasksQueueWPMutex();
	partitionNum = repo->getPartitionNum();

	int chunkNum = 0;

	for (int partitionID = 1; partitionID <= partitionNum; ++partitionID)
	{
		resultBuffer[partitionID] = resultWP[(workerID - 1) * partitionNum + partitionID - 1];

		chunkManager[ORDERBYS][partitionID] = bitmap->getChunkManager(partitionID, ORDERBYS);
		chunkManager[ORDERBYO][partitionID] = bitmap->getChunkManager(partitionID, ORDERBYO);
		chunkNum += chunkManager[ORDERBYS][partitionID]->getChunkNumber();
		chunkNum += chunkManager[ORDERBYO][partitionID]->getChunkNumber();

		tempBufferQueue[ORDERBYS][partitionID] = new TempBufferQueue(partitionID);
		tempBufferQueue[ORDERBYO][partitionID] = new TempBufferQueue(partitionID);

		vector<TempBuffer *> tempBufferByS;
		TempBuffer *tempBuffer;

		vector<TasksQueueChunk *> tasksQueueChunkS; //
		TasksQueueChunk *tasksQueueChunk;			//

		const uchar *startPtr = chunkManager[ORDERBYS][partitionID]->getStartPtr();
		startPtr -= sizeof(ChunkManagerMeta);
		tempBuffer = new TempBuffer(partitionID, 0, ORDERBYS);
		tempBufferByS.push_back(tempBuffer);
		chunkStartPtr[ORDERBYS][partitionID].push_back(startPtr + sizeof(ChunkManagerMeta));

		tasksQueueChunk = new TasksQueueChunk(startPtr + sizeof(ChunkManagerMeta), 0, ORDERBYS); //
		tasksQueueChunkS.push_back(tasksQueueChunk);											 //

		for (size_t chunkID = 1; chunkID < chunkManager[ORDERBYS][partitionID]->getChunkNumber(); chunkID++)
		{
			tempBuffer = new TempBuffer(partitionID, chunkID, ORDERBYS);
			tempBufferByS.push_back(tempBuffer);
			chunkStartPtr[ORDERBYS][partitionID].push_back(startPtr + chunkID * MemoryBuffer::pagesize);

			tasksQueueChunk = new TasksQueueChunk(startPtr + chunkID * MemoryBuffer::pagesize, chunkID, ORDERBYS); //
			tasksQueueChunkS.push_back(tasksQueueChunk);														   //
		}
		chunkTempBuffer[ORDERBYS][partitionID] = tempBufferByS;

		chunkQueue[ORDERBYS][partitionID] = tasksQueueChunkS; //

		vector<TempBuffer *> tempBufferByO;
		vector<TasksQueueChunk *> tasksQueueChunkO; //

		startPtr = chunkManager[ORDERBYO][partitionID]->getStartPtr();
		startPtr -= sizeof(ChunkManagerMeta);
		tempBuffer = new TempBuffer(partitionID, 0, ORDERBYO);
		tempBufferByO.push_back(tempBuffer);
		chunkStartPtr[ORDERBYO][partitionID].push_back(startPtr + sizeof(ChunkManagerMeta));

		tasksQueueChunk = new TasksQueueChunk(startPtr + sizeof(ChunkManagerMeta), 0, ORDERBYO); //
		tasksQueueChunkO.push_back(tasksQueueChunk);											 //

		for (size_t chunkID = 1; chunkID < chunkManager[ORDERBYO][partitionID]->getChunkNumber(); chunkID++)
		{
			tempBuffer = new TempBuffer(partitionID, chunkID, ORDERBYO);
			tempBufferByO.push_back(tempBuffer);
			chunkStartPtr[ORDERBYO][partitionID].push_back(startPtr + chunkID * MemoryBuffer::pagesize);

			tasksQueueChunk = new TasksQueueChunk(startPtr + chunkID * MemoryBuffer::pagesize, chunkID, ORDERBYO); //
			tasksQueueChunkO.push_back(tasksQueueChunk);														   //
		}
		chunkTempBuffer[ORDERBYO][partitionID] = tempBufferByO;

		chunkQueue[ORDERBYO][partitionID] = tasksQueueChunkO; //
	}
	// cout << "chunkNum: " << chunkNum << endl;
}

TripleBitWorkerQuery::~TripleBitWorkerQuery()
{
	for (int soType = 0; soType < 2; soType++)
	{
		for (ID predicateID = 1; predicateID <= partitionNum; predicateID++)
		{
			for (ID chunkID = 0; chunkID < chunkManager[soType][predicateID]->getChunkNumber(); chunkID++)
			{
				delete chunkTempBuffer[soType][predicateID][chunkID];
				chunkTempBuffer[soType][predicateID][chunkID] = NULL;
				delete chunkQueue[soType][predicateID][chunkID];
				chunkQueue[soType][predicateID][chunkID] = NULL;
				chunkStartPtr[soType][predicateID][chunkID] = NULL;
			}
			delete tempBufferQueue[soType][predicateID];
			tempBufferQueue[soType][predicateID] = NULL;
			chunkManager[soType][predicateID] = NULL;
		}
	}

	uriTable = NULL;
	preTable = NULL;
	bitmap = NULL;
	tripleBitRepo = NULL;
	EntityIDList.clear();
}

void TripleBitWorkerQuery::releaseBuffer()
{
	idTreeBFS.clear();
	leafNode.clear();
	varVec.clear();

	EntityIDListIterType iter = EntityIDList.begin();

	for (; iter != EntityIDList.end(); iter++)
	{
		if (iter->second != NULL)
			BufferManager::getInstance()->freeBuffer(iter->second);
	}

	BufferManager::getInstance()->reserveBuffer();
	EntityIDList.clear();
}

//执行函数
Status TripleBitWorkerQuery::query(TripleBitQueryGraph *queryGraph, vector<string> &resultSet, timeval &transTime)
{
	this->_queryGraph = queryGraph;
	this->_query = &(queryGraph->getQuery());
	this->resultPtr = &resultSet;
	this->transactionTime = &transTime;

	switch (_queryGraph->getOpType())
	{
	case TripleBitQueryGraph::QUERY:
		return excuteQuery();
	case TripleBitQueryGraph::INSERT_DATA:
		return excuteInsertData();
		/*
	case TripleBitQueryGraph::DELETE_DATA:
		return excuteDeleteData();
	case TripleBitQueryGraph::DELETE_CLAUSE:
		return excuteDeleteClause();
	case TripleBitQueryGraph::UPDATE:
		return excuteUpdate();
		*/
	}
	return OK;
}

//缓冲区数据持久化到磁盘
void TripleBitWorkerQuery::endForWork()
{

	tempBuffers.clear();
	vector<TempBuffer *>().swap(tempBuffers);

	for (size_t predicateID = 1; predicateID <= partitionNum; ++predicateID)
	{
		TempBuffer *tempBuffer;
		for (size_t chunkID = 0; chunkID < chunkManager[ORDERBYS][predicateID]->getChunkNumber(); chunkID++)
		{
			tempBuffer = chunkTempBuffer[ORDERBYS][predicateID][chunkID];
			if (tempBuffer != NULL && tempBuffer->getSize())
			{
				tempBuffers.push_back(tempBuffer);
			}
		}

		for (size_t chunkID = 0; chunkID < chunkManager[ORDERBYO][predicateID]->getChunkNumber(); chunkID++)
		{
			tempBuffer = chunkTempBuffer[ORDERBYO][predicateID][chunkID];
			if (tempBuffer != NULL && tempBuffer->getSize())
			{
				tempBuffers.push_back(tempBuffer);
			}
		}
	}

	int start, end;
	int persize = (tempBuffers.size() % gthread == 0) ? (tempBuffers.size() / gthread) : (tempBuffers.size() / gthread + 1);

	for (int i = 0; i < gthread; i++)
	{
		start = persize * i;
		end = (start + persize > tempBuffers.size()) ? tempBuffers.size() : (start + persize);
		ThreadPool::getSubTranPool()[i]->addTask(boost::bind(&TripleBitWorkerQuery::executeEndForWorkTaskPartition, this, boost::ref(tempBuffers), start, end));
	}
}

void TripleBitWorkerQuery::executeEndForWorkTaskPartition(vector<TempBuffer *> &tempBuffers, int start, int end)
{
	for (int i = start; i < end; i++)
	{
		// cout << "TripleBitWorkerQuery::executeEndForWorkTaskPartition" << endl;
		combineTempBufferToSource(tempBuffers[i]);
	}
}

//排序
void TripleBitWorkerQuery::sortDataBase()
{
	SortChunkTask *sortChunkTask = NULL;

	sortTask.clear();
	vector<SortChunkTask *>().swap(sortTask);

	for (int soType = 0; soType < 2; soType++)
	{
		for (int predicateID = 1; predicateID <= partitionNum; ++predicateID)
		{
			for (size_t chunkID = 0; chunkID < chunkManager[soType][predicateID]->getChunkNumber(); chunkID++)
			{
				MetaData *metaData = (MetaData *)(chunkStartPtr[soType][predicateID][chunkID]);
				if (metaData->updateCount)
				{
					sortChunkTask = new SortChunkTask(chunkStartPtr[soType][predicateID][chunkID], predicateID, chunkID, soType);
					sortTask.push_back(sortChunkTask);
					metaData->updateCount = 0;
				}
			}
		}
	}

	int start, end;
	int persize = (sortTask.size() % gthread == 0) ? (sortTask.size() / gthread) : (sortTask.size() / gthread + 1);
	for (int i = 0; i < gthread; i++)
	{
		start = persize * i;
		end = (start + persize > sortTask.size()) ? sortTask.size() : (start + persize);
		ThreadPool::getSubTranPool()[i]->addTask(boost::bind(&TripleBitWorkerQuery::executeSortChunkTaskPartition, this, boost::ref(sortTask), start, end));
	}
}

void TripleBitWorkerQuery::executeSortChunkTaskPartition(const vector<SortChunkTask *> &sortTask, int start, int end)
{
	for (int i = start; i < end; i++)
	{
		executeSortChunkTask(sortTask[i]);
	}
}

//块级别重排
void TripleBitWorkerQuery::executeSortChunkTask(SortChunkTask *sortChunkTask)
{
	const uchar *startPtr = sortChunkTask->startPtr; //第一个数据块的开始指针(指向第一个数据块的元数据)
	ID chunkID = sortChunkTask->chunkID;
	ID predicateID = sortChunkTask->predicateID;
	bool soType = sortChunkTask->soType;
	ID subjectID;
	ID objectID;
	uint len;
	ID lastSubjectID;
	ID lastObjectID;
	TempFile sorted("./sort");
	MetaData *metaData;
	MetaData *metaDataFirst;

	//读数据
	const uchar *readStartPtr = startPtr; //数据块的开始指针
	metaData = (MetaData *)readStartPtr;
	const uchar *read = readStartPtr + sizeof(MetaData);	   //块的数据区开始指针
	const uchar *readEnd = readStartPtr + metaData->usedSpace; //块的数据区结束指针
	//读取所有块的内容
	//第一个块
	while (read < readEnd)
	{
		chunkManager[0][predicateID]->read(read, subjectID, objectID);
		if (subjectID == 0)
		{
			continue;
		}
		sorted.writeID(subjectID);
		sorted.writeID(objectID);
	}
	//其他块
	while (metaData->NextPageNo != -1)
	{
		readStartPtr = TempMMapBuffer::getInstance().getAddress() + (metaData->NextPageNo) * MemoryBuffer::pagesize;
		metaData = (MetaData *)readStartPtr;
		read = readStartPtr + sizeof(MetaData);
		readEnd = readStartPtr + metaData->usedSpace;
		while (read < readEnd)
		{
			chunkManager[0][predicateID]->read(read, subjectID, objectID);
			if (subjectID == 0)
			{
				continue;
			}
			sorted.writeID(subjectID);
			sorted.writeID(objectID);
		}
	}

	//排序
	TempFile tempSorted("./tempSort");
	if (soType == ORDERBYS)
	{
		Sorter::sort(sorted, tempSorted, TempFile::skipIDID, TempFile::compare12);
	}
	else if (soType == ORDERBYO)
	{
		Sorter::sort(sorted, tempSorted, TempFile::skipIDID, TempFile::compare21);
	}
	else
	{
		cout << "TripleBitWorkerQuery::executeSortChunkTask error" << endl;
	}

	//写数据
	uchar *writeStartPtr = const_cast<uchar *>(startPtr); //数据块的开始指针
	//取metaData
	metaData = (MetaData *)writeStartPtr;
	//记录第一个数据块的元数据
	metaDataFirst = metaData;
	//重置metaData
	metaDataFirst->pageNo = -1;
	metaData->usedSpace = sizeof(MetaData);
	metaData->tripleCount = 0;
	metaData->updateCount = 0;
	uchar *write; //数据块的数据区的开始指针
	//设置write指针
	write = writeStartPtr + sizeof(MetaData);
	uchar *writeEnd; //数据块的结尾指针
	//设置writeEnd指针
	if (chunkID == 0)
	{
		writeEnd = writeStartPtr - sizeof(ChunkManagerMeta) + MemoryBuffer::pagesize;
	}
	else
	{
		writeEnd = writeStartPtr + MemoryBuffer::pagesize;
	}
	//设置文件指针
	MemoryMappedFile mappedIn;
	assert(mappedIn.open(tempSorted.getFile().c_str()));
	const uchar *fileRead = mappedIn.getBegin();
	const uchar *fileReadEnd = mappedIn.getEnd();
	//第一次插入
	while (fileRead < fileReadEnd)
	{
		TempFile::readIDID(fileRead, subjectID, objectID); // fileRead自动后移
		if (subjectID == 0)
		{
			continue;
		}
		lastSubjectID = subjectID;
		lastObjectID = objectID;
		len = sizeof(ID) * 2;
		//插入数据
		chunkManager[soType][predicateID]->write(write, subjectID, objectID);
		//更新metaData
		metaData->usedSpace += len;
		metaData->tripleCount++;
		break;
	}
	//继续插入
	while (fileRead < fileReadEnd)
	{
		TempFile::readIDID(fileRead, subjectID, objectID); // fileRead自动后移
		if (subjectID == 0)
		{
			continue;
		}
		if (subjectID == lastSubjectID && objectID == lastObjectID)
		{
			continue;
		}
		lastSubjectID = subjectID;
		lastObjectID = objectID;
		len = sizeof(ID) * 2;
		if (write + len > writeEnd)
		{
			assert(metaData->NextPageNo != -1);
			writeStartPtr = TempMMapBuffer::getInstance().getAddress() + (metaData->NextPageNo) * MemoryBuffer::pagesize;
			metaData = (MetaData *)writeStartPtr;
			//重置metaData
			metaData->usedSpace = sizeof(MetaData);
			metaData->tripleCount = 0;
			metaData->updateCount = 0;
			metaDataFirst->pageNo = metaData->pageNo;
			write = writeStartPtr + sizeof(MetaData);
			writeEnd = writeStartPtr + MemoryBuffer::pagesize;
		}
		//插入数据
		chunkManager[soType][predicateID]->write(write, subjectID, objectID); // write自动后移
		//更新metaData
		metaData->usedSpace += len;
		metaData->tripleCount++;
	}

	tempSorted.discard();
	sorted.discard();

	delete sortChunkTask;
	sortChunkTask = NULL;
}

//插入数据(整个数据)
//并行
Status TripleBitWorkerQuery::excuteInsertData()
{
	tripleSize = _query->tripleNodes.size();
	classifyTripleNode();

	struct timeval start, end;

	size_t needPage = 2 * (tripleSize / ((MemoryBuffer::pagesize - sizeof(MetaData)) / (3 * sizeof(ID))) + 1 + bitmap->usedPageByS);
	TempMMapBuffer::getInstance().checkSize(needPage);

	gettimeofday(&start, NULL);

	//插入数据
	for (auto &pair_triple : tripleNodeMap)
	{
		ThreadPool::getSubTranPool()[pair_triple.first % gthread]->addTask(boost::bind(&TripleBitWorkerQuery::executeInsertData, this, boost::ref(pair_triple.second)));
	}
	ThreadPool::waitSubTranThread();

	// buffer缓冲区数据持久化
	endForWork();
	ThreadPool::waitSubTranThread();

	// bitmap->print(2);

	//插入数据排序
	sortDataBase();
	ThreadPool::waitSubTranThread();

	gettimeofday(&end, NULL);
	cout << "insert number: " << tripleSize << endl;
	cout << "time: " << ((end.tv_sec - start.tv_sec) * 1000000 + end.tv_usec - start.tv_usec) / 1000000.0 << "s" << endl;

	return OK;
}

//插入一个谓词的数据
// tripleNodes = 同一个谓词的 vector<TripleNode*>
void TripleBitWorkerQuery::executeInsertData(const vector<TripleNode *> &tripleNodes)
{
	TripleBitQueryGraph::OpType operationType = TripleBitQueryGraph::INSERT_DATA;
	timeval start; //, end;

	size_t chunkID;
	ChunkTask *chunkTask = new ChunkTask[tripleNodes.size() * 2];
	int i = 0;
	gettimeofday(&start, NULL);
	for (auto &triplenode : tripleNodes)
	{
		chunkID = chunkManager[ORDERBYS][triplenode->predicateID]->getChunkIndex()->searchChunk(triplenode->subjectID, triplenode->objectID);
		chunkTask[i].setChunkTask(start, operationType, *triplenode, ORDERBYS, chunkID);
		taskEnQueue(&chunkTask[i], chunkQueue[ORDERBYS][triplenode->predicateID][chunkID]);
		i++;

		chunkID = chunkManager[ORDERBYO][triplenode->predicateID]->getChunkIndex()->searchChunk(triplenode->objectID, triplenode->subjectID);
		chunkTask[i].setChunkTask(start, operationType, *triplenode, ORDERBYO, chunkID);
		taskEnQueue(&chunkTask[i], chunkQueue[ORDERBYO][triplenode->predicateID][chunkID]);
		i++;
	}
	chunkTasksMutex.lock();
	chunkTasks.push_back(chunkTask);
	chunkTasksMutex.unlock();
}

void TripleBitWorkerQuery::taskEnQueue(ChunkTask *chunkTask, TasksQueueChunk *tasksQueue)
{
	if (tasksQueue->isEmpty())
	{
		tasksQueue->EnQueue(chunkTask);
		ThreadPool::getSubTranPool()[chunkTask->Triple.predicateID % gthread]->addTask(boost::bind(&TripleBitWorkerQuery::handleTasksQueueChunk, this, tasksQueue));
	}
	else
	{
		tasksQueue->EnQueue(chunkTask);
	}
}

void TripleBitWorkerQuery::handleTasksQueueChunk(TasksQueueChunk *tasksQueue)
{
	ChunkTask *chunkTask = NULL;
	while ((chunkTask = tasksQueue->Dequeue()) != NULL)
	{
		switch (chunkTask->operationType)
		{
		case TripleBitQueryGraph::QUERY:
			executeChunkTaskQuery(chunkTask);
			break;
		case TripleBitQueryGraph::INSERT_DATA:
			executeInsertChunkData(chunkTask);
			break;
			/*
		case TripleBitQueryGraph::DELETE_DATA:
			excuteDeleteChunkData_lab(chunkTask);
			break;
		case TripleBitQueryGraph::DELETE_CLAUSE:
			executeChunkTaskDeleteClause(chunkTask);
			break;
		case TripleBitQueryGraph::UPDATE:
			break;
			*/
		default:
			break;
		}
	}
}
//插入一条数据
void TripleBitWorkerQuery::executeInsertChunkData(ChunkTask *chunkTask)
{
	ID predicateID = chunkTask->Triple.predicateID;
	ID chunkID = chunkTask->chunkID;
	bool opSO = chunkTask->opSO;

	chunkTempBuffer[opSO][predicateID][chunkID]->insertTriple(chunkTask->Triple.subjectID, chunkTask->Triple.objectID);
	if (chunkTempBuffer[opSO][predicateID][chunkID]->isFull())
	{
		// combine the data in tempbuffer into the source data
		// cout << "TripleBitWorkerQuery::executeInsertChunkData" << endl;
		combineTempBufferToSource(chunkTempBuffer[opSO][predicateID][chunkID]);
	}
}

//插入数据
void TripleBitWorkerQuery::combineTempBufferToSource(TempBuffer *buffer)
{
	assert(buffer != NULL);
	ID predicateID = buffer->getPredicateID();
	ID chunkID = buffer->getChunkID();
	bool soType = buffer->getSOType();
	buffer->sort(soType);
	buffer->uniqe();
	uchar *currentPtrChunk, *endPtrChunk, *startPtrChunk; //数据块指针
	//初始化块指针
	startPtrChunk = const_cast<uchar *>(chunkStartPtr[soType][predicateID][chunkID]);
	if (chunkID == 0)
	{
		endPtrChunk = startPtrChunk - sizeof(ChunkManagerMeta) + MemoryBuffer::pagesize;
	}
	else
	{
		endPtrChunk = startPtrChunk + MemoryBuffer::pagesize;
	}
	MetaData *metaData = (MetaData *)startPtrChunk;
	MetaData *metaDataFirst = metaData;
	currentPtrChunk = startPtrChunk + metaData->usedSpace;

	//待插入数据指针
	ChunkTriple *readBuffer = buffer->getBuffer(), *endBuffer = buffer->getEnd();
	//插入三元组长度
	uint len;
	//如果已经有下一块，直接定位到最后一块
	if (metaData->pageNo != -1)
	{
		startPtrChunk = reinterpret_cast<uchar *>(TempMMapBuffer::getInstance().getAddress()) + (metaData->pageNo) * MemoryBuffer::pagesize;
		endPtrChunk = startPtrChunk + MemoryBuffer::pagesize;
		metaData = (MetaData *)startPtrChunk;
		currentPtrChunk = startPtrChunk + metaData->usedSpace;
	}
	//在最后一块插入数据
	while (readBuffer < endBuffer)
	{
		len = sizeof(ID) * 2;
		//块满申请新块
		if (currentPtrChunk + len > endPtrChunk)
		{
			size_t pageNo;
			bool newchunk = false;
			if (metaData->NextPageNo != -1)
			{
				pageNo = metaData->NextPageNo;
				newchunk = false;
			}
			else
			{
				TempMMapBuffer::getInstance().getPage(pageNo);
				newchunk = true;
			}
			metaDataFirst->pageNo = pageNo;
			startPtrChunk = reinterpret_cast<uchar *>(TempMMapBuffer::getInstance().getAddress()) + (pageNo)*MemoryBuffer::pagesize;
			endPtrChunk = startPtrChunk + MemoryBuffer::pagesize;
			if (newchunk)
			{
				//更新上一个块的元信息
				metaData->NextPageNo = pageNo;
				//初始化新申请块的元信息
				metaData = (MetaData *)startPtrChunk;
				metaData->pageNo = pageNo;
				metaData->NextPageNo = -1;
				metaData->usedSpace = sizeof(MetaData);
				metaData->tripleCount = 0;
				metaData->updateCount = 0;
				currentPtrChunk = startPtrChunk + sizeof(MetaData);
			}
			else
			{
				//重置数据块的元信息
				metaData = (MetaData *)startPtrChunk;
				metaData->usedSpace = sizeof(MetaData);
				metaData->tripleCount = 0;
				metaData->updateCount = 0;
				currentPtrChunk = startPtrChunk + sizeof(MetaData);
			}
		}
		//插入数据
		chunkManager[soType][predicateID]->write(currentPtrChunk, readBuffer->subjectID, readBuffer->objectID);
		//更新metaData
		metaData->usedSpace += len;
		metaData->tripleCount++;
		metaData->updateCount++;
		//更新循环变量
		readBuffer++;
		//更新原始块信息
		metaDataFirst->updateCount = 1;
	}
	//清空buffer
	buffer->clear();
}

/*
//删除数据
Status TripleBitWorkerQuery::excuteDeleteData()
{
	tripleSize = _query->tripleNodes.size();
	classifyTripleNode();

	timeval start, end;
	TripleBitWorkerQuery::timeCount = _query->tripleNodes.size() * 2;
	gettimeofday(&start, NULL);
	TripleBitWorkerQuery::tripleCount = 0;
	for (auto &pair_triple : tripleNodeMap){
		ThreadPool::getSubTranPool()[pair_triple.first % gthread]->addTask(boost::bind(&TripleBitWorkerQuery::executeDeleteData, this, boost::ref(pair_triple.second)));
	}
	ThreadPool::waitSubTranThread();

	gettimeofday(&end, NULL);
	cout << "delete triples: " << tripleSize << "\ttime: " << ((end.tv_sec - start.tv_sec) * 1000000 + end.tv_usec - start.tv_usec) / 1000000.0 << "s" << endl;

	return OK;
}


void TripleBitWorkerQuery::executeDeleteData(const set<TripleNode *> &tripleNodes)
{
	TripleBitQueryGraph::OpType operationType = TripleBitQueryGraph::DELETE_DATA;
	timeval start;//, end;
	size_t chunkID;
	ChunkTask *chunkTask = new ChunkTask[tripleNodes.size() * 2];
	int i = 0;
	gettimeofday(&start, NULL);
	for (auto &triplenode : tripleNodes){
		chunkID = chunkManager[ORDERBYS][triplenode->predicateID]->getChunkIndex()->searchChunk(triplenode->subjectID, triplenode->object);
		chunkTask[i].setChunkTask(start, operationType, *triplenode, ORDERBYS, chunkID);
		taskEnQueue(&chunkTask[i++], chunkQueue[ORDERBYS][triplenode->predicateID][chunkID]);

		chunkID = chunkManager[ORDERBYO][triplenode->predicateID]->getChunkIndex()->searchChunk(triplenode->object,  triplenode->subjectID);
		chunkTask[i].setChunkTask(start, operationType, *triplenode, ORDERBYO, chunkID);
		taskEnQueue(&chunkTask[i++], chunkQueue[ORDERBYO][triplenode->predicateID][chunkID]);
	}
}
*/
/*
bool TripleBitWorkerQuery::excuteDeleteChunkData_lab(ChunkTask *chunkTask)
{
	ID subjectID = chunkTask->Triple.subjectID;
	ID tempSubjectID;
	ID predicateID = chunkTask->Triple.predicateID;
	ID objectID = chunkTask->Triple.objectID;
	ID tempObjectID;
	bool soType = chunkTask->opSO;
	ID chunkID = chunkTask->chunkID;

	const uchar *reader, *limit, *currentStartPtr = chunkStartPtr[soType][predicateID][chunkID], *tempReader;
	uchar *temp;
	MetaData *metaData = (MetaData *) currentStartPtr;
	size_t lastPageNo = metaData->pageNo;

	reader = currentStartPtr + sizeof(MetaData);
	limit = currentStartPtr + metaData->usedSpace;


	if (soType == ORDERBYS)
	{
		while (reader < limit)
		{
			temp = const_cast<uchar *>(reader);
			chunkManager[soType][predicateID]->read(reader, tempSubjectID, tempObjectID);
			if (tempSubjectID && tempSubjectID == subjectID && tempObjectID == objectID)
			{
				chunkManager[soType][predicateID]->deleteTriple(temp);
				metaData->tripleCount--;
				metaData->updateCount++;
				TripleBitWorkerQuery::tripleCount++;
				break;
			}
		}
		while (metaData->NextPageNo)
		{
			currentStartPtr = reinterpret_cast<uchar*>(TempMMapBuffer::getInstance().getAddress()) + MemoryBuffer::pagesize * (metaData->NextPageNo);
			metaData = (MetaData *) currentStartPtr;
			if (metaData->pageNo > lastPageNo)
			{
				break;
			}
			reader = currentStartPtr + sizeof(MetaData);
			limit = currentStartPtr + metaData->usedSpace;
			while (reader < limit)
			{
				temp = const_cast<uchar*>(reader);
				chunkManager[soType][predicateID]->read(reader, tempSubjectID, tempObjectID);
				if (tempSubjectID && tempSubjectID == subjectID && tempObjectID == objectID)
				{
					chunkManager[soType][predicateID]->deleteTriple(temp);
					metaData->tripleCount--;
					metaData->updateCount++;
					TripleBitWorkerQuery::tripleCount++;
					break;
				}
			}
		}
	}
	else if (soType == ORDERBYO)
	{
		if (subjectID == 0)   //删除所有记录，主要用于基于模式删除
		{
			while (reader < limit)
			{
				tempReader = chunkManager[soType][predicateID]->readXY(reader, tempSubjectID, tempObject, tempObjType);
				if (tempSubjectID == 0)
				{
					reader = tempReader;
					continue;
				}
				reader = (const uchar *) chunkManager[soType][predicateID]->deleteTriple(const_cast<uchar *>(reader));
				Chunk::tripleCountDecrease(metaData);
				metaData->updateCount++;
				TripleBitWorkerQuery::tripleCount++;
				// tripleBitRepo->getSTInfoByOP()->put_sync(tempObject, tempObjType, predicateID, -1);
			}
			while (metaData->NextPageNo)
			{
				currentStartPtr = reinterpret_cast<uchar *>(TempMMapBuffer::getInstance().getAddress())
								  + MemoryBuffer::pagesize * metaData->NextPageNo;
				metaData = (MetaData *) currentStartPtr;
				if (metaData->pageNo > lastPageNo)
				{
					break;
				}
				reader = currentStartPtr + sizeof(MetaData);
				limit = currentStartPtr + metaData->usedSpace;
				while (reader < limit)
				{
					tempReader = chunkManager[soType][predicateID]->readXY(reader, tempSubjectID, tempObject, tempObjType);
					if (tempSubjectID == 0)
					{
						reader = tempReader;
						continue;
					}
					reader = (const uchar *) chunkManager[soType][predicateID]->deleteTriple(const_cast<uchar *>(reader));
					Chunk::tripleCountDecrease(metaData);
					metaData->updateCount++;
					TripleBitWorkerQuery::tripleCount++;
					// tripleBitRepo->getSTInfoByOP()->put_sync(tempObject, tempObjType, predicateID, -1);
				}
			}
		}
		else
		{
			while (reader < limit)
			{
				temp = const_cast<uchar *>(reader);
				reader = chunkManager[soType][predicateID]->readXY(reader, tempSubjectID, tempObject, tempObjType);
				if (tempSubjectID && compare_double(tempObject, object) == 0 && tempObjType == objType && tempSubjectID == subjectID)
				{
					temp = chunkManager[soType][predicateID]->deleteTriple(temp, objType);
					Chunk::tripleCountDecrease(metaData);
					metaData->updateCount++;
					TripleBitWorkerQuery::tripleCount++;
					// tripleBitRepo->getSTInfoByOP()->put_sync(tempObject, tempObjType, predicateID, -1);
				}
			}
			while (metaData->NextPageNo)
			{
				currentStartPtr = reinterpret_cast<uchar *>(TempMMapBuffer::getInstance().getAddress())
								  + MemoryBuffer::pagesize * (metaData->NextPageNo - 1);
				metaData = (MetaData *) currentStartPtr;
				if (metaData->pageNo > lastPageNo)
				{
					break;
				}
				reader = currentStartPtr + sizeof(MetaData);
				metaData->updateCount++;
				limit = currentStartPtr + metaData->usedSpace;
				while (reader < limit)
				{
					temp = const_cast<uchar *>(reader);
					reader = chunkManager[soType][predicateID]->readXY(reader, tempSubjectID, tempObject, tempObjType);
					if (tempSubjectID && compare_double(tempObject, object) == 0 && tempObjType == objType && tempSubjectID == subjectID)
					{
						temp = chunkManager[soType][predicateID]->deleteTriple(temp, objType);
						Chunk::tripleCountDecrease(metaData);
						TripleBitWorkerQuery::tripleCount++;
						// tripleBitRepo->getSTInfoByOP()->put_sync(tempObject, tempObjType, predicateID, -1);
					}
				}
			}
		}
	}
	// TripleBitWorkerQuery::timeCount--;
	return true;
}
*/

/*
bool TripleBitWorkerQuery::excuteDeleteChunkData(ChunkTask *chunkTask)
{
	ID subjectID = chunkTask->Triple.subjectID;
	ID predicateID = chunkTask->Triple.predicateID;
	ID tempSubjectID;
	double object = chunkTask->Triple.object;
	double tempObject;
	char objType = chunkTask->Triple.objType;
	char tempObjType;
	bool soType = chunkTask->opSO;
	ID chunkID = chunkTask->chunkID;

	const uchar *reader, *limit, *currentStartPtr = chunkStartPtr[soType][predicateID][chunkID], *tempReader;
	uchar *temp;
	MetaData *metaData = (MetaData *) currentStartPtr;
	size_t lastPageNo = metaData->pageNo;

	reader = currentStartPtr + sizeof(MetaData);
	limit = currentStartPtr + metaData->usedSpace;
	if (soType == ORDERBYS)
	{
		while (reader < limit)
		{
			temp = const_cast<uchar *>(reader);
			reader = chunkManager[soType][predicateID]->readXY(reader, tempSubjectID, tempObject, tempObjType);
			if (tempSubjectID && tempSubjectID == subjectID && tempObject == object && tempObjType == objType)
			{
				temp = chunkManager[soType][predicateID]->deleteTriple(temp, objType);
				Chunk::tripleCountDecrease(metaData);
				metaData->updateCount++;
				TripleBitWorkerQuery::tripleCount++;
				tripleBitRepo->getSTInfoBySP()->put_sync(tempSubjectID, STRING, predicateID, -1);
			}
		}
		while (metaData->NextPageNo)
		{
			currentStartPtr = reinterpret_cast<uchar *>(TempMMapBuffer::getInstance().getAddress())
							  + MemoryBuffer::pagesize * (metaData->NextPageNo - 1);
			metaData = (MetaData *) currentStartPtr;
			if (metaData->pageNo > lastPageNo)
			{
				break;
			}
			reader = currentStartPtr + sizeof(MetaData);
			limit = currentStartPtr + metaData->usedSpace;
			while (reader < limit)
			{
				temp = const_cast<uchar *>(reader);
				reader = chunkManager[soType][predicateID]->readXY(reader, tempSubjectID, tempObject, tempObjType);
				if (tempSubjectID && tempSubjectID == subjectID && abs(tempObject - object) <= ELAPSED && tempObjType == objType)
				{
					temp = chunkManager[soType][predicateID]->deleteTriple(temp, objType);
					Chunk::tripleCountDecrease(metaData);
					metaData->updateCount++;
					TripleBitWorkerQuery::tripleCount++;
					tripleBitRepo->getSTInfoBySP()->put_sync(tempSubjectID, STRING, predicateID, -1);
				}
			}
		}
	}
	else if (soType == ORDERBYO)
	{
		if (subjectID == 0)   //删除所有记录，主要用于基于模式删除
		{
			while (reader < limit)
			{
				tempReader = chunkManager[soType][predicateID]->readXY(reader, tempSubjectID, tempObject, tempObjType);
				if (tempSubjectID == 0)
				{
					reader = tempReader;
					continue;
				}
				reader = (const uchar *) chunkManager[soType][predicateID]->deleteTriple(const_cast<uchar *>(reader));
				Chunk::tripleCountDecrease(metaData);
				metaData->updateCount++;
				TripleBitWorkerQuery::tripleCount++;
				tripleBitRepo->getSTInfoByOP()->put_sync(tempObject, tempObjType, predicateID, -1);
			}
			while (metaData->NextPageNo)
			{
				currentStartPtr = reinterpret_cast<uchar *>(TempMMapBuffer::getInstance().getAddress())
								  + MemoryBuffer::pagesize * metaData->NextPageNo;
				metaData = (MetaData *) currentStartPtr;
				if (metaData->pageNo > lastPageNo)
				{
					break;
				}
				reader = currentStartPtr + sizeof(MetaData);
				limit = currentStartPtr + metaData->usedSpace;
				while (reader < limit)
				{
					tempReader = chunkManager[soType][predicateID]->readXY(reader, tempSubjectID, tempObject, tempObjType);
					if (tempSubjectID == 0)
					{
						reader = tempReader;
						continue;
					}
					reader = (const uchar *) chunkManager[soType][predicateID]->deleteTriple(const_cast<uchar *>(reader));
					Chunk::tripleCountDecrease(metaData);
					metaData->updateCount++;
					TripleBitWorkerQuery::tripleCount++;
					tripleBitRepo->getSTInfoByOP()->put_sync(tempObject, tempObjType, predicateID, -1);
				}
			}
		}
		else
		{
			while (reader < limit)
			{
				temp = const_cast<uchar *>(reader);
				reader = chunkManager[soType][predicateID]->readXY(reader, tempSubjectID, tempObject, tempObjType);
				if (tempSubjectID && compare_double(tempObject, object) == 0 && tempObjType == objType && tempSubjectID == subjectID)
				{
					temp = chunkManager[soType][predicateID]->deleteTriple(temp, objType);
					Chunk::tripleCountDecrease(metaData);
					metaData->updateCount++;
					TripleBitWorkerQuery::tripleCount++;
					tripleBitRepo->getSTInfoByOP()->put_sync(tempObject, tempObjType, predicateID, -1);
				}
			}
			while (metaData->NextPageNo)
			{
				currentStartPtr = reinterpret_cast<uchar *>(TempMMapBuffer::getInstance().getAddress())
								  + MemoryBuffer::pagesize * (metaData->NextPageNo - 1);
				metaData = (MetaData *) currentStartPtr;
				if (metaData->pageNo > lastPageNo)
				{
					break;
				}
				reader = currentStartPtr + sizeof(MetaData);
				metaData->updateCount++;
				limit = currentStartPtr + metaData->usedSpace;
				while (reader < limit)
				{
					temp = const_cast<uchar *>(reader);
					reader = chunkManager[soType][predicateID]->readXY(reader, tempSubjectID, tempObject, tempObjType);
					if (tempSubjectID && compare_double(tempObject, object) == 0 && tempObjType == objType && tempSubjectID == subjectID)
					{
						temp = chunkManager[soType][predicateID]->deleteTriple(temp, objType);
						Chunk::tripleCountDecrease(metaData);
						TripleBitWorkerQuery::tripleCount++;
						tripleBitRepo->getSTInfoByOP()->put_sync(tempObject, tempObjType, predicateID, -1);
					}
				}
			}
		}
	}
	TripleBitWorkerQuery::timeCount--;
	return true;
}
*/

/*
Status TripleBitWorkerQuery::excuteDeleteClause()
{
	vector<TripleNode>::iterator iter = _query->tripleNodes.begin();
	if (iter->constSubject)
	{
		cout << "subjectID: " << iter->subjectID << "\t";
	}
	if (iter->constPredicate)
	{
		cout << "predicateID: " << iter->predicateID << "\t";
	}
	if (iter->constObject)
	{
		cout << "object: ";
		cout.setf(ios::fixed, ios::floatfield);
		cout << iter->object;
		cout.unsetf(ios::floatfield);
		cout << "\t" << (int)(iter->objType);
	}
	cout << endl;
	TripleBitQueryGraph::OpType operationType = TripleBitQueryGraph::DELETE_CLAUSE;
	timeval start;
	gettimeofday(&start, NULL);
	size_t rank = 0;
	//excuteDeleteClause 无法确认chunkTask个数，以subTran和chunkTask计数
	TripleBitWorkerQuery::tripleCount = 0;
	TripleBitWorkerQuery::timeCount = 0;
	if (iter->constPredicate)
	{
		//predicate已知
		//ID partitionID = iter->predicateID;
		TripleBitWorkerQuery::timeCount++;
		SubTrans *subTrans = new SubTrans(start, workerID, 0, 0, operationType, *iter);
		ThreadPool::getSubTranPool()[(++rank) % gthread]->addTask(
			boost::bind(&TripleBitWorkerQuery::excuteSubTransDeleteClause, this, subTrans));
	}
	else
	{
		//predicate未知
		if (!iter->constSubject && !iter->constObject)
		{
			//subject、object未知
			for (size_t i = 1; i <= tripleBitRepo->getPartitionNum(); i++)
			{
				iter->predicateID = i;
				TripleBitWorkerQuery::timeCount++;
				SubTrans *subTrans = new SubTrans(*transactionTime, workerID, 0, 0, operationType, *iter);
				ThreadPool::getSubTranPool()[(++rank) % gthread]->addTask(
					boost::bind(&TripleBitWorkerQuery::excuteSubTransDeleteClause, this, subTrans));
			}
		}
		else if (iter->constSubject && iter->constObject)
		{
			longlong subjectCounts = 0, objectCounts = 0;
			tripleBitRepo->getSTInfoBySP()->get(iter->subjectID, STRING, subjectCounts);
			tripleBitRepo->getSTInfoByOP()->get(iter->object, iter->objType, objectCounts);
			//			tripleBitRepo->getSpStatisBuffer()->getStatisBySO(iter->subjectID, subjectCounts, STRING);
			//			tripleBitRepo->getOpStatisBuffer()->getStatisBySO(iter->object, objectCounts, iter->objType);
			if (subjectCounts == 0 || objectCounts == 0)
			{
				return OK;
			}
			//若S的triple数少，根据sp匹配o进行删除

			if (subjectCounts <= objectCounts)
			{
				vector<ID> pids;
				tripleBitRepo->getSTInfoBySP()->get(iter->subjectID, STRING, pids);
				//				tripleBitRepo->getSpStatisBuffer()->findAllPredicateBySO(iter->subjectID, pids, STRING);
				for (vector<ID>::iterator it = pids.begin(); it != pids.end(); it++)
				{
					iter->predicateID = *it;
					TripleBitWorkerQuery::timeCount++;
					SubTrans *subTrans = new SubTrans(*transactionTime, workerID, 0, 0, operationType, *iter);
					ThreadPool::getSubTranPool()[(++rank) % gthread]->addTask(
						boost::bind(&TripleBitWorkerQuery::excuteSubTransDeleteClause, this, subTrans));
				}
			}
			else
			{
				vector<ID> pids;
				tripleBitRepo->getSTInfoByOP()->get(iter->object, iter->objType, pids);
				//				tripleBitRepo->getOpStatisBuffer()->findAllPredicateBySO(iter->object, pids, iter->objType);
				for (vector<ID>::iterator it = pids.begin(); it != pids.end(); it++)
				{
					iter->predicateID = *it;
					TripleBitWorkerQuery::timeCount++;
					SubTrans *subTrans = new SubTrans(*transactionTime, workerID, 0, 0, operationType, *iter);
					ThreadPool::getSubTranPool()[(++rank) % gthread]->addTask(
						boost::bind(&TripleBitWorkerQuery::excuteSubTransDeleteClause, this, subTrans));
				}
			}
		}
		else if (iter->constSubject)
		{
			//subject已知
			vector<ID> pids;
			tripleBitRepo->getSTInfoBySP()->get(iter->subjectID, STRING, pids);
			//			tripleBitRepo->getSpStatisBuffer()->findAllPredicateBySO(iter->subjectID, pids, STRING);
			for (vector<ID>::iterator it = pids.begin(); it != pids.end(); it++)
			{
				iter->predicateID = *it;
				TripleBitWorkerQuery::timeCount++;
				SubTrans *subTrans = new SubTrans(*transactionTime, workerID, 0, 0, operationType, *iter);
				ThreadPool::getSubTranPool()[(++rank) % gthread]->addTask(
					boost::bind(&TripleBitWorkerQuery::excuteSubTransDeleteClause, this, subTrans));
			}
		}
		else if (iter->constObject)
		{
			//object已知
			vector<ID> pids;
			tripleBitRepo->getSTInfoByOP()->get(iter->object, iter->objType, pids);
			//			tripleBitRepo->getOpStatisBuffer()->findAllPredicateBySO(iter->object, pids, iter->objType);
			for (vector<ID>::iterator it = pids.begin(); it != pids.end(); it++)
			{
				iter->predicateID = *it;
				TripleBitWorkerQuery::timeCount++;
				SubTrans *subTrans = new SubTrans(*transactionTime, workerID, 0, 0, operationType, *iter);
				ThreadPool::getSubTranPool()[(++rank) % gthread]->addTask(
					boost::bind(&TripleBitWorkerQuery::excuteSubTransDeleteClause, this, subTrans));
			}
		}
	}
	while (TripleBitWorkerQuery::timeCount != 0)
	{
	}
	cout << "delete triples: " << TripleBitWorkerQuery::tripleCount / 2 << endl;
	return OK;
}
*/

/*
bool TripleBitWorkerQuery::excuteSubTransDeleteClause(SubTrans *subTran)
{
	ID subjectID = subTran->triple.constSubject ? subTran->triple.subjectID : 0;
	ID predicateID = subTran->triple.predicateID;
	double object = subTran->triple.constObject ? subTran->triple.object : 0;
	char objType = subTran->triple.constObject ? subTran->triple.objType : NONE;
	size_t chunkCount = 0, chunkIDMin = 0, chunkIDMax = 0;

	if (!subTran->triple.constSubject && !subTran->triple.constObject)
	{
		chunkIDMax = chunkManager[ORDERBYS][predicateID]->getChunkNumber() - 1;
		chunkCount = chunkIDMax - chunkIDMin + 1;
		size_t chunkCountO = 0, chunkIDMinO = 0, chunkIDMaxO = 0;
		chunkIDMaxO = chunkManager[ORDERBYO][predicateID]->getChunkNumber() - 1;
		chunkCountO = chunkIDMaxO - chunkIDMinO + 1;
		std::tr1::shared_ptr<SubTaskPackageForDelete> taskPackage(
			new SubTaskPackageForDelete(chunkCount, subTran->operationType, subjectID, subTran->triple.constSubject,
										subTran->triple.constObject));
		if (chunkCount != 0)
		{
			for (size_t offsetID = chunkIDMin; offsetID <= chunkIDMax; offsetID++)
			{
				TripleBitWorkerQuery::timeCount++;
				ChunkTask *chunkTask = new ChunkTask(subTran->transTime, subTran->operationType, subjectID, predicateID,
													 object, objType, subTran->triple.scanOperation, taskPackage, ORDERBYS, offsetID);
				ThreadPool::getChunkPool()[(predicateID + offsetID) % cthread]->addTask(
					boost::bind(&TripleBitWorkerQuery::executeChunkTaskDeleteClause, this, chunkTask));
			}
		}
		std::tr1::shared_ptr<SubTaskPackageForDelete> taskPackageO(
			new SubTaskPackageForDelete(chunkCountO, subTran->operationType, object, objType,
										subTran->triple.constSubject, subTran->triple.constObject));
		if (chunkCountO != 0)
		{
			for (size_t offsetID = chunkIDMinO; offsetID <= chunkIDMaxO; offsetID++)
			{
				TripleBitWorkerQuery::timeCount++;
				ChunkTask *chunkTask = new ChunkTask(subTran->transTime, subTran->operationType, subjectID, predicateID,
													 object, objType, subTran->triple.scanOperation, taskPackageO, ORDERBYO, offsetID);
				ThreadPool::getChunkPool()[(predicateID + offsetID) % cthread]->addTask(
					boost::bind(&TripleBitWorkerQuery::executeChunkTaskDeleteClause, this, chunkTask));
			}
		}
	}
	else if (subTran->triple.constSubject)
	{
		//subject已知、object无论是否已知，均先处理subject的删除
		if (chunkManager[ORDERBYS][predicateID]->getChunkIndex()->searchChunk(subjectID, DBL_MIN, chunkIDMin))
		{
			chunkIDMax = chunkManager[ORDERBYS][predicateID]->getChunkIndex()->searchChunk(subjectID, DBL_MAX);
			assert(chunkIDMax >= chunkIDMin);
		}
		else
		{
			delete subTran;
			subTran = NULL;
			TripleBitWorkerQuery::timeCount--;
			return true;
		}
		chunkCount = chunkIDMax - chunkIDMin + 1;
		if (chunkCount != 0)
		{
			std::tr1::shared_ptr<SubTaskPackageForDelete> taskPackage(
				new SubTaskPackageForDelete(chunkCount, subTran->operationType, subjectID,
											subTran->triple.constSubject, subTran->triple.constObject));
			for (size_t offsetID = chunkIDMin; offsetID <= chunkIDMax; offsetID++)
			{
				TripleBitWorkerQuery::timeCount++;
				ChunkTask *chunkTask = new ChunkTask(subTran->transTime, subTran->operationType, subjectID, predicateID,
													 object, objType, subTran->triple.scanOperation, taskPackage, ORDERBYS, offsetID);
				ThreadPool::getChunkPool()[(predicateID + offsetID) % cthread]->addTask(
					boost::bind(&TripleBitWorkerQuery::executeChunkTaskDeleteClause, this, chunkTask));
			}
		}
	}
	else if (!subTran->triple.constSubject && subTran->triple.constObject)
	{
		if (chunkManager[ORDERBYO][predicateID]->getChunkIndex()->searchChunk(object, ID_MIN, chunkIDMin))
		{
			chunkIDMax = chunkManager[ORDERBYO][predicateID]->getChunkIndex()->searchChunk(object, ID_MAX);
			assert(chunkIDMax >= chunkIDMin);
			chunkCount = chunkIDMax - chunkIDMin + 1;
		}
		else
		{
			delete subTran;
			subTran = NULL;
			TripleBitWorkerQuery::timeCount--;
			return true;
		}
		if (chunkCount != 0)
		{
			std::tr1::shared_ptr<SubTaskPackageForDelete> taskPackage(
				new SubTaskPackageForDelete(chunkCount, subTran->operationType, object, objType,
											subTran->triple.constSubject, subTran->triple.constObject));
			cout << predicateID << "\t" << chunkIDMin << "\t" << chunkIDMax << endl;
			for (size_t offsetID = chunkIDMin; offsetID <= chunkIDMax; offsetID++)
			{
				TripleBitWorkerQuery::timeCount++;
				ChunkTask *chunkTask = new ChunkTask(subTran->transTime, subTran->operationType, subjectID, predicateID,
													 object, objType, subTran->triple.scanOperation, taskPackage, ORDERBYO, offsetID);
				ThreadPool::getChunkPool()[(predicateID + offsetID) % cthread]->addTask(
					boost::bind(&TripleBitWorkerQuery::executeChunkTaskDeleteClause, this, chunkTask));
			}
		}
	}

	delete subTran;
	subTran = NULL;
	TripleBitWorkerQuery::timeCount--;
	return true;
}
*/

/*
bool TripleBitWorkerQuery::executeChunkTaskDeleteClause(ChunkTask *chunkTask)
{
	ID subjectID = chunkTask->Triple.subjectID;
	ID predicateID = chunkTask->Triple.predicateID;
	double object = chunkTask->Triple.object;
	char objType = chunkTask->Triple.objType;
	bool soType = chunkTask->opSO;
	ID chunkID = chunkTask->chunkID;
	ID tempSubjectID;
	double tempObject;
	char tempObjType;
	const uchar *reader, *limit, *currentStartPtr = chunkStartPtr[soType][predicateID][chunkID];
	uchar *temp;
	MetaData *metaData = (MetaData *) currentStartPtr;
	size_t lastPageNo = metaData->pageNo;
	reader = currentStartPtr + sizeof(MetaData);
	limit = currentStartPtr + metaData->usedSpace;
	int size = 0;
	if (!chunkTask->taskPackageForDelete->constSubject && !chunkTask->taskPackageForDelete->constObject)   //subject未知，即为已知predicate，删除subject、object
	{
		while (reader < limit)
		{
			temp = const_cast<uchar *>(reader);
			reader = chunkManager[soType][predicateID]->readXY(reader, tempSubjectID, tempObject, tempObjType);
			if (tempSubjectID != 0)
			{
				temp = chunkManager[soType][predicateID]->deleteTriple(temp, tempObjType);
				Chunk::tripleCountDecrease(metaData);
				metaData->updateCount++;
				TripleBitWorkerQuery::tripleCount++;
				if (soType == ORDERBYS)
				{
					tripleBitRepo->getSTInfoBySP()->put_sync(tempSubjectID, STRING, predicateID, -1);
				}
				else
				{
					tripleBitRepo->getSTInfoByOP()->put_sync(tempObject, tempObjType, predicateID, -1);
				}
			}
		}
		while (metaData->NextPageNo)
		{
			currentStartPtr = reinterpret_cast<uchar *>(TempMMapBuffer::getInstance().getAddress())
							  + (metaData->NextPageNo - 1) * MemoryBuffer::pagesize;
			metaData = (MetaData *) currentStartPtr;
			if (metaData->pageNo > lastPageNo)
			{
				break;
			}
			reader = currentStartPtr + sizeof(MetaData);
			limit = currentStartPtr + metaData->usedSpace;
			while (reader < limit)
			{
				temp = const_cast<uchar *>(reader);
				reader = chunkManager[soType][predicateID]->readXY(reader, tempSubjectID, tempObject, tempObjType);
				if (tempSubjectID != 0)
				{
					temp = chunkManager[soType][predicateID]->deleteTriple(temp, tempObjType);
					Chunk::tripleCountDecrease(metaData);
					metaData->updateCount++;
					TripleBitWorkerQuery::tripleCount++;
					if (soType == ORDERBYS)
					{
						tripleBitRepo->getSTInfoBySP()->put_sync(tempSubjectID, STRING, predicateID, -1);
					}
					else
					{
						tripleBitRepo->getSTInfoByOP()->put_sync(tempObject, tempObjType, predicateID, -1);
					}
				}
			}
		}
	}
	else
	{
		MidResultBuffer *midResultBuffer;
		if (soType == ORDERBYS)
		{
			midResultBuffer = new MidResultBuffer(MidResultBuffer::OBJECT);
		}
		else if (soType == ORDERBYO)
		{
			midResultBuffer = new MidResultBuffer(MidResultBuffer::SIGNALID);
		}
		while (reader < limit)
		{
			temp = const_cast<uchar *>(reader);
			reader = chunkManager[soType][predicateID]->readXY(reader, tempSubjectID, tempObject, tempObjType);
			if (soType == ORDERBYS && tempSubjectID && tempSubjectID == subjectID)
			{
				if (chunkTask->taskPackageForDelete->constObject && compare_double(tempObject, object) == 0
						&& tempObjType == objType)
				{
					midResultBuffer->insertObject(tempObject, tempObjType);
					temp = chunkManager[soType][predicateID]->deleteTriple(temp, tempObjType);
					Chunk::tripleCountDecrease(metaData);
					metaData->updateCount++;
					TripleBitWorkerQuery::tripleCount++;
					tripleBitRepo->getSTInfoBySP()->put_sync(tempSubjectID, STRING, predicateID, -1);
				}
				else if (!chunkTask->taskPackageForDelete->constObject)
				{
					midResultBuffer->insertObject(tempObject, tempObjType);
					temp = chunkManager[soType][predicateID]->deleteTriple(temp, tempObjType);
					Chunk::tripleCountDecrease(metaData);
					metaData->updateCount++;
					TripleBitWorkerQuery::tripleCount++;
					tripleBitRepo->getSTInfoBySP()->put_sync(tempSubjectID, STRING, predicateID, -1);
				}

			}
			else if (soType == ORDERBYO && tempSubjectID && compare_double(tempObject, object) == 0
					 && tempObjType == objType)
			{
				midResultBuffer->insertSIGNALID(tempSubjectID);
				temp = chunkManager[soType][predicateID]->deleteTriple(temp, tempObjType);
				size++;
				Chunk::tripleCountDecrease(metaData);
				metaData->updateCount++;
				TripleBitWorkerQuery::tripleCount++;
				tripleBitRepo->getSTInfoByOP()->put_sync(tempObject, tempObjType, predicateID, -1);
			}
		}
		while (metaData->NextPageNo)
		{
			currentStartPtr = reinterpret_cast<uchar *>(TempMMapBuffer::getInstance().getAddress())
							  + metaData->NextPageNo * MemoryBuffer::pagesize;
			metaData = (MetaData *) currentStartPtr;
			if (metaData->pageNo > lastPageNo)
			{
				break;
			}
			reader = currentStartPtr + sizeof(MetaData);
			limit = currentStartPtr + metaData->usedSpace;
			while (reader < limit)
			{
				temp = const_cast<uchar *>(reader);
				reader = chunkManager[soType][predicateID]->readXY(reader, tempSubjectID, tempObject, tempObjType);
				if (soType == ORDERBYS && tempSubjectID && tempSubjectID == subjectID)
				{
					if (chunkTask->taskPackageForDelete->constObject && compare_double(tempObject, object) == 0
							&& tempObjType == objType)
					{
						midResultBuffer->insertObject(tempObject, tempObjType);
						temp = chunkManager[soType][predicateID]->deleteTriple(temp, tempObjType);
						Chunk::tripleCountDecrease(metaData);
						metaData->updateCount++;
						TripleBitWorkerQuery::tripleCount++;
						tripleBitRepo->getSTInfoBySP()->put_sync(tempSubjectID, STRING, predicateID, -1);
					}
					else if (!chunkTask->taskPackageForDelete->constObject)
					{
						midResultBuffer->insertObject(tempObject, tempObjType);
						temp = chunkManager[soType][predicateID]->deleteTriple(temp, tempObjType);
						Chunk::tripleCountDecrease(metaData);
						metaData->updateCount++;
						TripleBitWorkerQuery::tripleCount++;
						tripleBitRepo->getSTInfoBySP()->put_sync(tempSubjectID, STRING, predicateID, -1);
					}
				}
				else if (soType == ORDERBYO && tempSubjectID && compare_double(tempObject, object) == 0
						 && tempObjType == objType)
				{
					midResultBuffer->insertSIGNALID(tempSubjectID);
					temp = chunkManager[soType][predicateID]->deleteTriple(temp, tempObjType);
					size++;
					Chunk::tripleCountDecrease(metaData);
					metaData->updateCount++;
					TripleBitWorkerQuery::tripleCount++;
					tripleBitRepo->getSTInfoByOP()->put_sync(tempObject, tempObjType, predicateID, -1);
				}
			}
		}
		if (midResultBuffer->getUsedSize() > 0)
		{
			if (chunkTask->taskPackageForDelete->completeSubTask(chunkID, midResultBuffer))
			{
				MidResultBuffer *buffer = chunkTask->taskPackageForDelete->getTaskResult();
				if (buffer == NULL)
				{
					delete midResultBuffer;
					midResultBuffer = NULL;
					delete chunkTask;
					chunkTask = NULL;
					TripleBitWorkerQuery::timeCount--;
					return true;
				}
				excuteDeleteDataForDeleteClause(chunkTask->chunkTaskTime, buffer, soType, predicateID,
												chunkTask->taskPackageForDelete->constSubject, subjectID, object, objType);
			}
		}
		else
		{
			delete midResultBuffer;
		}

		midResultBuffer = NULL;
	}

	delete chunkTask;
	chunkTask = NULL;
	TripleBitWorkerQuery::timeCount--;
	return true;
}
*/

/*
void TripleBitWorkerQuery::excuteDeleteDataForDeleteClause(timeval &startTime, MidResultBuffer *buffer, const bool soType, const ID predicateID, const bool constSubject, const ID subjectID, const double object, const char objType)
{
	int chunkID;
	std::tr1::shared_ptr<subTaskPackage> taskPackage(new subTaskPackage);
	TripleBitQueryGraph::OpType operationType = TripleBitQueryGraph::DELETE_DATA;
	TripleNode::Op scanType = TripleNode::NOOP;
	if (soType == ORDERBYS)
	{
		if (constSubject)   //subject是常量，仅删除对应的object
		{
			MidResultBuffer::SignalO *objects = buffer->getObjectBuffer();
			for (size_t i = 0; i < buffer->getUsedSize(); ++i)
			{
				chunkID = chunkManager[ORDERBYO][predicateID]->getChunkIndex()->searchChunk(objects[i].object,
						  subjectID);
				TripleBitWorkerQuery::timeCount++;
				ChunkTask *chunkTask = new ChunkTask(startTime, operationType, subjectID, predicateID,
													 objects[i].object, objects[i].objType, scanType, taskPackage, ORDERBYO, chunkID);
				ThreadPool::getChunkPool()[(predicateID + chunkID) % cthread]->addTask(
					boost::bind(&TripleBitWorkerQuery::excuteDeleteChunkData, this, chunkTask));
			}
		}
		else     //subject是未知量，删除所有subject与object
		{
			size_t chunkCount = 0, chunkIDMin = 0, chunkIDMax = 0;
			chunkIDMax = chunkManager[ORDERBYO][predicateID]->getChunkNumber() - 1;
			chunkCount = chunkIDMax - chunkIDMin + 1;
			std::tr1::shared_ptr<SubTaskPackageForDelete> taskPackage(new SubTaskPackageForDelete());
			if (chunkCount != 0)
			{
				for (size_t offsetID = chunkIDMin; offsetID <= chunkIDMax; offsetID++)
				{
					TripleBitWorkerQuery::timeCount++;
					ChunkTask *chunkTask = new ChunkTask(startTime, operationType, 0, predicateID, object, objType,
														 scanType, taskPackage, ORDERBYO, offsetID); //subjectID为0表示删除所有记录
					ThreadPool::getChunkPool()[(predicateID + chunkID) % cthread]->addTask(
						boost::bind(&TripleBitWorkerQuery::excuteDeleteChunkData, this, chunkTask));
				}
			}
		}
	}
	else if (soType == ORDERBYO)
	{
		ID *subejctIDs = buffer->getSignalIDBuffer();
		for (size_t i = 0; i < buffer->getUsedSize(); ++i)
		{
			chunkID = chunkManager[ORDERBYS][predicateID]->getChunkIndex()->searchChunk(subejctIDs[i], object);
			TripleBitWorkerQuery::timeCount++;
			ChunkTask *chunkTask = new ChunkTask(startTime, operationType, subejctIDs[i], predicateID, object, objType,
												 scanType, taskPackage, ORDERBYS, chunkID);
			ThreadPool::getChunkPool()[(predicateID + chunkID) % cthread]->addTask(
				boost::bind(&TripleBitWorkerQuery::excuteDeleteChunkData, this, chunkTask));
		}
	}
	delete buffer;
	buffer = NULL;
}
*/

/*
Status TripleBitWorkerQuery::excuteUpdate()
{
	TripleNode delTripleNode = _query->tripleNodes[0];
	TripleNode insertTripleNode = _query->tripleNodes[1];
	TripleBitQueryGraph::OpType delOPType = TripleBitQueryGraph::DELETE_CLAUSE;
	//TripleBitQueryGraph::OpType insertOPType = TripleBitQueryGraph::INSERT_DATA;

	if (delTripleNode.constSubject)
	{
		insertTripleNode.subjectID = delTripleNode.subjectID;
		cout << "del subjectID: " << delTripleNode.subjectID << "\t";
	}
	else if (delTripleNode.constObject)
	{
		insertTripleNode.objType = delTripleNode.objType;
		insertTripleNode.object = delTripleNode.object;
		cout << "del object: ";
		cout.setf(ios::fixed, ios::floatfield);
		cout << delTripleNode.object;
		cout.unsetf(ios::floatfield);
		cout << "\t";
	}
	insertTripleNode.predicateID = delTripleNode.predicateID;
	cout << "predicateID: " << delTripleNode.predicateID << endl;

	cout << "insert subjectID: " << insertTripleNode.subjectID << "\tpredicateID: " << insertTripleNode.predicateID
		 << "\tobject: " << insertTripleNode.object << endl;

	timeval start;
	gettimeofday(&start, NULL);
	SubTrans *subTrans1 = new SubTrans(start, workerID, 0, 0, delOPType, delTripleNode);
	srand((unsigned) time(NULL));

	TripleBitWorkerQuery::timeCount = 1;
	ThreadPool::getSubTranPool()[rand() % gthread]->addTask(
		boost::bind(&TripleBitWorkerQuery::excuteSubTransDeleteClause, this, subTrans1));
	while (TripleBitWorkerQuery::timeCount != 0)
	{
	}
	executeInsertTripleNodeForUpdate(insertTripleNode);
	return OK;
}
*/

/*
void TripleBitWorkerQuery::executeInsertTripleNodeForUpdate(TripleNode &tripleNode)
{

	timeval start;
	TripleBitQueryGraph::OpType operationType = TripleBitQueryGraph::INSERT_DATA;
	TripleNode::Op scanType = TripleNode::NOOP;
	gettimeofday(&start, NULL);
	ID chunkID = chunkManager[ORDERBYS][tripleNode.predicateID]->getChunkIndex()->searchChunk(tripleNode.subjectID,
				 tripleNode.object);
	ChunkTask *chunkTaskS = new ChunkTask(start, operationType, tripleNode.subjectID, tripleNode.predicateID,
										  tripleNode.object, tripleNode.objType, scanType, ORDERBYS, chunkID);
	executeInsertChunkTaskForUpdate(chunkTaskS);
	chunkID = chunkManager[ORDERBYO][tripleNode.predicateID]->getChunkIndex()->searchChunk(tripleNode.object,
			  tripleNode.subjectID);
	ChunkTask *chunkTaskO = new ChunkTask(start, operationType, tripleNode.subjectID, tripleNode.predicateID,
										  tripleNode.object, tripleNode.objType, scanType, ORDERBYO, chunkID);
	executeInsertChunkTaskForUpdate(chunkTaskO);
}

void TripleBitWorkerQuery::executeInsertChunkTaskForUpdate(ChunkTask *chunkTask)
{
	ID predicateID = chunkTask->Triple.predicateID;
	ID chunkID = chunkTask->chunkID;
	bool soType = chunkTask->opSO;

	uchar *currentPtrChunk, *chunkBegin, *startPtrChunk, *endPtrChunk;
	const uchar *startPtr = chunkStartPtr[soType][predicateID][chunkID];

	//bool first = true;

	if (chunkID == 0)
	{
		chunkBegin = const_cast<uchar *>(startPtr) - sizeof(ChunkManagerMeta);
	}
	else
	{
		chunkBegin = const_cast<uchar *>(startPtr);
	}
	startPtrChunk = const_cast<uchar *>(startPtr);
	endPtrChunk = chunkBegin + MemoryBuffer::pagesize;
	MetaData *metaData = (MetaData *) startPtrChunk;

	MetaData *metaDataChunk = metaData;

	currentPtrChunk = startPtrChunk + metaData->usedSpace;
	bool isTempMMapBuffer = false;

	if (metaData->pageNo != NO_TEMPBUFFER_PAGE)
	{
		startPtrChunk = chunkBegin = reinterpret_cast<uchar *>(TempMMapBuffer::getInstance().getAddress())
									 + (metaData->NextPageNo - 1) * MemoryBuffer::pagesize;
		isTempMMapBuffer = true;
		endPtrChunk = chunkBegin + MemoryBuffer::pagesize;
		metaData = (MetaData *) startPtrChunk;
		currentPtrChunk = startPtrChunk + metaData->usedSpace;
	}

	uint len = sizeof(ID) + sizeof(char) + Chunk::getLen(chunkTask->Triple.objType);
	if (currentPtrChunk + len > endPtrChunk)
	{
		metaData->usedSpace = currentPtrChunk - startPtrChunk;

		while (currentPtrChunk + len > endPtrChunk)
		{
			if (metaData->NextPageNo == 0)
			{
				size_t pageNo = 0;	//apply new page
				startPtrChunk = chunkBegin = reinterpret_cast<uchar *>(TempMMapBuffer::getInstance().getPage(pageNo));

				endPtrChunk = chunkBegin + MemoryBuffer::pagesize;
				isTempMMapBuffer = true;
				metaData->NextPageNo = pageNo;
				metaData = (MetaData *) chunkBegin;
				metaData->NextPageNo = 0;
				metaData->usedSpace = sizeof(MetaData);
				metaData->pageNo = pageNo;
				if (soType == ORDERBYS)
				{
					metaData->min = chunkTask->Triple.subjectID;
				}
				else
				{
					metaData->min = chunkTask->Triple.object;
				}
				currentPtrChunk = chunkBegin + sizeof(MetaData);
				break;
			}
			else
			{
				startPtrChunk = chunkBegin = reinterpret_cast<uchar *>(TempMMapBuffer::getInstance().getAddress())
											 + (metaData->NextPageNo - 1) * MemoryBuffer::pagesize;
				isTempMMapBuffer = true;
				endPtrChunk = chunkBegin + MemoryBuffer::pagesize;
				metaData = (MetaData *) startPtrChunk;
				currentPtrChunk = chunkBegin + metaData->usedSpace;
			}
		}

	}

	chunkManager[soType][predicateID]->writeXY(currentPtrChunk, chunkTask->Triple.subjectID, chunkTask->Triple.object,
			chunkTask->Triple.objType);

	if (isTempMMapBuffer)
	{
		if (soType == ORDERBYS)
		{
			metaData->max = chunkTask->Triple.subjectID;
		}
		else
		{
			metaData->max = chunkTask->Triple.object;
		}
	}
	Chunk::tripleCountAdd(metaData);
	currentPtrChunk += len;
	metaData->updateCount++;
	metaData->usedSpace = currentPtrChunk - startPtrChunk;

	if (metaDataChunk != metaData)
	{
		metaDataChunk->pageNo = metaData->pageNo;
	}

	delete chunkTask;
	chunkTask = NULL;
}
*/

void TripleBitWorkerQuery::classifyTripleNode()
{
	tripleNodeMap.clear();
	for (vector<TripleNode>::iterator iter = _query->tripleNodes.begin(); iter != _query->tripleNodes.end(); ++iter)
	{
		tripleNodeMap[iter->predicateID].push_back(&(*iter));
	}
}

//查询
Status TripleBitWorkerQuery::excuteQuery()
{
	if (_query->tripleNodes.size() == ONE)
	{
		if (_query->joinVariables.size() == ONE)
		{
			singlePatternWithSingleVariable();
		}
		else if (_query->joinVariables.size() == TWO)
		{
			// singlePatternWithTwoVariable();
		}
		else if (_query->joinVariables.size() == THREE)
		{
		}
	}
	else if (_query->joinVariables.size() == ONE)
	{
		//		singleVariableJoin();
	}
	else
	{
		if (_query->joinGraph == TripleBitQueryGraph::ACYCLIC)
		{
			//			acyclicJoin();
		}
		else if (_query->joinGraph == TripleBitQueryGraph::CYCLIC)
		{
			//			cyclicJoin();
		}
	}
	return OK;
}

//查询
Status TripleBitWorkerQuery::singlePatternWithSingleVariable()
{
	vector<TripleNode>::iterator iter = _query->tripleNodes.begin();
	TripleBitQueryGraph::OpType operationType = TripleBitQueryGraph::QUERY;
	size_t chunkCount = 0, chunkIDMin = 0, chunkIDMax = 0;
	size_t resultSize = 0;
	//查询S
	if (!iter->constSubject)
	{
		if (chunkManager[ORDERBYO][iter->predicateID]->getChunkIndex()->searchChunk(iter->objectID, iter->objectID + 1, chunkIDMin))
		{
			chunkIDMax = chunkManager[ORDERBYO][iter->predicateID]->getChunkIndex()->searchChunk(iter->objectID, UINT_MAX);
			assert(chunkIDMax >= chunkIDMin);
			chunkCount = chunkIDMax - chunkIDMin + 1;
		}
		else
		{
			return NOT_FOUND;
		}

		chunkCount = chunkIDMax - chunkIDMin + 1;
		if (chunkCount != 0){
			ChunkTask* chunkTask = new ChunkTask[chunkCount];
			for (size_t offsetID = chunkIDMin; offsetID <= chunkIDMax; offsetID++) {
			chunkTask[offsetID - chunkIDMin].setChunkTask(*transactionTime, operationType, *iter, ORDERBYO, offsetID);
			}
			ID startChunkID, endChunkID;
			size_t perSize = (chunkCount / cthread);
			perSize = chunkCount % cthread == 0 ? perSize : (perSize + 1);

			for (int i = 0; i < cthread; i++) {
			startChunkID = perSize * i;
			endChunkID = (startChunkID + perSize > chunkCount) ? chunkCount : (startChunkID + perSize);
			ThreadPool::getChunkPool()[i]->addTask(boost::bind(&TripleBitWorkerQuery::executeChunkTaskSQuery, this, chunkTask, startChunkID, endChunkID));
			}

			ThreadPool::waitChunkThread();

			for (auto& pid_pair : resultSet) {
			for (auto &chunkID_pair : pid_pair.second)
			{
				resultSize += chunkID_pair.second->getUsedSize();
				delete chunkID_pair.second;
				chunkID_pair.second = NULL;
			}
			pid_pair.second.clear();
			}

			resultSet.clear();
			//			string uri;
			//			for (size_t index = 0; index < resultSize; index++) {
			//				tripleBitRepo->getURITable()->getURIById(uri, midResultBuffer->getSignalIDBuffer()[index]);
			//				cout << uri << endl;
			//			}
			cout << "result size: " << resultSize << endl;
			/**
			 * 处理结果集 midResultBuffer
			 */
			delete[] chunkTask;
			chunkTask = NULL;
	}
}
else if (!iter->constPredicate)
{
	//		midResultBuffer = new MidResultBuffer(MidResultBuffer::SIGNALID);
	//		vector<ID> pOfS, pOfO;
	//		tripleBitRepo->getSTInfo()->get(iter->subjectID, STRING, pOfS);
	//		tripleBitRepo->getSTInfo()->get(iter->object, iter->objType, pOfO);
	//		for (auto & pid : pOfS) {
	//			if (find(pOfO.begin(), pOfO.end(), pid) != pOfO.end()) {
	//				size_t spcount, opcount;
	//				tripleBitRepo->getSTInfo()->get(iter->subjectID, STRING, iter->predicateID, spcount);
	//				tripleBitRepo->getSTInfo()->get(iter->object, iter->objType, iter->predicateID, opcount);
	//				if (spcount <= opcount) {
	//					if (chunkManager[ORDERBYS][iter->predicateID]->getChunkIndex()->searchChunk(iter->subjectID,
	//							iter->subjectID + 1, chunkIDMin)) {
	//						chunkIDMax = chunkManager[ORDERBYS][iter->predicateID]->getChunkIndex()->searchChunk(
	//								iter->subjectID,
	//								UINT_MAX);
	//						assert(chunkIDMax >= chunkIDMin);
	//					} else {
	//						return NOT_FOUND;
	//					}
	//					chunkCount = chunkIDMax - chunkIDMin + 1;
	//					if (chunkCount != 0) {
	//						midResultBuffer = new MidResultBuffer(MidResultBuffer::SIGNALID);
	//						ChunkTask *chunkTask = new ChunkTask[chunkCount];
	//						for (size_t offsetID = chunkIDMin; offsetID <= chunkIDMax; offsetID++) {
	//							chunkTask[offsetID].setChunkTask(*transactionTime, operationType, *iter, ORDERBYS,
	//									offsetID);
	//							taskEnQueue(&chunkTask[offsetID], chunkQueue[ORDERBYS][iter->predicateID][offsetID]);
	//						}
	//					}
	//				} else {
	//					if (chunkManager[ORDERBYO][iter->predicateID]->getChunkIndex()->searchChunk(iter->object,
	//							iter->object + 1, chunkIDMin)) {
	//						chunkIDMax = chunkManager[ORDERBYO][iter->predicateID]->getChunkIndex()->searchChunk(
	//								iter->object,
	//								UINT_MAX);
	//						assert(chunkIDMax >= chunkIDMin);
	//						chunkCount = chunkIDMax - chunkIDMin + 1;
	//					} else {
	//						return NOT_FOUND;
	//					}
	//
	//					if (chunkCount != 0) {
	//						midResultBuffer = new MidResultBuffer(MidResultBuffer::SIGNALID);
	//						ChunkTask *chunkTask = new ChunkTask[chunkCount];
	//						for (size_t offsetID = chunkIDMin; offsetID <= chunkIDMax; offsetID++) {
	//							chunkTask[offsetID].setChunkTask(*transactionTime, operationType, *iter, ORDERBYO,
	//									offsetID);
	//							taskEnQueue(&chunkTask[offsetID], chunkQueue[ORDERBYO][iter->predicateID][offsetID]);
	//						}
	//
	//						ThreadPool::waitSubTranThread();
	//
	//						/**
	//						 * 处理结果集 midResultBuffer
	//						 */
	//					}
	//				}
	//			}
	//		}
}
//查询O
else if (!iter->constObject)
{
	// string s, p;
	// tripleBitRepo->getURITable()->getURIById(s, iter->subjectID);
	// tripleBitRepo->getPredicateTable()->getPredicateByID(p, iter->predicateID);
	// for (string to : tripleBitRepo->shixu[s][p]) {
	//	cout << to << endl;
	// }

	if (chunkManager[ORDERBYS][iter->predicateID]->getChunkIndex()->searchChunk(iter->subjectID, iter->subjectID + 1, chunkIDMin))
	{
		chunkIDMax = chunkManager[ORDERBYS][iter->predicateID]->getChunkIndex()->searchChunk(iter->subjectID, UINT_MAX);
		assert(chunkIDMax >= chunkIDMin);
	}
	else
	{
		return NOT_FOUND;
	}
	chunkCount = chunkIDMax - chunkIDMin + 1;
	if (chunkCount != 0)
	{
		//
		struct timeval start, end;
		gettimeofday(&start, NULL);

		ChunkTask *chunkTask = new ChunkTask[chunkCount];
		for (size_t offsetID = chunkIDMin; offsetID <= chunkIDMax; offsetID++)
		{
			chunkTask[offsetID - chunkIDMin].setChunkTask(*transactionTime, operationType, *iter, ORDERBYS, offsetID);
		}
		ID startChunkID, endChunkID;
		size_t perSize = (chunkCount / cthread);
		perSize = chunkCount % cthread == 0 ? perSize : (perSize + 1);

		for (int i = 0; i < cthread; i++)
		{
			startChunkID = perSize * i;
			endChunkID = (startChunkID + perSize > chunkCount) ? chunkCount : (startChunkID + perSize);
			ThreadPool::getChunkPool()[i]->addTask(boost::bind(&TripleBitWorkerQuery::executeChunkTaskSQuery, this, chunkTask, startChunkID, endChunkID));
		}

		ThreadPool::waitChunkThread();

		gettimeofday(&end, NULL);
		cout << "time: " << ((end.tv_sec - start.tv_sec) * 1000000 + end.tv_usec - start.tv_usec) / 1000000.0 << "s" << endl;

		delete[] chunkTask;
		chunkTask = NULL;
	}
}

return OK;
}

/*
Status TripleBitWorkerQuery::singlePatternWithTwoVariable() {
	cout<<"singwithtwo"<<endl;
	vector<TripleNode>::iterator iter = _query->tripleNodes.begin();
	TripleBitQueryGraph::OpType operationType = TripleBitQueryGraph::QUERY;
	size_t chunkCount = 0, chunkIDMin = 0, chunkIDMax = 0;
	size_t resultSize = 0;
	if (!iter->constSubject && iter->constPredicate && !iter->constObject) {
		chunkIDMax = chunkManager[ORDERBYS][iter->predicateID]->getChunkNumber() - 1;
		chunkCount = chunkIDMax - chunkIDMin + 1;
		if (chunkCount != 0) {
			ChunkTask *chunkTask = new ChunkTask[chunkCount];
			for (size_t offsetID = chunkIDMin; offsetID <= chunkIDMax; offsetID++) {
				chunkTask[offsetID].setChunkTask(*transactionTime, operationType, *iter, ORDERBYS, offsetID);
			}

			ID startChunkID, endChunkID;
			size_t perSize = (chunkCount / cthread);
			perSize = chunkCount % cthread == 0 ? perSize : (perSize + 1);

			timeval start, end;
			gettimeofday(&start, NULL);
			for (int i = 0; i < cthread; i++) {
				startChunkID = perSize * i;
				endChunkID = (startChunkID + perSize > chunkCount) ? chunkCount : (startChunkID + perSize);
				ThreadPool::getChunkPool()[i]->addTask( boost::bind(&TripleBitWorkerQuery::executeChunkTaskSQuery, this, chunkTask, startChunkID, endChunkID));
			}
			ThreadPool::waitChunkThread();
			gettimeofday(&end, NULL);

			cout << "find time: " << ((end.tv_sec - start.tv_sec) * 1000000 + end.tv_usec - start.tv_usec) / 1000000.0
				 << "s" << endl;

			for (auto &pid_pair : resultSet) {
				for (auto &chunkID_pair : pid_pair.second) {
					resultSize += chunkID_pair.second->getUsedSize();
					delete chunkID_pair.second;
					chunkID_pair.second = NULL;
				}
				pid_pair.second.clear();
			}

			resultSet.clear();
			cout << "result size: " << resultSize << endl;

			delete[] chunkTask;
			chunkTask = NULL;
		} else {
			return NOT_FOUND;
		}

	}

	return resultSize > 0 ? OK : NOT_FOUND;
}
*/

//块级别查找
void TripleBitWorkerQuery::executeChunkTaskSQuery(ChunkTask *chunkTask, int start, int end)
{
	for (int chunkID = start; chunkID < end; chunkID++)
	{
		executeChunkTaskQuery(&(chunkTask[chunkID]));
	}
}

//在一个chunk中寻找
void TripleBitWorkerQuery::executeChunkTaskQuery(ChunkTask *chunkTask)
{
	MidResultBuffer *buffer = NULL;
	switch (chunkTask->Triple.operation)
	{
		/*
	case TripleNode::FINDSBYPO:
		buffer = new MidResultBuffer(MidResultBuffer::SIGNALID);
		findSubjectByPredicateAndObject(chunkTask, buffer);
		break;
		*/
	case TripleNode::FINDOBYSP:
		buffer = new MidResultBuffer(MidResultBuffer::OBJECT);
		findObjectBySubjectAndPredicate(chunkTask, buffer);
		break;
		/*
	case TripleNode::FINDSBYP:
		buffer = new MidResultBuffer(MidResultBuffer::SIGNALID);
		findSubjectByPredicate(chunkTask, buffer);
		break;
	case TripleNode::FINDOBYP:
		buffer = new MidResultBuffer(MidResultBuffer::OBJECT);
		findObjectByPredicate(chunkTask, buffer);
		break;
	case TripleNode::FINDSOBYP:
		buffer = new MidResultBuffer(MidResultBuffer::SUBJECTOBJECT);
		findSubjectAndObjectByPredicate(chunkTask, buffer);
		break;
		*/
	default:
		break;
	}
	//	chunkTasksMutex.lock();
	//	resultSet[chunkTask->Triple.predicateID][chunkTask->chunkID] = buffer;
	//	chunkTasksMutex.unlock();
	//输出查找结果
	// string uri;
	// for (size_t index = 0; index < buffer->getUsedSize(); index++) {
	//	MidResultBuffer::SignalO object = buffer->getObjectBuffer()[index];
	//	tripleBitRepo->getURITable()->getURIById(uri, object.oID);
	//	cout << uri << endl;
	//}
}

/*
void TripleBitWorkerQuery::findSubjectByPredicateAndObject(ChunkTask* chunkTask, MidResultBuffer* buffer) {
	const uchar* endPtrChunk, * currentPtrChunk;
	ID predicateID = chunkTask->Triple.predicateID,  tempSubjectID;
	ID chunkID = chunkTask->chunkID;
	double object = chunkTask->Triple.object, tempObject;
	char objType = chunkTask->Triple.objType, tempObjType;
	bool soType = chunkTask->opSO;
	const uchar* startPtr = chunkStartPtr[soType][predicateID][chunkID];
	if (chunkID == 0) {
		endPtrChunk = const_cast<uchar*>(startPtr) - sizeof(ChunkManagerMeta) + MemoryBuffer::pagesize;
	}
	else {
		endPtrChunk = const_cast<uchar*>(startPtr) + MemoryBuffer::pagesize;
	}
	MetaData* metaData = (MetaData*) const_cast<uchar*>(startPtr);

	currentPtrChunk = startPtr + sizeof(MetaData);
	while (currentPtrChunk < endPtrChunk) {
		currentPtrChunk = chunkManager[soType][predicateID]->readXY(currentPtrChunk, tempSubjectID, tempObject, tempObjType);
		if (tempSubjectID != 0 && tempObjType == objType && abs(tempObject - object) <= ELAPSED) {
			buffer->insertSIGNALID(tempSubjectID);
		}
	}
	if (metaData->pageNo != ULONG_LONG_MAX) {
		size_t lastPage = metaData->pageNo;
		while (metaData->NextPageNo && metaData->pageNo < lastPage) {
			currentPtrChunk = reinterpret_cast<uchar*>(TempMMapBuffer::getInstance().getAddress()) + (metaData->pageNo - 1) * MemoryBuffer::pagesize;
			endPtrChunk = currentPtrChunk + MemoryBuffer::pagesize;
			metaData = (MetaData*)(const_cast<uchar*>(currentPtrChunk));
			currentPtrChunk += sizeof(MetaData);
			while (currentPtrChunk < endPtrChunk) {
				currentPtrChunk = chunkManager[soType][predicateID]->readXY(currentPtrChunk, tempSubjectID, tempObject, tempObjType);
				if (tempSubjectID != 0 && tempObjType == objType && abs(tempObject - object) <= ELAPSED) {
					buffer->insertSIGNALID(tempSubjectID);
				}
			}
		}
	}
}
*/

//一个chunk块内寻找
void TripleBitWorkerQuery::findObjectBySubjectAndPredicate(ChunkTask *chunkTask, MidResultBuffer *buffer)
{
	const uchar *endPtrChunk, *currentPtrChunk;
	ID predicateID = chunkTask->Triple.predicateID;
	ID subjectID = chunkTask->Triple.subjectID;
	ID chunkID = chunkTask->chunkID;
	ID tempSubjectID, tempObjectID;
	bool soType = chunkTask->opSO;
	const uchar *startPtr = chunkStartPtr[soType][predicateID][chunkID];
	if (chunkID == 0)
	{
		endPtrChunk = const_cast<uchar *>(startPtr) - sizeof(ChunkManagerMeta) + MemoryBuffer::pagesize;
	}
	else
	{
		endPtrChunk = const_cast<uchar *>(startPtr) + MemoryBuffer::pagesize;
	}
	MetaData *metaData = (MetaData *)const_cast<uchar *>(startPtr);

	currentPtrChunk = startPtr + sizeof(MetaData);
	while (currentPtrChunk < endPtrChunk)
	{
		chunkManager[soType][predicateID]->read(currentPtrChunk, tempSubjectID, tempObjectID);
		if (tempSubjectID < subjectID)
		{
			continue;
		}
		else if (tempSubjectID == subjectID)
		{
			buffer->insertObject(tempObjectID);
			string uri;
			tripleBitRepo->getURITable()->getURIById(uri, tempObjectID);
			cout << uri;
		}
		else
		{
			break;
		}
	}

	if (metaData->pageNo != -1)
	{									 //有增加数据块
		int lastPage = metaData->pageNo; //结束块的块号
		while (metaData->NextPageNo != -1)
		{
			currentPtrChunk = reinterpret_cast<uchar *>(TempMMapBuffer::getInstance().getAddress()) + (metaData->NextPageNo) * MemoryBuffer::pagesize;
			endPtrChunk = currentPtrChunk + MemoryBuffer::pagesize;
			metaData = (MetaData *)(const_cast<uchar *>(currentPtrChunk));
			currentPtrChunk += sizeof(MetaData);
			while (currentPtrChunk < endPtrChunk)
			{
				chunkManager[soType][predicateID]->read(currentPtrChunk, tempSubjectID, tempObjectID);
				if (tempSubjectID < subjectID)
				{
					continue;
				}
				else if (tempSubjectID == subjectID)
				{
					buffer->insertObject(tempObjectID);
					string uri;
					tripleBitRepo->getURITable()->getURIById(uri, tempObjectID);
					cout << uri;
				}
				else
				{
					break;
				}
			}
			if (metaData->pageNo == lastPage)
			{
				break;
			}
		}
	}
}
/*
void TripleBitWorkerQuery::findSubjectByPredicate(ChunkTask *chunkTask, MidResultBuffer *buffer) {
	const uchar *endPtrChunk, *currentPtrChunk;
	ID predicateID = chunkTask->Triple.predicateID, tempSubjectID;
	ID chunkID = chunkTask->chunkID;
	double tempObject;
	char tempObjType;
	bool soType = chunkTask->opSO;
	const uchar* startPtr = chunkStartPtr[soType][predicateID][chunkID];

	if (chunkID == 0) {
		endPtrChunk = const_cast<uchar*>(startPtr) - sizeof(ChunkManagerMeta) + MemoryBuffer::pagesize;
	} else {
		endPtrChunk = const_cast<uchar*>(startPtr) + MemoryBuffer::pagesize;
	}
	MetaData *metaData = (MetaData *) const_cast<uchar*>(startPtr);

	currentPtrChunk = startPtr + sizeof(MetaData);
	while (currentPtrChunk < endPtrChunk) {
		currentPtrChunk = chunkManager[soType][predicateID]->readXY(currentPtrChunk, tempSubjectID, tempObject, tempObjType);
		if (tempSubjectID != 0) {
			buffer->insertSIGNALID(tempSubjectID);
		}
	}

	if (metaData->pageNo != ULONG_LONG_MAX) {
		size_t lastPage = metaData->pageNo;
		while (metaData->NextPageNo && metaData->pageNo < lastPage) {
			currentPtrChunk = reinterpret_cast<uchar*>(TempMMapBuffer::getInstance().getAddress())
							  + (metaData->pageNo - 1) * MemoryBuffer::pagesize;
			endPtrChunk = currentPtrChunk + MemoryBuffer::pagesize;
			metaData = (MetaData *) (const_cast<uchar*>(currentPtrChunk));
			currentPtrChunk += sizeof(MetaData);
			while (currentPtrChunk < endPtrChunk) {
				currentPtrChunk = chunkManager[soType][predicateID]->readXY(currentPtrChunk, tempSubjectID, tempObject,
								  tempObjType);
				if (tempSubjectID != 0) {
					buffer->insertSIGNALID(tempSubjectID);
				}
			}
		}
	}
}

void TripleBitWorkerQuery::findObjectByPredicate(ChunkTask *chunkTask, MidResultBuffer *buffer) {
	const uchar *endPtrChunk, *currentPtrChunk;
	ID predicateID = chunkTask->Triple.predicateID,  tempSubjectID;
	ID chunkID = chunkTask->chunkID;
	double tempObject;
	char tempObjType;
	bool soType = chunkTask->opSO;
	const uchar* startPtr = chunkStartPtr[soType][predicateID][chunkID];

	if (chunkID == 0) {
		endPtrChunk = const_cast<uchar*>(startPtr) - sizeof(ChunkManagerMeta) + MemoryBuffer::pagesize;
	} else {
		endPtrChunk = const_cast<uchar*>(startPtr) + MemoryBuffer::pagesize;
	}
	MetaData *metaData = (MetaData *) const_cast<uchar*>(startPtr);

	currentPtrChunk = startPtr + sizeof(MetaData);
	while (currentPtrChunk < endPtrChunk) {
		currentPtrChunk = chunkManager[soType][predicateID]->readXY(currentPtrChunk, tempSubjectID, tempObject,
						  tempObjType);
		if (tempSubjectID != 0) {
			buffer->insertObject(tempObject, tempObjType);
		}
	}

	if (metaData->pageNo != ULONG_LONG_MAX) {
		size_t lastPage = metaData->pageNo;
		while (metaData->NextPageNo && metaData->pageNo < lastPage) {
			currentPtrChunk = reinterpret_cast<uchar*>(TempMMapBuffer::getInstance().getAddress())
							  + (metaData->pageNo - 1) * MemoryBuffer::pagesize;
			endPtrChunk = currentPtrChunk + MemoryBuffer::pagesize;
			metaData = (MetaData *) (const_cast<uchar*>(currentPtrChunk));
			currentPtrChunk += sizeof(MetaData);
			while (currentPtrChunk < endPtrChunk) {
				currentPtrChunk = chunkManager[soType][predicateID]->readXY(currentPtrChunk, tempSubjectID, tempObject,
								  tempObjType);
				if (tempSubjectID != 0) {
					buffer->insertObject(tempObject, tempObjType);
				}
			}
		}
	}

}

void TripleBitWorkerQuery::findSubjectAndObjectByPredicate(ChunkTask *chunkTask, MidResultBuffer *buffer) {
	const uchar *endPtrChunk, *currentPtrChunk;
	ID predicateID = chunkTask->Triple.predicateID, tempSubjectID;
	ID chunkID = chunkTask->chunkID;
	double tempObject;
	char tempObjType;
	bool soType = chunkTask->opSO;
	const uchar* startPtr = chunkStartPtr[soType][predicateID][chunkID];

	if (chunkID == 0) {
		endPtrChunk = const_cast<uchar*>(startPtr) - sizeof(ChunkManagerMeta) + MemoryBuffer::pagesize;
	} else {
		endPtrChunk = const_cast<uchar*>(startPtr) + MemoryBuffer::pagesize;
	}
	MetaData *metaData = (MetaData *) const_cast<uchar*>(startPtr);

	currentPtrChunk = startPtr + sizeof(MetaData);
	while (currentPtrChunk < endPtrChunk) {
		currentPtrChunk = chunkManager[soType][predicateID]->readXY(currentPtrChunk, tempSubjectID, tempObject, tempObjType);
		if (tempSubjectID != 0) {
			buffer->insertSOPO(tempSubjectID, tempObject, tempObjType);
		}
	}

	if (metaData->pageNo != ULONG_LONG_MAX) {
		size_t lastPage = metaData->pageNo;
		while (metaData->NextPageNo && metaData->pageNo < lastPage) {
			currentPtrChunk = reinterpret_cast<uchar*>(TempMMapBuffer::getInstance().getAddress()) + (metaData->pageNo - 1) * MemoryBuffer::pagesize;
			endPtrChunk = currentPtrChunk + MemoryBuffer::pagesize;
			metaData = (MetaData *) (const_cast<uchar*>(currentPtrChunk));
			currentPtrChunk += sizeof(MetaData);
			while (currentPtrChunk < endPtrChunk) {
				currentPtrChunk = chunkManager[soType][predicateID]->readXY(currentPtrChunk, tempSubjectID, tempObject, tempObjType);
				if (tempSubjectID != 0) {
					buffer->insertSOPO(tempSubjectID, tempObject, tempObjType);
				}
			}
		}
	}
}
*/
