/*
 * TasksQueueChunk.h
 *
 *  Created on: 2013-8-20
 *      Author: root
 */

#ifndef TASKSQUEUECHUNK_H_
#define TASKSQUEUECHUNK_H_

#include "../TripleBit.h"
#include "../TripleBitQueryGraph.h"
#include "subTaskPackage.h"
#include "IndexForTT.h"
#include "Tools.h"
#include "Tasks.h"

/*void PrintChunkTask(ChunkTask* chunkTask) {
 cout << "opType:" << chunkTask->operationType << " subject:" << chunkTask->Triple.subjectID << " object:" << chunkTask->Triple.object << " operation:" << chunkTask->Triple.operation << endl;
 }*/

class NodeChunkQueue: private Uncopyable {
public:
	ChunkTask* chunkTask;
	NodeChunkQueue* next;
	NodeChunkQueue() :
			chunkTask(NULL), next(NULL) {
	}
	~NodeChunkQueue() {
	}
	NodeChunkQueue(ChunkTask *chunk_Task) :
			chunkTask(chunk_Task), next(NULL) {
	}
};

class TasksQueueChunk {
private:
	const uchar* chunkBegin;
	ID chunkID;
	bool soType;
	size_t winSize;

private:
	ChunkTask* head;
	ChunkTask* tail;
	pthread_mutex_t headMutex, tailMutex;

public:
	TasksQueueChunk();
	TasksQueueChunk(const uchar* chunk_Begin, ID chunk_ID, bool so_Type) :
			chunkBegin(chunk_Begin), chunkID(chunk_ID), soType(so_Type) {
		head = tail = new ChunkTask();
		pthread_mutex_init(&headMutex, NULL);
		pthread_mutex_init(&tailMutex, NULL);
		winSize = windowSize;
	}

	~TasksQueueChunk() {
		if (head != NULL) {
			delete head;
			head = NULL;
			tail = NULL;
		}
		pthread_mutex_destroy(&headMutex);
		pthread_mutex_destroy(&tailMutex);
	}

	void setTasksQueueChunk(const uchar* chunk_Begin, ID chunk_ID, bool so_Type){
		chunkBegin = chunk_Begin;
		chunkID = chunk_ID;
		soType = so_Type;
	}

	bool isEmpty() {
		pthread_mutex_lock(&headMutex);
		if (head->next == NULL) {
			pthread_mutex_unlock(&headMutex);
			return true;
		} else {
			pthread_mutex_unlock(&headMutex);
			return false;
		}
	}
	const ID getChunkID() {
		return chunkID;
	}
	const uchar* getChunkBegin() {
		return chunkBegin;
	}
	const bool getSOType() {
		return soType;
	}

	const size_t getWinSize() {
		return winSize;
	}

	void EnQueue(ChunkTask *chunkTask) {
		//NodeChunkQueue* nodeChunkQueue = new NodeChunkQueue(chunkTask);
		//pthread_mutex_lock(&tailMutex);
		tail->next = chunkTask;
		tail = chunkTask;
		//pthread_mutex_unlock(&tailMutex);
	}

	ChunkTask* Dequeue() {
		ChunkTask* chunkTask = NULL;
		if (head != tail) {
			chunkTask = head->next;
			head->next = chunkTask->next;
			if(chunkTask->next == NULL){
				tail = head;
			}
		}
		return chunkTask;
	}

	/*ChunkTask * Dequeue() {
		NodeChunkQueue* node = NULL;
		ChunkTask* chunkTask = NULL;

		//pthread_mutex_lock(&headMutex);
		node = head;
		NodeChunkQueue* newNode = node->next;
		if (newNode == NULL) {
			//pthread_mutex_unlock(&headMutex);
			return chunkTask;
		}
		chunkTask = newNode->chunkTask;
		head = newNode;
		//pthread_mutex_unlock(&headMutex);
		delete node;
		return chunkTask;
	}*/
}
;

#endif /* TASKSQUEUECHUNK_H_ */
