/*
 * SortChunkTaskQueue.h
 *
 *  Created on: 2018年1月3日
 *      Author: XuQingQing
 */

#ifndef TRIPLEBIT_COMM_SORTCHUNKTASKQUEUE_H_
#define TRIPLEBIT_COMM_SORTCHUNKTASKQUEUE_H_

#include "../TripleBit.h"
#include "Tools.h"

class SortChunkTask: private Uncopyable {
public:
	const uchar *startPtr;
	ID chunkID;
	ID predicateID;
	bool soType;
	SortChunkTask *next;

	SortChunkTask(){
		next = NULL;
	}
	SortChunkTask(const uchar *startPtr, ID predicateID, ID chunkID, bool soType) :
		startPtr(startPtr), predicateID(predicateID), chunkID(chunkID), soType(soType) {
		next = NULL;
	}
};

struct SortChunkTaskQueue {
	SortChunkTask *head, *tail;
	pthread_mutex_t headMutex, tailMutex;
	size_t index;

	SortChunkTaskQueue(size_t index): index(index) {
		SortChunkTask *node = new SortChunkTask;
		head = tail = node;
		pthread_mutex_init(&headMutex, NULL);
		pthread_mutex_init(&tailMutex, NULL);
	}
	~SortChunkTaskQueue() {
		if (head != NULL) {
			delete head;
			head = NULL;
			tail = NULL;
		}
		pthread_mutex_destroy(&headMutex);
		pthread_mutex_destroy(&tailMutex);
	}

	void EnQueue(SortChunkTask *task) {
		tail->next = task;
		tail = task;
	}

	SortChunkTask* Dequeue() {
		SortChunkTask* task = NULL;
		if(head != tail){
			task = head->next;
			head->next = task->next;
			if(task->next == NULL){
				tail = head;
			}
		}
		return task;
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
};



#endif /* TRIPLEBIT_COMM_SORTCHUNKTASKQUEUE_H_ */
