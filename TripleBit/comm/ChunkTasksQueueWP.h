/*
 * ChunkTasksQueueWP.h
 *
 *  Created on: 2013-7-1
 *      Author: root
 */

#ifndef CHUNKTASKSQUEUEWP_H_
#define CHUNKTASKSQUEUEWP_H_

#include "../TripleBit.h"
#include "../TripleBitQueryGraph.h"
#include "Tools.h"
#include "IndexForTT.h"
#include "Tasks.h"

#include <boost/thread/mutex.hpp>
#include <boost/thread/condition.hpp>

using namespace std;
/**
 * for combine
 */

class ChunkTrans: private Uncopyable {
public:
	const uchar *startPtr;
	ID chunkID;
	bool soType;
	std::tr1::shared_ptr<IndexForTT> indexForTT;

	ChunkTrans(const uchar *startPtr, ID chunkID, bool soType, std::tr1::shared_ptr<IndexForTT> index_forTT) :
		startPtr(startPtr), chunkID(chunkID), soType(soType), indexForTT(index_forTT) {
	}
};

class ChunkNodeTasksQueueWP: private Uncopyable {
public:
	ChunkTrans *tasksWP;
	ChunkNodeTasksQueueWP *next;
	ChunkNodeTasksQueueWP() :
			tasksWP(NULL), next(NULL) {
	}
	~ChunkNodeTasksQueueWP() {
	}
	ChunkNodeTasksQueueWP(ChunkTrans *chunkTrans) :
			tasksWP(chunkTrans), next(NULL) {
	}
};

class ChunkTasksQueueWP {
private:
	ChunkNodeTasksQueueWP *head, *tail;
	pthread_mutex_t headMutex, tailMutex;

public:
	ChunkTasksQueueWP() {
		ChunkNodeTasksQueueWP *node = new ChunkNodeTasksQueueWP;
		head = tail = node;
		pthread_mutex_init(&headMutex, NULL);
		pthread_mutex_init(&tailMutex, NULL);
	}
	~ChunkTasksQueueWP() {
		if (head != NULL) {
			delete head;
			head = NULL;
			tail = NULL;
		}
		pthread_mutex_destroy(&headMutex);
		pthread_mutex_destroy(&tailMutex);
	}

	void EnQueue(ChunkTrans *chunkTrans) {
		ChunkNodeTasksQueueWP *node = new ChunkNodeTasksQueueWP(chunkTrans);
		pthread_mutex_lock(&tailMutex);
		tail->next = node;
		tail = node;
		pthread_mutex_unlock(&tailMutex);
	}

	ChunkTrans *Dequeue() {
		ChunkNodeTasksQueueWP *node = NULL;
		ChunkTrans *trans = NULL;

		pthread_mutex_lock(&headMutex);
		node = head;
		ChunkNodeTasksQueueWP *newNode = node->next;
		if (newNode == NULL) {
			pthread_mutex_unlock(&headMutex);
			return trans;
		}
		trans = newNode->tasksWP;
		head = newNode;
		pthread_mutex_unlock(&headMutex);
		delete node;
		return trans;
	}

	bool Queue_Empty() {
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

#endif /* CHUNKTASKSQUEUEWP_H_ */
