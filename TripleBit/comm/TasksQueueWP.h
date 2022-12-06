/*
 * TasksQueueWP.h
 *
 *  Created on: 2013-7-1
 *      Author: root
 */

#ifndef TASKSQUEUEWP_H_
#define TASKSQUEUEWP_H_

#include "../TripleBit.h"
#include "../TripleBitQueryGraph.h"
#include "Tools.h"
#include "IndexForTT.h"
#include "Tasks.h"

#include <boost/thread/mutex.hpp>
#include <boost/thread/condition.hpp>

using namespace std;

//class NodeTasksQueueWP: private Uncopyable {
//public:
//	SubTrans *tasksWP;
//	NodeTasksQueueWP *next;
//	NodeTasksQueueWP() :
//			tasksWP(NULL), next(NULL) {
//	}
//	~NodeTasksQueueWP() {
//	}
//	NodeTasksQueueWP(SubTrans *subTrans) :
//			tasksWP(subTrans), next(NULL) {
//	}
//};

class TasksQueueWP {
private:
	SubTrans *head, *tail;
	pthread_mutex_t headMutex, tailMutex;

public:
	TasksQueueWP() {
		SubTrans *node = new SubTrans;
		head = tail = node;
		pthread_mutex_init(&headMutex, NULL);
		pthread_mutex_init(&tailMutex, NULL);
	}
	~TasksQueueWP() {
		if (head != NULL) {
			delete head;
			head = NULL;
			tail = NULL;
		}
		pthread_mutex_destroy(&headMutex);
		pthread_mutex_destroy(&tailMutex);
	}

	void EnQueue(SubTrans *subTrans) {
		pthread_mutex_lock(&tailMutex);
		tail->next = subTrans;
		tail = subTrans;
		pthread_mutex_unlock(&tailMutex);
	}

	SubTrans *Dequeue() {
		SubTrans *trans = NULL;
		pthread_mutex_lock(&headMutex);
		if(head != tail){
			trans = head->next;
			head->next = trans->next;
			if(trans->next == NULL){
				tail = head;
			}
		}
		pthread_mutex_unlock(&headMutex);
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


//class TasksQueueWP {
//private:
//	NodeTasksQueueWP *head, *tail;
//	pthread_mutex_t headMutex, tailMutex;
//
//public:
//	TasksQueueWP() {
//		NodeTasksQueueWP *node = new NodeTasksQueueWP;
//		head = tail = node;
//		pthread_mutex_init(&headMutex, NULL);
//		pthread_mutex_init(&tailMutex, NULL);
//	}
//	~TasksQueueWP() {
//		if (head != NULL) {
//			delete head;
//			head = NULL;
//			tail = NULL;
//		}
//		pthread_mutex_destroy(&headMutex);
//		pthread_mutex_destroy(&tailMutex);
//	}
//
//	void EnQueue(SubTrans *subTrans) {
//		NodeTasksQueueWP *node = new NodeTasksQueueWP(subTrans);
//		pthread_mutex_lock(&tailMutex);
//		tail->next = node;
//		tail = node;
//		pthread_mutex_unlock(&tailMutex);
//	}
//
//	SubTrans *Dequeue() {
//		NodeTasksQueueWP *node = NULL;
//		SubTrans *trans = NULL;
//
//		pthread_mutex_lock(&headMutex);
//		node = head;
//		NodeTasksQueueWP *newNode = node->next;
//		if (newNode == NULL) {
//			pthread_mutex_unlock(&headMutex);
//			return trans;
//		}
//		trans = newNode->tasksWP;
//		head = newNode;
//		pthread_mutex_unlock(&headMutex);
//		delete node;
//		return trans;
//	}
//
//	bool Queue_Empty() {
//		pthread_mutex_lock(&headMutex);
//		if (head->next == NULL) {
//			pthread_mutex_unlock(&headMutex);
//			return true;
//		} else {
//			pthread_mutex_unlock(&headMutex);
//			return false;
//		}
//	}
//};

#endif /* TASKSQUEUEWP_H_ */
