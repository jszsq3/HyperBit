/*
 * TempBufferQueue.h
 *
 *  Created on: 2018年1月3日
 *      Author: XuQingQing
 */

#ifndef TRIPLEBIT_COMM_TEMPBUFFERQUEUE_H_
#define TRIPLEBIT_COMM_TEMPBUFFERQUEUE_H_

#include "../TempBuffer.h"
struct TempBufferQueue {
	TempBuffer *head, *tail;
	ID predicateID;
	pthread_mutex_t mutex;
	pthread_cond_t pthreadCond;

	TempBufferQueue(){}
	TempBufferQueue(ID predicateID) :
			predicateID(predicateID) {
		head = tail = new TempBuffer;
		pthread_mutex_init(&mutex, NULL);
		pthread_cond_init(&pthreadCond, NULL);
	}

	~TempBufferQueue() {
		if (head != NULL) {
			delete head;
			head = NULL;
			tail = NULL;
		}
		pthread_mutex_destroy(&mutex);
		pthread_cond_destroy(&pthreadCond);
	}

	bool isEmpty() {
		pthread_mutex_lock(&mutex);
		if (head == tail) {
			pthread_mutex_unlock(&mutex);
			return true;
		} else {
			pthread_mutex_unlock(&mutex);
			return false;
		}
	}

	void EnQueue(TempBuffer *tempBuffer) {
		/*if (head == tail) {
			pthread_mutex_lock(&mutex);
			empty = false;
			pthread_mutex_unlock(&mutex);
		}*/
		tail->next = tempBuffer;
		tail = tempBuffer;
	}

	TempBuffer *Dequeue() {
		TempBuffer * tempBuffer = NULL;
		if (head != tail) {
			tempBuffer = head->next;
			head->next = tempBuffer->next;
			if (head->next == NULL) {
				tail = head;
				/*pthread_mutex_lock(&mutex);
				empty = true;
				pthread_mutex_unlock(&mutex);*/
				pthread_cond_broadcast(&pthreadCond);
			}
		}
		return tempBuffer;
	}
};

#endif /* TRIPLEBIT_COMM_TEMPBUFFERQUEUE_H_ */
