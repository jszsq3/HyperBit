/*
 * ThreadPool.cpp
 *
 *  Created on: 2010-6-18
 *      Author: liupu
 */

#include "ThreadPool.h"
#include "TripleBit.h"
#include <string>
#include <iostream>

using namespace std;

vector<ThreadPool*> ThreadPool::workPool;
vector<ThreadPool*> ThreadPool::chunkPool;
//ThreadPool *ThreadPool::partitionPool = NULL;
vector<ThreadPool*> ThreadPool::subTranThreadPool;

ThreadPool::ThreadPool() {
	gettimeofday(&start,NULL);
	shutDown = false;
	NodeTask *nodeTask = new NodeTask;
	head = tail = nodeTask;
	pthread_mutex_init(&headMutex, NULL);
	pthread_mutex_init(&tailMutex, NULL);
	pthread_cond_init(&pthreadCond, NULL);
	pthread_cond_init(&pthreadEmpty, NULL);
	pthread_create(&tid, NULL, threadFunc, this);
}

ThreadPool::~ThreadPool() {
	stopAll();
	if (head != NULL) {
		delete head;
		head = NULL;
		tail = NULL;
	}
	pthread_mutex_destroy(&headMutex);
	pthread_mutex_destroy(&tailMutex);
	pthread_cond_destroy(&pthreadCond);
	pthread_cond_destroy(&pthreadEmpty);
}

void* ThreadPool::threadFunc(void * threadData) {
	//pthread_t tid = pthread_self();
	int rnt;
	ThreadPool* pool = (ThreadPool*) threadData;
	while (1) {
		rnt = pthread_mutex_lock(&pool->headMutex);
		if (rnt != 0) {
			cout << "Get mutex error" << endl;
		}

		while (pool->isEmpty() && pool->shutDown == false) {
			pthread_cond_wait(&pool->pthreadCond, &pool->headMutex);
		}
		if (pool->shutDown == true) {
			pthread_mutex_unlock(&pool->headMutex);
			pthread_exit(NULL);
		}

		Task task = pool->Dequeue();

		task();
		//pool->decTaskNum();
		//cout << pool->getTaskNum() << endl;
		if (pool->isEmpty()) {
			//gettimeofday(&pool->end,NULL);
			//cerr << "thread " << tid << "\t"  << ((pool->end.tv_sec - pool->start.tv_sec) * 1000000 + pool->end.tv_usec - pool->start.tv_usec) << endl;
			pthread_cond_broadcast(&pool->pthreadEmpty);
		}
		pthread_mutex_unlock(&pool->headMutex);
	}
	return (void*) 0;
}

void ThreadPool::Enqueue(const Task &task) {
	NodeTask *nodeTask = new NodeTask(task);
	pthread_mutex_lock(&tailMutex);
	tail->next = nodeTask;
	tail = nodeTask;
	pthread_mutex_unlock(&tailMutex);
}

ThreadPool::Task ThreadPool::Dequeue() {
	NodeTask *node, *newNode;
	node = head;
	newNode = head->next;
	Task task = newNode->value;
	head = newNode;
//	pthread_mutex_unlock(&headMutex);
	delete node;
	return task;
}

int ThreadPool::addTask(const Task &task) {
	Enqueue(task);
	pthread_cond_broadcast(&pthreadCond);
	return 0;
}

int ThreadPool::stopAll() {
	shutDown = true;
	pthread_mutex_unlock(&headMutex);
	pthread_cond_broadcast(&pthreadCond);
	pthread_join(tid, NULL);

	return 0;
}

int ThreadPool::wait() {
	pthread_mutex_lock(&headMutex);
	while (!isEmpty()) {
		pthread_cond_wait(&pthreadEmpty, &headMutex);
	}
	pthread_mutex_unlock(&headMutex);
	return 0;
}

vector<ThreadPool*> & ThreadPool::getSubTranPool() {
	if (subTranThreadPool.empty()) {
		for (int i = 0; i < gthread; i++) {
			subTranThreadPool.push_back(new ThreadPool());
			//subTranThreadPool[i]->setTaskNum((tripleSize*2+insert_pid_num)/gthread);
		}
	}
	return subTranThreadPool;
}

vector<ThreadPool*> &ThreadPool::getWorkPool() {
	if (workPool.empty()) {
		for (int i = 0; i < WORK_THREAD_NUMBER; i++) {
			workPool.push_back(new ThreadPool());
		}
	}
	return workPool;
}

vector<ThreadPool*> &ThreadPool::getChunkPool() {
	if (chunkPool.empty()) {
		for (int i = 0; i < cthread/*CHUNK_THREAD_NUMBER*/; i++) {
			chunkPool.push_back(new ThreadPool());
		}
	}
	return chunkPool;
}

void ThreadPool::waitWorkThread() {
	for (int i = 0; i < WORK_THREAD_NUMBER; i++) {
		workPool[i]->wait();
	}
}

void ThreadPool::waitSubTranThread() {
	for (int i = 0; i < gthread; i++) {
		subTranThreadPool[i]->wait();
	}
}

void ThreadPool::waitChunkThread() {
	for (int i = 0; i < cthread/*CHUNK_THREAD_NUMBER*/; i++) {
		chunkPool[i]->wait();
	}
}

/*ThreadPool &ThreadPool::getPartitionPool() {
 if (partitionPool == NULL) {
 partitionPool = new ThreadPool(CHUNK_THREAD_NUMBER);
 }
 return *partitionPool;
 }*/

void ThreadPool::createAllPool() {
	//getWorkPool();
	//getChunkPool();
	//getPartitionPool();
	//getSubTranPool();
}

void ThreadPool::deleteAllPool() {
	for (vector<ThreadPool*>::iterator it = workPool.begin(); it != workPool.end(); it++) {
		if (*it != NULL) {
			delete *it;
			*it = NULL;
		}
	}

	for (vector<ThreadPool*>::iterator it = subTranThreadPool.begin(); it != subTranThreadPool.end(); it++) {
		if (*it != NULL) {
			delete *it;
			*it = NULL;
		}
	}

	for (vector<ThreadPool*>::iterator it = chunkPool.begin(); it != chunkPool.end(); it++) {
		if (*it != NULL) {
			delete *it;
			*it = NULL;
		}
	}
	/*if (partitionPool != NULL) {
	 delete partitionPool;
	 partitionPool = NULL;
	 }*/
}

void ThreadPool::waitAllPoolComplete() {
	waitWorkThread();
	waitChunkThread();
	//getPartitionPool().wait();
	waitSubTranThread();
}
