/*
 * ThreadPool.h
 *
 *  Created on: 2010-6-18
 *      Author: liupu
 */

#ifndef THREADPOOL_H_
#define THREADPOOL_H_

#include <vector>
#include "TripleBit.h"
#include <pthread.h>
#include <boost/function.hpp>

using namespace std;

class ThreadPool {
private:
	ThreadPool();

public:
	typedef boost::function<void()> Task;

	class NodeTask {
	public:
		Task value;
		NodeTask *next;
		NodeTask() :
				value(0), next(NULL) {
		}
		NodeTask(const Task &val) :
				value(val), next(NULL) {
		}
	};

public:
	NodeTask *head, *tail;
	timeval start,end;

private:
	bool shutDown;
	pthread_t tid;
	pthread_mutex_t headMutex, tailMutex;
	pthread_cond_t pthreadCond, pthreadEmpty;

	static vector<ThreadPool*> workPool, chunkPool, subTranThreadPool;

private:
	static void *threadFunc(void *threadData);
	int moveToIdle(pthread_t tid);
	int moveToBusy(pthread_t);
	void Enqueue(const Task &task);
	Task Dequeue();
	bool isEmpty() {
		return head->next == NULL;
	}

public:
	static vector<ThreadPool*> &getWorkPool();
	static vector<ThreadPool*> &getChunkPool();
	/*static ThreadPool &getPartitionPool();*/
	static vector<ThreadPool*> &getSubTranPool();
	static void waitWorkThread();
	static void waitSubTranThread();
	static void waitChunkThread();
	static void createAllPool();
	static void deleteAllPool();
	static void waitAllPoolComplete();
	virtual ~ThreadPool();
	int addTask(const Task &task);
	int stopAll();
	int wait();//wait方法不可用，不能检测任务队列是否最终为空

	pthread_t get_tid(){
		return tid;
	}
};

#endif /* THREADPOOL_H_ */
