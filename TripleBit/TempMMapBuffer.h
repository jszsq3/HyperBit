/*
 * TempMMapBuffer.h
 *
 *  Created on: 2014-1-14
 *      Author: root
 */

#ifndef TEMPMMAPBUFFER_H_
#define TEMPMMAPBUFFER_H_

#include "TripleBit.h"

class TempMMapBuffer {
private:
	int fd;//文件指针
	char volatile *mmapAddr; //映射到内存的起始地址
	string filename; //文件名称
	size_t size;  //文件总的大小(B)
	size_t usedPage; //已经使用的页数
	pthread_mutex_t mutex;//并发控制锁

	static TempMMapBuffer *instance;

private:
	TempMMapBuffer(const char *filename, size_t initSize);
	~TempMMapBuffer();
	void resize(size_t incrementSize);
	//Status resize(size_t newSize, bool clear);
	void memset(char value);

public:
	uchar *getBuffer();
	uchar *getBuffer(int pos);
	void discard();
	void close();
	Status flush();
	size_t getSize() {
		return size;
	}
	size_t getLength() {
		return size;
	}
	uchar *getAddress() const {
		return (uchar*) mmapAddr;
	}
	void getPage(size_t &pageNo);

	void setUsedPage(const size_t usedPage){
		this->usedPage = usedPage;
	}
	size_t getUsedPage() {
		return usedPage;
	}

	void checkSize(size_t needPage);

public:
	static void create(const char *filename, size_t initSize);
	static TempMMapBuffer &getInstance();
	static void deleteInstance();
};

#endif /* TEMPMMAPBUFFER_H_ */
