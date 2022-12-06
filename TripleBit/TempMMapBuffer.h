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
	int fd;//�ļ�ָ��
	char volatile *mmapAddr; //ӳ�䵽�ڴ����ʼ��ַ
	string filename; //�ļ�����
	size_t size;  //�ļ��ܵĴ�С(B)
	size_t usedPage; //�Ѿ�ʹ�õ�ҳ��
	pthread_mutex_t mutex;//����������

	static TempMMapBuffer *instance;
	static TempMMapBuffer *instance_s;
	static TempMMapBuffer *instance_o;

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
	static void create_so(int type,const char *filename, size_t initSize);
	static TempMMapBuffer &getInstance();
	static TempMMapBuffer &getInstance_s();
	static TempMMapBuffer &getInstance_o();
	static void deleteInstance();
	static void deleteInstance_s();
	static void deleteInstance_o();

};

#endif /* TEMPMMAPBUFFER_H_ */
