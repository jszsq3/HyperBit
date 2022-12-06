/*
 * MMapBuffer.h
 *
 *  Created on: Oct 6, 2010
 *      Author: root
 */

#ifndef MMAPBUFFER_H_
#define MMAPBUFFER_H_

#include "TripleBit.h"
#include "MemoryBuffer.h"
//#define VOLATILE   
class MMapBuffer {
	int fd;
	char volatile* mmap_addr;
	char* curretHead;
	string filename;
	size_t size;
public:
	uchar* resize(size_t incrementSize);
	uchar* getBuffer();
	uchar* getBuffer(int pos);
	void discard();
	void close();
	Status flush();
	string getFileName() {
		return filename;
	}
	size_t getSize() {
		return size;
	}
	size_t get_length() {
		return size;
	}
	uchar * get_address() const {
		return (uchar*) mmap_addr;
	}
	int get_page_count(){
		return getSize()/MemoryBuffer::pagesize;
	}

	virtual Status resize(size_t new_size, bool clear);
	virtual void memset(char value);

	MMapBuffer(const char* filename, size_t initSize);
	MMapBuffer(const char* filename);
	virtual ~MMapBuffer();

public:
	static MMapBuffer* create(const char* filename, size_t initSize);
};

#endif /* MMAPBUFFER_H_ */
