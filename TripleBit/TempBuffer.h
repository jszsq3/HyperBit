/*
 * TempBuffer.h
 *
 *  Created on: 2013-7-10
 *      Author: root
 */

#ifndef TEMPBUFFER_H_
#define TEMPBUFFER_H_

#include "TripleBit.h"
#include "ThreadPool.h"
#include "util/atomic.hpp"

class TempBuffer {
public:
	ChunkTriple* buffer;
	int pos; //buffer's index

	size_t usedSize;
	size_t totalSize;
	TempBuffer * next;

	//static triplebit::atomic<size_t> count;
private:
	ID predicateID;
	ID chunkID;
	bool soType;
public:
	ID getPredicateID(){
		return predicateID;
	}
	ID getChunkID(){
		return chunkID;
	}

	bool getSOType(){
		return soType;
	}
	Status insertTriple(ID subjectID, ID objectID);
	void Print();
	Status sort(bool soType);
	void uniqe();
	Status clear();
	ChunkTriple& operator[](const size_t index);
	bool isEquals(ChunkTriple* lTriple, ChunkTriple* rTriple);
	bool isFull() {
		return usedSize >= totalSize;
	}
	bool isEmpty() {
		return usedSize == 0;
	}
	size_t getSize() const {
		return usedSize;
	}
	ChunkTriple* getBuffer() const {
		return buffer;
	}

	ChunkTriple* getEnd() {
		return getBuffer() + usedSize;
	}

	TempBuffer();
	TempBuffer(ID predicateID, ID chunkID, bool soType);
	~TempBuffer();

};
#endif /* TEMPBUFFER_H_ */
