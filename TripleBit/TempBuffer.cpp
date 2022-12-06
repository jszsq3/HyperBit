/*
 * TempBuffer.cpp
 *
 *  Created on: 2013-7-10
 *      Author: root
 */

#include "TempBuffer.h"
#include "MemoryBuffer.h"
#include <math.h>
#include <pthread.h>

//triplebit::atomic<size_t> TempBuffer::count(0);

TempBuffer::TempBuffer() {
	// TODO Auto-generated constructor stub
	buffer = (ChunkTriple*) malloc(TEMPBUFFER_INIT_PAGE_COUNT * getpagesize() * 1000);
	usedSize = 0;
	totalSize = TEMPBUFFER_INIT_PAGE_COUNT * getpagesize() * 1000 / sizeof(ChunkTriple);
	pos = 0;
	next = NULL;
}

TempBuffer::TempBuffer(ID predicateID, ID chunkID, bool soType):predicateID(predicateID), chunkID(chunkID), soType(soType) {
	// TODO Auto-generated constructor stub
	buffer = (ChunkTriple*) malloc(TEMPBUFFER_INIT_PAGE_COUNT * getpagesize() * 1000);
	usedSize = 0;
	totalSize = TEMPBUFFER_INIT_PAGE_COUNT * getpagesize() * 1000 / sizeof(ChunkTriple);
	pos = 0;
	next = NULL;
}

TempBuffer::~TempBuffer() {
	// TODO Auto-generated destructor stub
	if (buffer != NULL)
		free(buffer);
	buffer = NULL;
}

Status TempBuffer::insertTriple(ID subjectID, ID objectID) {
	buffer[pos].subjectID = subjectID;
	buffer[pos].objectID = objectID;
	pos++;
	usedSize++;
	return OK;
}

Status TempBuffer::clear() {
	pos = 0;
	usedSize = 0;
	return OK;
}

ChunkTriple& TempBuffer::operator[](const size_t index) {
	if (index >= usedSize) {
		return buffer[0];
	}
	return buffer[index];
}

void TempBuffer::Print() {
	for (int i = 0; i < usedSize; ++i) {
		cout << "subjectID:" << buffer[i].subjectID;
		cout << " objectID:" << buffer[i].objectID << " ";
	}
	cout << endl;
}

int cmpByS(const void* lhs, const void* rhs) {
	ChunkTriple* lTriple = (ChunkTriple*)lhs;
	ChunkTriple* rTriple = (ChunkTriple*)rhs;
	if (lTriple->subjectID != rTriple->subjectID) {
		return lTriple->subjectID - rTriple->subjectID;
	}
	else {
		return lTriple->objectID - rTriple->objectID;
	}
}

int cmpByO(const void* lhs, const void* rhs) {
	ChunkTriple* lTriple = (ChunkTriple*)lhs;
	ChunkTriple* rTriple = (ChunkTriple*)rhs;
	if (lTriple->objectID != rTriple->objectID) {
		return lTriple->objectID - rTriple->objectID;
	}
	else {
		return lTriple->subjectID - rTriple->subjectID;
	}
}

Status TempBuffer::sort(bool soType) {
	if (soType == ORDERBYS) {
		qsort(buffer, usedSize, sizeof(ChunkTriple), cmpByS);
	} else if (soType == ORDERBYO) {
		qsort(buffer, usedSize, sizeof(ChunkTriple), cmpByO);
	}

	return OK;
}

bool TempBuffer::isEquals(ChunkTriple* lTriple, ChunkTriple* rTriple) {
	if (lTriple->subjectID == rTriple->subjectID && lTriple->objectID == rTriple->objectID ) {
		return true;
	}
	return false;
}

void TempBuffer::uniqe() {
	if (usedSize <= 1)
		return;
	ChunkTriple *lastPtr, *currentPtr, *endPtr;
	lastPtr = currentPtr = buffer;
	endPtr = getEnd();
	currentPtr++;
	while (currentPtr < endPtr) {
		if (isEquals(lastPtr, currentPtr)) {
			currentPtr++;
		} else {
			lastPtr++;
			*lastPtr = *currentPtr;
			currentPtr++;
		}
	}
	usedSize = lastPtr - buffer + 1;
}

