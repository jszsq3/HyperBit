/*
 * LineHashIndex.h
 *
 *  Created on: Nov 15, 2010
 *      Author: root
 */



#ifndef LINEHASHINDEX_H_
#define LINEHASHINDEX_H_

class MemoryBuffer;
class ChunkManager;
class MMapBuffer;

#include "TripleBit.h"

class LineHashIndex {
public:
	struct chunkMetaData{
		ID minx;    //The minIDx of a chunk
		ID miny;		//The minIDy of a chunk
		chunkMetaData(){}
		chunkMetaData(ID minx, ID miny): minx(minx), miny(miny){}
	};

	enum IndexType {
		SUBJECT_INDEX, OBJECT_INDEX
	};
private:
	MemoryBuffer* idTable;
	ChunkManager& chunkManager;
	IndexType indexType;
	uchar* lineHashIndexBase; //used to do update

public:
	//some useful thing about the chunkManager
	const uchar *startPtr, *endPtr;
	vector<chunkMetaData> chunkMeta;

private:
	size_t searchChunkFrank(ID id);
	ID MetaXID(size_t index);
	ID MetaYID(size_t index);
public:
	LineHashIndex(ChunkManager& _chunkManager, IndexType index_type);
	size_t searchChunk(ID x, ID y);
	bool searchChunk(ID x, ID y, size_t& offsetID);
	virtual ~LineHashIndex();
	//void updateChunkMetaData(uint offsetId);
public:
	static LineHashIndex* load(ChunkManager& manager, IndexType index_type, size_t& offset);
};

#endif
