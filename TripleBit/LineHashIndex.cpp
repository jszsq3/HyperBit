/*
 * LineHashIndex.cpp
 *
 *  Created on: Nov 15, 2010
 *      Author: root
 */

#include "LineHashIndex.h"
#include "MMapBuffer.h"
#include "MemoryBuffer.h"
#include "BitmapBuffer.h"

#include <math.h>

LineHashIndex::LineHashIndex(ChunkManager& _chunkManager, IndexType index_type) : chunkManager(_chunkManager), indexType(index_type) {
	idTable = NULL; 
}

LineHashIndex::~LineHashIndex() {
	if (idTable != NULL) {
		delete idTable;
		idTable = NULL;
	}
	startPtr = NULL;
	endPtr = NULL;
	chunkMeta.clear();
}

ID LineHashIndex::MetaXID(size_t index) {
	assert(index < chunkMeta.size());
	return chunkMeta[index].minx;
}

ID LineHashIndex::MetaYID(size_t index) {
	assert(index < chunkMeta.size());
	return chunkMeta[index].miny;
}

//返回ID出现的第一个数据块
size_t LineHashIndex::searchChunkFrank(ID id) {
	size_t low = 0, mid, high = chunkMeta.size() - 1;

	if (low == high) {
		return low;
	}
	while (low < high) {
		mid = low + (high - low) / 2;
		while (MetaXID(mid) == id) {
			if (mid > 0 && MetaXID(mid - 1) < id) {
				return mid - 1;
			}
			if (mid == 0) {
				return mid;
			}
			mid--;
		}
		if (MetaXID(mid) < id) {
			low = mid + 1;
		} else if (MetaXID(mid) > id) {
			high = mid;
		}
	}
	if (low > 0 && MetaXID(low) >= id) {
		return low - 1;
	} else {
		return low;
	}
}

//返回<x,y>所在的块
size_t LineHashIndex::searchChunk(ID x, ID y) {
	if (MetaXID(0) > x || chunkMeta.size() == 0) {
		return 0;
	}

	size_t offsetID = searchChunkFrank(x);
	if (offsetID == chunkMeta.size() - 1) {
		return offsetID;
	}

	while (offsetID < chunkMeta.size() - 1) {
		if (MetaXID(offsetID + 1) == x) {
			if (MetaYID(offsetID + 1) > y) {
				return offsetID;
			}
			else {
				offsetID++;
			}
		}
		else {
			return offsetID;
		}
	}
	return offsetID;
}

//return the  exactly which chunk the triple(xID, yID) is in
bool LineHashIndex::searchChunk(ID x, ID y, size_t& offsetID){
	if (MetaXID(0) > x || chunkMeta.size() == 0) {
		offsetID = 0;
		return false;
	}

	offsetID = searchChunkFrank(x);
	if (offsetID == chunkMeta.size()) {
		return false;
	}

	while (offsetID < chunkMeta.size() - 1) {
		if (MetaXID(offsetID + 1) == x) {
			if (MetaYID(offsetID + 1) > y) {
				return true;
			}
			else {
				offsetID++;
			}
		}
		else {
			return true;
		}
	}
	return true;
}

/*
void LineHashIndex::updateChunkMetaData(uint offsetId) {
	//if (offsetId == 0) {
	const uchar* reader = NULL;
	ID subjectID;
	double object;
	char objType;
	reader = startPtr + chunkMeta[offsetId].offsetBegin;
	if (chunkManager.getChunkManagerMeta()->soType == ORDERBYS) {
		reader = Chunk::readID(reader, subjectID);
		reader = Chunk::read(reader, objType, CHAR);
		reader = Chunk::read(reader, object, objType);
		chunkMeta[offsetId].minx = subjectID;
		chunkMeta[offsetId].miny = object;
	} else if (chunkManager.getChunkManagerMeta()->soType == ORDERBYO) {
		reader = Chunk::read(reader, objType, CHAR);
		reader = Chunk::read(reader, object, objType);
		reader = Chunk::readID(reader, subjectID);
		chunkMeta[offsetId].minx = object;
		chunkMeta[offsetId].miny = subjectID;
	}
}
*/

LineHashIndex* LineHashIndex::load(ChunkManager& manager, IndexType index_type, size_t& offset) {
	LineHashIndex* index = new LineHashIndex(manager, index_type);
	const uchar* chunkBegin;
	const uchar* reader;
	register ID subjectID;
	register ID objectID;
	index->startPtr = index->chunkManager.getStartPtr();
	index->endPtr = index->chunkManager.getEndPtr();
	if (index_type == SUBJECT_INDEX) {
		if (index->startPtr == index->endPtr) {
			index->chunkMeta.push_back( { 0, 0 });
			cout << "LineHashIndex::load error" << endl;
			return index;
		}
		reader = index->startPtr + sizeof(MetaData);
		Chunk::readID(reader, subjectID);
		Chunk::readID(reader, objectID);
		index->chunkMeta.push_back( { subjectID, objectID });
		chunkBegin = index->startPtr - sizeof(ChunkManagerMeta) + MemoryBuffer::pagesize;
		while (chunkBegin < index->endPtr) {
			reader = chunkBegin + sizeof(MetaData);
			Chunk::readID(reader, subjectID);
			Chunk::readID(reader, objectID);
			index->chunkMeta.push_back( { subjectID, objectID });
			chunkBegin = chunkBegin + MemoryBuffer::pagesize;
		}

	} else if (index_type == OBJECT_INDEX) {
		if (index->startPtr == index->endPtr) {
			index->chunkMeta.push_back( { 0, 0 });
			cout << "LineHashIndex::load error" << endl;
			return index;
		}
		reader = index->startPtr + sizeof(MetaData);
		Chunk::readID(reader, objectID);
		Chunk::readID(reader, subjectID);
		index->chunkMeta.push_back( { objectID, subjectID });
		chunkBegin = index->startPtr - sizeof(ChunkManagerMeta) + MemoryBuffer::pagesize;
		while (chunkBegin < index->endPtr) {
			reader = chunkBegin + sizeof(MetaData);
			Chunk::readID(reader, objectID);
			Chunk::readID(reader, subjectID);
			index->chunkMeta.push_back( { objectID, subjectID });
			chunkBegin = chunkBegin + MemoryBuffer::pagesize;
		}
	}
	else {
		cout << "LineHashIndex::load error" << endl;
	}

	return index;
}
