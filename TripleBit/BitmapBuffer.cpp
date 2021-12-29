/*
 * ChunkManager.cpp
 *
 *  Created on: 2010-4-12
 *      Author: root
 */

#include "MemoryBuffer.h"
#include "BitmapBuffer.h"
#include "MMapBuffer.h"
#include "TempFile.h"
#include "TempMMapBuffer.h"

//#define WORD_ALIGN 1
//#define MYDEBUG

BitmapBuffer::BitmapBuffer(const string _dir) : dir(_dir) {
	// TODO Auto-generated constructor stub
	string filename(dir);
	filename.append("/tempByS");
	tempByS = new MMapBuffer(filename.c_str(), INIT_PAGE_COUNT * MemoryBuffer::pagesize);
	
	filename.assign(dir.begin(), dir.end());
	filename.append("/tempByO");
	tempByO = new MMapBuffer(filename.c_str(), INIT_PAGE_COUNT * MemoryBuffer::pagesize);
	usedPageByS = usedPageByO = 0;
}

BitmapBuffer::~BitmapBuffer() {
	// TODO Auto-generated destructor stub
	for (map<ID, ChunkManager*>::iterator iter = predicate_managers[0].begin(); iter != predicate_managers[0].end(); iter++) {
		if (iter->second != 0) {
			delete iter->second;
			iter->second = NULL;
		}
	}

	for (map<ID, ChunkManager*>::iterator iter = predicate_managers[1].begin(); iter != predicate_managers[1].end(); iter++) {
		if (iter->second != 0) {
			delete iter->second;
			iter->second = NULL;
		}
	}
}

Status BitmapBuffer::insertPredicate(ID predicateID, OrderByType soType) {
	predicate_managers[soType][predicateID] = new ChunkManager(predicateID, soType, this);
	return OK;
}

Status BitmapBuffer::insertTriple(ID predicateID, ID subjectID, ID objectID, ID timeID, OrderByType soType) {
	getChunkManager(predicateID, soType)->insert(subjectID, objectID,timeID);
	return OK;
}

size_t BitmapBuffer::getTripleCount() {
	size_t tripleCount = 0;
	map<ID, ChunkManager*>::iterator begin, limit;
	for (begin = predicate_managers[0].begin(), limit = predicate_managers[0].end(); begin != limit; begin++) {
		tripleCount = tripleCount + begin->second->getTripleCount();
	}
	cout << "triple count: " << tripleCount << endl;

	tripleCount = 0;
	for (begin = predicate_managers[1].begin(), limit = predicate_managers[1].end(); begin != limit; begin++) {
		tripleCount = tripleCount + begin->second->getTripleCount();
	}
	cout << "triple count: " << tripleCount << endl;

	return tripleCount;
}

/*
 *	@param id: the chunk manager id ( predicate id );
 *       type: the predicate_manager type;
 */
ChunkManager* BitmapBuffer::getChunkManager(ID predicateID, OrderByType soType) {
	if (!predicate_managers[soType].count(predicateID)) {
		insertPredicate(predicateID, soType);
	}
	return predicate_managers[soType][predicateID];
}

void BitmapBuffer::flush() {
	tempByS->flush();
	tempByO->flush();
}

uchar* BitmapBuffer::getPage(bool soType, size_t& pageNo) {
	uchar* newPageStartPtr;
	bool tempresize = false;

	if (soType == ORDERBYS) {
		if (usedPageByS * MemoryBuffer::pagesize >= tempByS->getSize()) {
			tempByS->resize(INCREMENT_PAGE_COUNT * MemoryBuffer::pagesize);
			tempresize = true;
		}
		pageNo = usedPageByS;
		newPageStartPtr = tempByS->get_address() + usedPageByS * MemoryBuffer::pagesize;
		usedPageByS++;
	} else if (soType == ORDERBYO) {
		if (usedPageByO * MemoryBuffer::pagesize >= tempByO->getSize()) {
			tempByO->resize(INCREMENT_PAGE_COUNT * MemoryBuffer::pagesize);
			tempresize = true;
		}
		pageNo = usedPageByO;
		newPageStartPtr = tempByO->get_address() + usedPageByO * MemoryBuffer::pagesize;
		usedPageByO++;
	}

	if (tempresize) {
		if (soType == ORDERBYS) {
			map<ID, ChunkManager*>::iterator iter, limit;
			iter = predicate_managers[0].begin();
			limit = predicate_managers[0].end();
			for (; iter != limit; iter++) {
				if (iter->second == NULL)
					continue;
				iter->second->meta = (ChunkManagerMeta*) (tempByS->get_address() + iter->second->usedPages[0] * MemoryBuffer::pagesize);
				if (iter->second->usedPages.size() == 1) {
					iter->second->meta->startPtr = tempByS->get_address() + iter->second->usedPages[0] * MemoryBuffer::pagesize + sizeof(ChunkManagerMeta);
					iter->second->meta->endPtr = tempByS->get_address() + iter->second->usedPages.back() * MemoryBuffer::pagesize + MemoryBuffer::pagesize - (iter->second->meta->length - iter->second->meta->usedSpace - sizeof(ChunkManagerMeta));
				} else {
					iter->second->meta->startPtr = tempByS->get_address() + iter->second->usedPages[0] * MemoryBuffer::pagesize;
					iter->second->meta->endPtr = tempByS->get_address() + iter->second->usedPages.back() * MemoryBuffer::pagesize + MemoryBuffer::pagesize - (iter->second->meta->length - iter->second->meta->usedSpace - sizeof(ChunkManagerMeta));
				}
			}

		} else if (soType == ORDERBYO) {
			map<ID, ChunkManager*>::iterator iter, limit;
			iter = predicate_managers[1].begin();
			limit = predicate_managers[1].end();
			for (; iter != limit; iter++) {
				if (iter->second == NULL)
					continue;
				iter->second->meta = (ChunkManagerMeta*) (tempByO->get_address() + iter->second->usedPages[0] * MemoryBuffer::pagesize);
				if (iter->second->usedPages.size() == 1) {
					iter->second->meta->startPtr = tempByO->get_address() + iter->second->usedPages[0] * MemoryBuffer::pagesize + sizeof(ChunkManagerMeta);
					iter->second->meta->endPtr = tempByO->get_address() + iter->second->usedPages.back() * MemoryBuffer::pagesize + MemoryBuffer::pagesize - (iter->second->meta->length - iter->second->meta->usedSpace - sizeof(ChunkManagerMeta));
				} else {
					iter->second->meta->startPtr = tempByO->get_address() + iter->second->usedPages[0] * MemoryBuffer::pagesize;
					iter->second->meta->endPtr = tempByO->get_address() + iter->second->usedPages.back() * MemoryBuffer::pagesize + MemoryBuffer::pagesize - (iter->second->meta->length - iter->second->meta->usedSpace - sizeof(ChunkManagerMeta));
				}
			}
		}
	}

	return newPageStartPtr;
}

void BitmapBuffer::save() {
	
	string filename = dir + "/BitmapBuffer";
	MMapBuffer* buffer;
	uchar* bufferWriter;

	string predicateFile(filename);
	predicateFile.append("_predicate");
	MMapBuffer* predicateBuffer = new MMapBuffer(predicateFile.c_str(), predicate_managers[0].size() * (sizeof(ID) + sizeof(SOType) + sizeof(size_t) * 2) * 2);
	uchar* predicateWriter = predicateBuffer->get_address();
	

	map<ID, ChunkManager*>::const_iterator iter = predicate_managers[0].begin();
	size_t offset = 0;

	buffer = new MMapBuffer(filename.c_str(), iter->second->meta->length);
	bufferWriter = buffer->get_address();

	vector<size_t>::iterator pageNoIter = iter->second->usedPages.begin(), limit = iter->second->usedPages.end();

	for (; pageNoIter != limit; pageNoIter++) {
		size_t pageNo = *pageNoIter;
		memcpy(bufferWriter, tempByS->get_address() + pageNo * MemoryBuffer::pagesize, MemoryBuffer::pagesize);
		bufferWriter = bufferWriter + MemoryBuffer::pagesize;
	}

	*((ID*)predicateWriter) = iter->first;
	predicateWriter += sizeof(ID);
	*((SOType*)predicateWriter) = ORDERBYS;
	predicateWriter += sizeof(SOType);
	*((size_t*)predicateWriter) = offset;
	predicateWriter += sizeof(size_t) * 2;//第二个为chunkmanager的index offset

	offset += iter->second->meta->length;


	uchar* startPos = bufferWriter + offset;

	iter++;
	for (; iter != predicate_managers[0].end(); iter++) {
		bufferWriter = buffer->resize(iter->second->meta->length);
		startPos = bufferWriter + offset;

		pageNoIter = iter->second->usedPages.begin();
		limit = iter->second->usedPages.end();

		for (; pageNoIter != limit; pageNoIter++) {
			size_t pageNo = *pageNoIter;
			memcpy(startPos, tempByS->get_address() + pageNo * MemoryBuffer::pagesize, MemoryBuffer::pagesize);
			startPos += MemoryBuffer::pagesize;
		}

		*((ID*)predicateWriter) = iter->first;
		predicateWriter += sizeof(ID);
		*((SOType*)predicateWriter) = ORDERBYS;
		predicateWriter += sizeof(SOType);
		*((size_t*)predicateWriter) = offset;
		predicateWriter += sizeof(size_t) * 2;
		offset += iter->second->meta->length;

		assert(iter->second->usedPages.size() * MemoryBuffer::pagesize == iter->second->meta->length);
	}

	buffer->flush();
	tempByS->discard();
	delete tempByS;
	tempByS = NULL;

	iter = predicate_managers[1].begin();
	for (; iter != predicate_managers[1].end(); iter++) {
		bufferWriter = buffer->resize(iter->second->meta->length);
		startPos = bufferWriter + offset;

		pageNoIter = iter->second->usedPages.begin();
		limit = iter->second->usedPages.end();
		for (; pageNoIter != limit; pageNoIter++) {
			size_t pageNo = *pageNoIter;
			memcpy(startPos, tempByO->get_address() + pageNo * MemoryBuffer::pagesize, MemoryBuffer::pagesize);
			startPos += MemoryBuffer::pagesize;
		}

		*((ID*)predicateWriter) = iter->first;
		predicateWriter += sizeof(ID);
		*((SOType*)predicateWriter) = ORDERBYO;
		predicateWriter += sizeof(SOType);
		*((size_t*)predicateWriter) = offset;
		predicateWriter += sizeof(size_t) * 2;
		offset += iter->second->meta->length;

		assert(iter->second->meta->length == iter->second->usedPages.size() * MemoryBuffer::pagesize);
	}
	buffer->flush();
	predicateBuffer->flush();

	predicateWriter = predicateBuffer->get_address();


	buffer->flush();
	tempByO->discard();
	delete tempByO;
	tempByO = NULL;

	if (buffer != NULL) {
		buffer->close();
		delete buffer;
		buffer = NULL;
	}

	predicateWriter = NULL;
	if (predicateBuffer != NULL) {
		predicateBuffer->close();
		delete predicateBuffer;
		predicateBuffer = NULL;
	}
	
}

BitmapBuffer *BitmapBuffer::load(MMapBuffer* bitmapImage, MMapBuffer* bitmapPredicateImage) {
	BitmapBuffer *buffer = new BitmapBuffer();
	uchar *predicateReader = bitmapPredicateImage->get_address();

	ID id;
	SOType soType;
	size_t offset = 0, indexOffset = 0, predicateOffset = 0;
	size_t sizePredicateBuffer = bitmapPredicateImage->get_length();

	while (predicateOffset < sizePredicateBuffer) {
		id = *(ID*) predicateReader; predicateReader += sizeof(ID);//取PID
		soType = *(SOType*) predicateReader; predicateReader += sizeof(SOType); //取soType
		offset = *(size_t*) predicateReader; predicateReader += sizeof(size_t);  //取偏移
		indexOffset = *(size_t*) predicateReader; predicateReader += sizeof(size_t);
		if (soType == ORDERBYS) {
			ChunkManager *manager = ChunkManager::load(id, ORDERBYS, bitmapImage->get_address(), offset); //加载chunkManager
			manager->chunkIndex = LineHashIndex::load(*manager, LineHashIndex::SUBJECT_INDEX, indexOffset);//新建索引
			buffer->predicate_managers[0][id] = manager;
		} else if (soType == ORDERBYO) {
			ChunkManager *manager = ChunkManager::load(id, ORDERBYO, bitmapImage->get_address(), offset);
			manager->chunkIndex = LineHashIndex::load(*manager, LineHashIndex::OBJECT_INDEX, indexOffset);
			buffer->predicate_managers[1][id] = manager;
		}
		predicateOffset += sizeof(ID) + sizeof(SOType) + sizeof(size_t) * 2;
	}

	return buffer;
}

void BitmapBuffer::endUpdate(MMapBuffer *bitmapPredicateImage, MMapBuffer *bitmapOld) {
	uchar *predicateReader = bitmapPredicateImage->get_address();

	int offsetId = 0, tableSize = 0;
	uchar *startPtr, *bufferWriter, *chunkBegin, *chunkManagerBegin, *bufferWriterBegin, *bufferWriterEnd;
	MetaData *metaData = NULL, *metaDataNew = NULL;
	size_t offsetPage = 0, lastoffsetPage = 0;

	ID id = 0;
	SOType soType = 0;
	size_t offset = 0, predicateOffset = 0;
	size_t sizePredicateBuffer = bitmapPredicateImage->get_length();

	string bitmapName = dir + "/BitmapBuffer_Temp";
	MMapBuffer *buffer = new MMapBuffer(bitmapName.c_str(), MemoryBuffer::pagesize);

	while (predicateOffset < sizePredicateBuffer) {
		bufferWriter = buffer->get_address() + offsetPage * MemoryBuffer::pagesize;
		lastoffsetPage = offsetPage;
		bufferWriterBegin = bufferWriter;

		id = *(ID*) predicateReader;
		predicateReader += sizeof(ID);
		soType = *(SOType*) predicateReader;
		predicateReader += sizeof(SOType);
		offset = *(size_t*) predicateReader;
		*((size_t*) predicateReader) = bufferWriterBegin - buffer->get_address();
		predicateReader += sizeof(size_t);
		predicateReader += sizeof(size_t); //skip the indexoffset

		startPtr = predicate_managers[soType][id]->getStartPtr();
		offsetId = 0;
		tableSize = predicate_managers[soType][id]->getChunkNumber();
		metaData = (MetaData*) startPtr;

		chunkBegin = startPtr - sizeof(ChunkManagerMeta);
		chunkManagerBegin = chunkBegin;
		memcpy(bufferWriter, chunkBegin, MemoryBuffer::pagesize);
		offsetPage++;
		metaDataNew = (MetaData*) (bufferWriterBegin + sizeof(ChunkManagerMeta));
		metaDataNew->NextPageNo = 0;

		while (metaData->NextPageNo != 0) {
			chunkBegin = TempMMapBuffer::getInstance().getAddress() + (metaData->NextPageNo - 1) * MemoryBuffer::pagesize;
			metaData = (MetaData*) chunkBegin;
			if (metaData->usedSpace == sizeof(MetaData))
				break;
			buffer->resize(MemoryBuffer::pagesize);
			bufferWriter = buffer->get_address() + offsetPage * MemoryBuffer::pagesize;
			memcpy(bufferWriter, chunkBegin, MemoryBuffer::pagesize);
			offsetPage++;
			metaDataNew = (MetaData*) bufferWriter;
			metaDataNew->NextPageNo = 0;
		}
		offsetId++;
		while (offsetId < tableSize) {
			buffer->resize(MemoryBuffer::pagesize);
			bufferWriter = buffer->get_address() + offsetPage * MemoryBuffer::pagesize;
			chunkBegin = chunkManagerBegin + offsetId * MemoryBuffer::pagesize;
			metaData = (MetaData*) chunkBegin;
			memcpy(bufferWriter, chunkBegin, MemoryBuffer::pagesize);
			offsetPage++;
			metaDataNew = (MetaData*) bufferWriter;
			metaDataNew->NextPageNo = 0;
			while (metaData->NextPageNo != 0) {
				chunkBegin = TempMMapBuffer::getInstance().getAddress() + metaData->NextPageNo * MemoryBuffer::pagesize;
				metaData = (MetaData*) chunkBegin;
				if (metaData->usedSpace == sizeof(MetaData))
					break;
				buffer->resize(MemoryBuffer::pagesize);
				bufferWriter = buffer->get_address() + offsetPage * MemoryBuffer::pagesize;
				memcpy(bufferWriter, chunkBegin, MemoryBuffer::pagesize);
				offsetPage++;
				metaDataNew = (MetaData*) bufferWriter;
				metaDataNew->NextPageNo = 0;
			}
			offsetId++;
		}

		bufferWriterEnd = buffer->get_address() + offsetPage * MemoryBuffer::pagesize;
		bufferWriterBegin = buffer->get_address() + lastoffsetPage * MemoryBuffer::pagesize;
		if (offsetPage == lastoffsetPage + 1) {
			ChunkManagerMeta *meta = (ChunkManagerMeta*) (bufferWriterBegin);
			MetaData *metaDataTemp = (MetaData*) (bufferWriterBegin + sizeof(ChunkManagerMeta));
			meta->usedSpace = metaDataTemp->usedSpace;
			meta->length = MemoryBuffer::pagesize;
		} else {
			ChunkManagerMeta *meta = (ChunkManagerMeta*) (bufferWriterBegin);
			MetaData *metaDataTemp = (MetaData*) (bufferWriterEnd - MemoryBuffer::pagesize);
			meta->usedSpace = bufferWriterEnd - bufferWriterBegin - sizeof(ChunkManagerMeta) - MemoryBuffer::pagesize + metaDataTemp->usedSpace;
			meta->length = bufferWriterEnd - bufferWriterBegin;
			assert(meta->length % MemoryBuffer::pagesize == 0);
		}
		buffer->flush();

		//not update the LineHashIndex
		predicateOffset += sizeof(ID) + sizeof(SOType) + sizeof(size_t) * 2;
	}

	predicateOffset = 0;
	predicateReader = bitmapPredicateImage->get_address();
	while (predicateOffset < sizePredicateBuffer) {
		id = *(ID*) predicateReader;
		predicateReader += sizeof(ID);
		soType = *(SOType*) predicateReader;
		predicateReader += sizeof(SOType);
		offset = *(size_t*) predicateReader;
		predicateReader += sizeof(size_t);
		predicateReader += sizeof(size_t);

#ifdef TTDEBUG
		cout << "id:" << id << " soType:" << soType << endl;
		cout << "offset:" << offset << " indexOffset:" << predicateOffset << endl;
#endif

		uchar *base = buffer->get_address() + offset;
		ChunkManagerMeta *meta = (ChunkManagerMeta*) base;
		meta->startPtr = base + sizeof(ChunkManagerMeta);
		meta->endPtr = meta->startPtr + meta->usedSpace;

		predicate_managers[soType][id]->meta = meta;
//		predicate_managers[soType][id]->buildChunkIndex();
		//predicate_managers[soType][id]->updateChunkIndex();

		predicateOffset += sizeof(ID) + sizeof(SOType) + sizeof(size_t) * 2;
	}
	buffer->flush();

	string bitmapNameOld = dir + "/BitmapBuffer";
//	MMapBuffer *bufferOld = new MMapBuffer(bitmapNameOld.c_str(), 0);
	bitmapOld->discard();
	if (rename(bitmapName.c_str(), bitmapNameOld.c_str()) != 0) {
		perror("rename bitmapName error!");
	}
}

void BitmapBuffer::print(int f) {
	for (const auto& e : predicate_managers[0]) {
		e.second->print(f);
	}

}

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////

void getTempFilename(string& filename, unsigned pid, unsigned _type) {
	filename.clear();
	filename.append(DATABASE_PATH);
	filename.append("temp_");
	char temp[5];
	sprintf(temp, "%d", pid);
	filename.append(temp);
	sprintf(temp, "%d", _type);
	filename.append(temp);
}

ChunkManager::ChunkManager(ID predicateID, OrderByType soType, BitmapBuffer* _bitmapBuffer) : bitmapBuffer(_bitmapBuffer) {
	usedPages.resize(0);
	size_t pageNo = 0;
	meta = NULL;
	lastChunkStartPtr = bitmapBuffer->getPage(soType, pageNo);
	usedPages.push_back(pageNo);

	meta = (ChunkManagerMeta*) lastChunkStartPtr;
	memset((char*) meta, 0, sizeof(ChunkManagerMeta));
	meta->endPtr = meta->startPtr = lastChunkStartPtr + sizeof(ChunkManagerMeta);
	meta->length = usedPages.size() * MemoryBuffer::pagesize;
	meta->usedSpace = 0;
	meta->tripleCount = 0;
	meta->pid = predicateID;
	meta->soType = soType;

	pthread_mutex_init(&mutex, NULL);

	chunkIndex = NULL;
}

ChunkManager::~ChunkManager() {
	lastChunkStartPtr = NULL;
	if (chunkIndex != NULL)
		delete chunkIndex;
	chunkIndex = NULL;
	pthread_mutex_destroy(&mutex);
}

void ChunkManager::write(uchar*& reader, ID subjectID, ID objectID,ID timeID) {
	Chunk::writeID(reader, subjectID);
	Chunk::writeID(reader, objectID);
	Chunk::writeID(reader, timeID);
}

void ChunkManager::read(const uchar*& reader, ID& subjectID, ID& objectID, ID& timeID) {
	Chunk::readID(reader, subjectID);
	Chunk::readID(reader, objectID);
	Chunk::readID(reader, timeID);
}

void ChunkManager::deleteTriple(uchar*& reader)
{
	Chunk::deleteData(reader);
}

//向chunkManager插入一条数据<S,O,T>
void ChunkManager::insert(ID subjectID, ID objectID,ID timeID) {
	uint len = sizeof(ID) * 3;
	uchar* writePtr;
	//需要申请新的块
	if (isChunkOverFlow(len) == true) {
		//重新申请块
		size_t pageNo;
		resize(pageNo);
		//写入数据
		writePtr = meta->endPtr + sizeof(MetaData);
		write(writePtr, subjectID, objectID, timeID);
		//初始化metaData
		MetaData* metaData = (MetaData*)(meta->endPtr);
		metaData->pageNo = -1;//最后一块是原始块
		metaData->NextPageNo = -1;//-1表示没有下一块
		metaData->usedSpace = sizeof(MetaData) + len;
		metaData->updateCount = 0;
		metaData->tripleCount = 1;
		//更新meta
		meta->endPtr = meta->endPtr + sizeof(MetaData) + len;
		meta->usedSpace = meta->length - (MemoryBuffer::pagesize  - sizeof(MetaData) - len) - sizeof(ChunkManagerMeta);

	}
	//不需要申请新的块
	//第一次添加 需要初始化metadata
	else if (meta->usedSpace == 0) {
		//写入数据
		writePtr = meta->startPtr + sizeof(MetaData);
		write(writePtr, subjectID, objectID, timeID);
		//初始化metaData
		MetaData* metaData = (MetaData*)(meta->startPtr);
		metaData->NextPageNo = -1;
		metaData->pageNo = -1;
		metaData->updateCount = 0;
		metaData->usedSpace = sizeof(MetaData) + len;
		metaData->tripleCount = 1;
		//更新meta
		meta->endPtr = meta->endPtr + sizeof(MetaData) + len;
		meta->usedSpace = meta->usedSpace + sizeof(MetaData) + len;
	}
	//不需要申请新的块
	// 直接添加
	else {
		//写入数据
		writePtr = meta->endPtr;
		write(writePtr, subjectID, objectID, timeID);
		//更新metaDate
		MetaData* metaData;
		if (meta->length == MemoryBuffer::pagesize) {
			metaData = (MetaData*)(meta->endPtr - meta->usedSpace);
		}
		else {
			size_t usedPage = MemoryBuffer::pagesize - (meta->length - meta->usedSpace - sizeof(ChunkManagerMeta));
			metaData = (MetaData*)(meta->endPtr - usedPage);
		}
		metaData->usedSpace = metaData->usedSpace + len;
		metaData->tripleCount++;
		//更新meta
		meta->endPtr = meta->endPtr + len;
		meta->usedSpace = meta->usedSpace + len;
	}
}

Status ChunkManager::resize(size_t &pageNo) {
	lastChunkStartPtr = bitmapBuffer->getPage(meta->soType, pageNo);
	usedPages.push_back(pageNo);
	meta->length = usedPages.size() * MemoryBuffer::pagesize;
	meta->endPtr = lastChunkStartPtr;

	return OK;
}

bool ChunkManager::isChunkOverFlow(uint len) {
	return sizeof(ChunkManagerMeta) + meta->usedSpace + len > meta->length;
}

void ChunkManager::setMetaDataMin(MetaData *metaData, ID x, double y) {
	if (meta->soType == ORDERBYS) {
		metaData->min = x;
		metaData->max = x;
	} else if (meta->soType == ORDERBYO) {
		metaData->min = y;
		metaData->max = y;
	}
}

size_t ChunkManager::getChunkNumber() {
	return (meta->length) / (MemoryBuffer::pagesize);
}

ChunkManager* ChunkManager::load(ID predicateID, bool soType, uchar* buffer, size_t& offset) {
	ChunkManagerMeta * meta = (ChunkManagerMeta*) (buffer + offset);
	if (meta->pid != predicateID || meta->soType != soType) {
		MessageEngine::showMessage("load chunkmanager error: check meta info", MessageEngine::ERROR);
		cout << meta->pid << ": " << meta->soType << endl;
		return NULL;
	}

	ChunkManager* manager = new ChunkManager();
	uchar* base = buffer + offset + sizeof(ChunkManagerMeta);
	manager->meta = meta;
	manager->meta->startPtr = base;
	manager->meta->endPtr = manager->meta->startPtr + manager->meta->usedSpace;
	offset = offset + manager->meta->length;

	return manager;
}

//把一个cunkManager的状态写入日志文件
void ChunkManager::print(int f)
{
	//建库
	if (f == 1) {
		//打开日志文件
		ofstream out;
		out.open("chunkmanager1log.txt", ios::out | ios::app);//写   追加
		if (out.is_open())
		{
			//so副本
			if (meta->soType == ORDERBYS) {
				const uchar* reader;
				const uchar* limit;
				int writenum;
				out << "P:" << meta->pid << endl;
				bool f = true;
				MetaData* metaData;
				ID s, o, t;
				for (const auto& e : usedPages) {
					//数据块开始的位置
					reader = bitmapBuffer->tempByS->get_address() + e * MemoryBuffer::pagesize;
					if (f) {
						f = false;
						reader += sizeof(ChunkManagerMeta);
					}
					metaData = (MetaData*)(reader);
					out << "metaData->pageNo: " << metaData->pageNo << endl;
					out << "metaData->NextPageNo: " << metaData->NextPageNo << endl;
					out << "metaData->tripleCount: " << metaData->tripleCount << endl;
					out << "metaData->usedSpace: " << metaData->usedSpace << endl;
					limit = reader + metaData->usedSpace;
					reader = reader + sizeof(MetaData);
					writenum = 10;
					while (reader < limit) {
						read(reader, s, o, t);
						if(writenum){
							out << s << " " << o << " " << t << endl;
							writenum--;
						}
					}
				}
			}
			//os副本
			else if (meta->soType == ORDERBYO) {
			}
			else {
				cout << "error" << endl;
			}
			//关闭文件
			out.close();
		}
		else {
			cout << "open file error" << endl;
		}
	}

	//插入
	if (f == 2) {
		//打开日志文件
		ofstream out;
		out.open("chunkmanager2log.txt", ios::out | ios::app);//写 追加
		if (out.is_open())
		{
			//so副本
			if (meta->soType == ORDERBYS) {
				const uchar* reader;
				const uchar* limit;
				int writenum;
				out << "P:" << meta->pid << endl;
				int chunkNum = meta->length / MemoryBuffer::pagesize;
				out << "Yuanshikuai shumu:" << chunkNum << endl;
				bool f = true;
				MetaData* metaData;
				ID s, o, t;
				int end;
				for (int i = 0; i < chunkNum; i++) {
					out << "yuanshikuai:" << i << endl;
					if (i == 0) {
						reader = meta->startPtr;
					}
					else {
						reader = meta->startPtr - sizeof(ChunkManagerMeta) + i * MemoryBuffer::pagesize;
						
					}
					metaData = (MetaData*)(reader);
					out << "metaData->pageNo: " << metaData->pageNo << endl;
					end = metaData->pageNo;
					out << "metaData->NextPageNo: " << metaData->NextPageNo << endl;
					out << "metaData->tripleCount: " << metaData->tripleCount << endl;
					out << "metaData->usedSpace: " << metaData->usedSpace << endl;
					limit = reader + metaData->usedSpace;
					reader = reader + sizeof(MetaData);
					writenum = 10;
					while (reader < limit) {
						read(reader, s, o, t);
						if(writenum){
							out << s << " " << o << " " << t << endl;
							writenum--;
						}
					}
					if(end != -1){
						while (metaData->NextPageNo != -1) {
							reader = reinterpret_cast<uchar*>(TempMMapBuffer::getInstance().getAddress()) + (metaData->NextPageNo) * MemoryBuffer::pagesize;
							metaData = (MetaData*)(reader);
							out << "metaData->pageNo: " << metaData->pageNo << endl;
							out << "metaData->NextPageNo: " << metaData->NextPageNo << endl;
							out << "metaData->tripleCount: " << metaData->tripleCount << endl;
							out << "metaData->usedSpace: " << metaData->usedSpace << endl;
							limit = reader + metaData->usedSpace;
							reader = reader + sizeof(MetaData);
							writenum = 10;
							while (reader < limit) {
								read(reader, s, o, t);
								if (writenum) {
									out << s << " " << o << " " << t << endl;
									writenum--;
								}
							}
							if (metaData->pageNo == end) {
								break;
							}
						}
					}
				}
			}
			//os副本
			else if (meta->soType == ORDERBYO) {
			}
			else {
				cout << "error" << endl;
			}
			//关闭文件
			out.close();
		}
		else {
			cout << "open file error" << endl;
		}
	}
}

/////////////////////////////////////////////////////////////////////////////////////////////////////////
Chunk::~Chunk() {
	// TODO Auto-generated destructor stub
}

void Chunk::deleteData(uchar*& reader) {
	Chunk::writeID(reader, 0);
}

uint Chunk::getLen(char dataType) {
	int len;
	switch (dataType) {
	//case BOOL:
	case CHAR:
		len = sizeof(char);
		break;
	/*case DATE:
	case DOUBLE:
	case LONGLONG:
		len = sizeof(double);
		break;
	case INT:
	case FLOAT:
	case UNSIGNED_INT:*/
	case STRING:
	default:
		len = sizeof(ID);
		break;
	}
	return len;
}

const uchar* Chunk::skipData(const uchar* reader, char dataType) {
	return reader + getLen(dataType);
}

Status Chunk::getObjTypeStatus(const uchar*& reader, uint& moveByteNum) {
	char objType = *(char*) reader;
	switch (objType) {
	/*case NONE:
		return DATA_NONE;
	case BOOL:*/
	case CHAR:
		moveByteNum = sizeof(char) + sizeof(char);
		reader += moveByteNum;
		return DATA_EXSIT;
	//case BOOL_DELETE:
	case CHAR_DELETE:
		moveByteNum = sizeof(char) + sizeof(char);
		reader += moveByteNum;
		return DATA_DELETE;
	/*case LONGLONG:
		moveByteNum = sizeof(char) + sizeof(longlong);
		reader += moveByteNum;
		return DATA_EXSIT;
	case LONGLONG_DELETE:
		moveByteNum = sizeof(char) + sizeof(longlong);
		reader += moveByteNum;
		return DATA_DELETE;
	case DATE:
	case DOUBLE:
		moveByteNum = sizeof(char) + sizeof(double);
		reader += moveByteNum;
		return DATA_EXSIT;
	case DATE_DELETE:
	case DOUBLE_DELETE:
		moveByteNum = sizeof(char) + sizeof(double);
		reader += moveByteNum;
		return DATA_DELETE;
	case FLOAT_DELETE:
	case INT_DELETE:
	case UNSIGNED_INT_DELETE:*/
	case STRING_DELETE:
		moveByteNum = sizeof(char) + sizeof(uint);
		reader += moveByteNum;
		return DATA_DELETE;
	/*case FLOAT:
	case INT:
	case UNSIGNED_INT:*/
	case STRING:
	default:
		moveByteNum = sizeof(char) + sizeof(uint);
		reader += moveByteNum;
		return DATA_EXSIT;
	}
}

//若无下一对xy，则返回endPtr表示该Chunk无下一对xy
const uchar* Chunk::skipForward(const uchar* reader, const uchar* endPtr, OrderByType soType) {
	if (soType == ORDERBYS) {
		while (reader + sizeof(ID) < endPtr && (*(ID*) reader == 0)) {
			reader += sizeof(ID);
			if (reader + sizeof(char) < endPtr && *(char*) reader != NONE) {
				char objType = *(char*) reader;
				reader += sizeof(char);
				if (reader + getLen(objType) >= endPtr) {
					return endPtr;
				}
			} else {
				return endPtr;
			}
		}
		if (reader + sizeof(ID) >= endPtr) {
			return endPtr;
		} else if (*(ID*) reader != 0) {
			return reader;
		}
	} else if (soType == ORDERBYO) {
		uint moveByteNum = 0;
		int status;
		while (reader + sizeof(char) < endPtr && (*(char*) reader != NONE)) {
			status = getObjTypeStatus(reader, moveByteNum);
			if (status == DATA_NONE) {
				return endPtr;
			} else if (status == DATA_EXSIT) {
				if ((reader += sizeof(ID)) <= endPtr) {
					return reader - moveByteNum;
				} else {
					return endPtr;
				}
			} else if (status == DATA_DELETE) {
				reader += sizeof(ID);
			}
		}
		return endPtr;
	}
	return endPtr;
}

const uchar* Chunk::skipBackward(const uchar* reader, const uchar* endPtr, OrderByType soType) {
	const uchar* temp = reader + sizeof(MetaData);
	reader = temp;
	uint len = 0;
	while (reader < endPtr) {
		len = reader - temp;
		reader = Chunk::skipForward(temp, endPtr, soType);
	}
	if (len) {
		return temp - len;
	}
	return endPtr;
}
