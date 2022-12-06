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
#include "LogmapBuffer.h"

//#define WORD_ALIGN 1
//#define MYDEBUG

BitmapBuffer::BitmapBuffer(const string _dir) : dir(_dir)
{
	// TODO Auto-generated constructor stub
	string filename(dir);
	filename.append("/tempByS");
	tempByS = new MMapBuffer(filename.c_str(), INIT_PAGE_COUNT * MemoryBuffer::pagesize);

	filename.assign(dir.begin(), dir.end());
	filename.append("/tempByO");
	tempByO = new MMapBuffer(filename.c_str(), INIT_PAGE_COUNT * MemoryBuffer::pagesize);
	usedPageByS = usedPageByO = 0;
}

BitmapBuffer::BitmapBuffer(const string _dir, int no) : dir(_dir), no(no)
{
	// TODO Auto-generated constructor stub
	string filename(dir);
	filename.append("/tempByS").append(toStr(no));
	tempByS = new MMapBuffer(filename.c_str(), INIT_PAGE_COUNT * MemoryBuffer::pagesize);
	filename.assign(dir.begin(), dir.end());
	filename.append("/tempByO").append(toStr(no));
	tempByO = new MMapBuffer(filename.c_str(), INIT_PAGE_COUNT * MemoryBuffer::pagesize);
	usedPageByS = usedPageByO = 0;
}

BitmapBuffer::~BitmapBuffer()
{
	// TODO Auto-generated destructor stub
	for (map<ID, ChunkManager *>::iterator iter = predicate_managers[0].begin(); iter != predicate_managers[0].end(); iter++)
	{
		if (iter->second != 0)
		{
			delete iter->second;
			iter->second = NULL;
		}
	}

	for (map<ID, ChunkManager *>::iterator iter = predicate_managers[1].begin(); iter != predicate_managers[1].end(); iter++)
	{
		if (iter->second != 0)
		{
			delete iter->second;
			iter->second = NULL;
		}
	}
}

Status BitmapBuffer::insertPredicate(ID predicateID, OrderByType soType)
{
	predicate_managers[soType][predicateID] = new ChunkManager(predicateID, soType, this);
	return OK;
}

Status BitmapBuffer::updateEdge(ID *origin, ID *new_ID, int updatepos, TempFile &fact)
{
	//找到origin所在的位置
	if (predicate_managers[0][origin[1]]->updateEdge(origin, new_ID, updatepos, fact) != OK)
	{
		cout << "update Edge failed!" << endl;
		return Fail;
	};
	return OK;
};

Status BitmapBuffer::insertTriple(ID predicateID, ID subjectID, ID objectID, OrderByType soType)
{
	// cout<<subjectID<<" "<<predicateID<<" "<<objectID<<endl;
	getChunkManager(predicateID, soType)->insert(subjectID, objectID);
	return OK;
}

Status BitmapBuffer::insertTriple(ID predicateID, ID subjectID, ID objectID, ID timeID, OrderByType soType)
{
	// cout<<subjectID<<" "<<predicateID<<" "<<objectID<<endl;
	getChunkManager(predicateID, soType)->insert(subjectID, objectID, timeID);
	return OK;
}

size_t BitmapBuffer::getTripleCount()
{
	size_t tripleCount = 0;
	map<ID, ChunkManager *>::iterator begin, limit;
	for (begin = predicate_managers[0].begin(), limit = predicate_managers[0].end(); begin != limit; begin++)
	{
		tripleCount = tripleCount + begin->second->getTripleCount();
	}
	cout << "triple count: " << tripleCount << endl;

	tripleCount = 0;
	for (begin = predicate_managers[1].begin(), limit = predicate_managers[1].end(); begin != limit; begin++)
	{
		tripleCount = tripleCount + begin->second->getTripleCount();
	}
	cout << "triple count: " << tripleCount << endl;

	return tripleCount;
}

/*
 *	@param id: the chunk manager id ( predicate id );
 *       type: the predicate_manager type;
 */
ChunkManager *BitmapBuffer::getChunkManager(OrderByType soType, ID predicateID)
{
	if (!predicate_managers[soType].count(predicateID))
	{
		return NULL;
	}
	else
	{
		return predicate_managers[soType][predicateID];
	}
}
ChunkManager *BitmapBuffer::getChunkManager(ID predicateID, OrderByType soType)
{
	if (!predicate_managers[soType].count(predicateID))
	{
		insertPredicate(predicateID, soType);
	}
	return predicate_managers[soType][predicateID];
}

void BitmapBuffer::flush()
{
	tempByS->flush();
	tempByO->flush();
}

uchar *BitmapBuffer::getPage(bool soType, size_t &pageNo)
{
	uchar *newPageStartPtr;
	bool tempresize = false;

	if (soType == ORDERBYS)
	{
		if (usedPageByS * MemoryBuffer::pagesize >= tempByS->getSize())
		{
			tempByS->resize(INCREMENT_PAGE_COUNT * MemoryBuffer::pagesize);
			tempresize = true;
		}
		pageNo = usedPageByS;
		newPageStartPtr = tempByS->get_address() + usedPageByS * MemoryBuffer::pagesize;
		usedPageByS++;
	}
	else if (soType == ORDERBYO)
	{
		if (usedPageByO * MemoryBuffer::pagesize >= tempByO->getSize())
		{
			tempByO->resize(INCREMENT_PAGE_COUNT * MemoryBuffer::pagesize);
			tempresize = true;
		}
		pageNo = usedPageByO;
		newPageStartPtr = tempByO->get_address() + usedPageByO * MemoryBuffer::pagesize;
		usedPageByO++;
	}

	if (tempresize)
	{
		if (soType == ORDERBYS)
		{
			map<ID, ChunkManager *>::iterator iter, limit;
			iter = predicate_managers[0].begin();
			limit = predicate_managers[0].end();
			for (; iter != limit; iter++)
			{
				if (iter->second == NULL)
					continue;
				iter->second->meta = (ChunkManagerMeta *)(tempByS->get_address() + iter->second->usedPages[0] * MemoryBuffer::pagesize);
				if (iter->second->usedPages.size() == 1)
				{
					iter->second->meta->startPtr = tempByS->get_address() + iter->second->usedPages[0] * MemoryBuffer::pagesize + sizeof(ChunkManagerMeta);
					iter->second->meta->endPtr = tempByS->get_address() + iter->second->usedPages.back() * MemoryBuffer::pagesize + MemoryBuffer::pagesize - (iter->second->meta->length - iter->second->meta->usedSpace - sizeof(ChunkManagerMeta));
				}
				else
				{
					iter->second->meta->startPtr = tempByS->get_address() + iter->second->usedPages[0] * MemoryBuffer::pagesize;
					iter->second->meta->endPtr = tempByS->get_address() + iter->second->usedPages.back() * MemoryBuffer::pagesize + MemoryBuffer::pagesize - (iter->second->meta->length - iter->second->meta->usedSpace - sizeof(ChunkManagerMeta));
				}
			}
		}
		else if (soType == ORDERBYO)
		{
			map<ID, ChunkManager *>::iterator iter, limit;
			iter = predicate_managers[1].begin();
			limit = predicate_managers[1].end();
			for (; iter != limit; iter++)
			{
				if (iter->second == NULL)
					continue;
				iter->second->meta = (ChunkManagerMeta *)(tempByO->get_address() + iter->second->usedPages[0] * MemoryBuffer::pagesize);
				if (iter->second->usedPages.size() == 1)
				{
					iter->second->meta->startPtr = tempByO->get_address() + iter->second->usedPages[0] * MemoryBuffer::pagesize + sizeof(ChunkManagerMeta);
					iter->second->meta->endPtr = tempByO->get_address() + iter->second->usedPages.back() * MemoryBuffer::pagesize + MemoryBuffer::pagesize - (iter->second->meta->length - iter->second->meta->usedSpace - sizeof(ChunkManagerMeta));
				}
				else
				{
					iter->second->meta->startPtr = tempByO->get_address() + iter->second->usedPages[0] * MemoryBuffer::pagesize;
					iter->second->meta->endPtr = tempByO->get_address() + iter->second->usedPages.back() * MemoryBuffer::pagesize + MemoryBuffer::pagesize - (iter->second->meta->length - iter->second->meta->usedSpace - sizeof(ChunkManagerMeta));
				}
			}
		}
	}

	return newPageStartPtr;
}

void BitmapBuffer::save()
{
	string filename = dir + "/BitmapBuffer";
	MMapBuffer *buffer;
	uchar *bufferWriter;
	//分order By S 和 Order By O两个存储数据
	string obsFile(filename);
	obsFile.append("_obs");
	string oboFile(filename);
	oboFile.append("_obo");
	MMapBuffer *obsBuffer, *oboBuffer;
	uchar *obsBufferWriter, *oboBufferWriter;
	//谓词索引
	string predicateFile(filename);
	predicateFile.append("_predicate");
	MMapBuffer *predicateBuffer = new MMapBuffer(predicateFile.c_str(), predicate_managers[0].size() * (sizeof(ID) + sizeof(SOType) + sizeof(size_t) * 2) * 2);
	uchar *predicateWriter = predicateBuffer->get_address();

	map<ID, ChunkManager *>::const_iterator iter = predicate_managers[0].begin();
	size_t offset = 0;
	size_t offset2 = 0; //存order by o 的时候用的
	//之前的buffer
	buffer = new MMapBuffer(filename.c_str(), iter->second->meta->length);
	bufferWriter = buffer->get_address();
	// order by s 的buffer
	obsBuffer = new MMapBuffer(obsFile.c_str(), iter->second->meta->length);
	obsBufferWriter = obsBuffer->get_address();

	vector<size_t>::iterator pageNoIter = iter->second->usedPages.begin(), limit = iter->second->usedPages.end();
	int i = 0;
	for (; pageNoIter != limit; pageNoIter++)
	{
		size_t pageNo = *pageNoIter;
		//向buffer写数据
		memcpy(bufferWriter, tempByS->get_address() + pageNo * MemoryBuffer::pagesize, MemoryBuffer::pagesize);
		bufferWriter = bufferWriter + MemoryBuffer::pagesize;
		//向obsBuffer写数据
		memcpy(obsBufferWriter, tempByS->get_address() + pageNo * MemoryBuffer::pagesize, MemoryBuffer::pagesize);
		obsBufferWriter = obsBufferWriter + MemoryBuffer::pagesize;
	}

	*((ID *)predicateWriter) = iter->first;
	predicateWriter += sizeof(ID);
	*((SOType *)predicateWriter) = ORDERBYS;
	predicateWriter += sizeof(SOType);
	*((size_t *)predicateWriter) = offset;
	predicateWriter += sizeof(size_t) * 2; //第二个为chunkmanager的index offset

	offset += iter->second->meta->length;

	uchar *startPos = bufferWriter + offset;
	uchar *startPos_so = obsBufferWriter + offset;

	iter++;
	for (; iter != predicate_managers[0].end(); iter++)
	{
		//完整buffer
		bufferWriter = buffer->resize(iter->second->meta->length);
		startPos = bufferWriter + offset;
		// obsBuffer
		obsBufferWriter = obsBuffer->resize(iter->second->meta->length);
		startPos_so = obsBufferWriter + offset;

		pageNoIter = iter->second->usedPages.begin();
		limit = iter->second->usedPages.end();

		for (; pageNoIter != limit; pageNoIter++)
		{
			size_t pageNo = *pageNoIter;
			//向buffer写数据
			memcpy(startPos, tempByS->get_address() + pageNo * MemoryBuffer::pagesize, MemoryBuffer::pagesize);
			startPos += MemoryBuffer::pagesize;
			//向obsBuffer写数据
			memcpy(startPos_so, tempByS->get_address() + pageNo * MemoryBuffer::pagesize, MemoryBuffer::pagesize);
			startPos_so = startPos_so + MemoryBuffer::pagesize;
		}

		*((ID *)predicateWriter) = iter->first;
		predicateWriter += sizeof(ID);
		*((SOType *)predicateWriter) = ORDERBYS;
		predicateWriter += sizeof(SOType);
		*((size_t *)predicateWriter) = offset;
		predicateWriter += sizeof(size_t) * 2;
		offset += iter->second->meta->length;

		assert(iter->second->usedPages.size() * MemoryBuffer::pagesize == iter->second->meta->length);
	}

	buffer->flush();
	obsBuffer->flush();
	tempByS->discard();
	delete tempByS;
	tempByS = NULL;
	if (obsBuffer != NULL)
	{
		obsBuffer->close();
		delete obsBuffer;
		obsBuffer = NULL;
	}

	// oder bys end  ..  oder by o start
	iter = predicate_managers[1].begin();
	bufferWriter = buffer->resize(iter->second->meta->length);
	bufferWriter = bufferWriter + offset;
	// oder by o的buffer
	oboBuffer = new MMapBuffer(oboFile.c_str(), iter->second->meta->length);
	oboBufferWriter = oboBuffer->get_address();

	pageNoIter = iter->second->usedPages.begin();
	limit = iter->second->usedPages.end();

	for (; pageNoIter != limit; pageNoIter++)
	{

		size_t pageNo = *pageNoIter;
		//向buffer写数据

		memcpy(bufferWriter, tempByO->get_address() + pageNo * MemoryBuffer::pagesize, MemoryBuffer::pagesize);
		bufferWriter = bufferWriter + MemoryBuffer::pagesize;

		//向obsBuffer写数据
		memcpy(oboBufferWriter, tempByO->get_address() + pageNo * MemoryBuffer::pagesize, MemoryBuffer::pagesize);
		oboBufferWriter = oboBufferWriter + MemoryBuffer::pagesize;
	}

	*((ID *)predicateWriter) = iter->first;
	predicateWriter += sizeof(ID);
	*((SOType *)predicateWriter) = ORDERBYO;
	predicateWriter += sizeof(SOType);
	*((size_t *)predicateWriter) = offset;
	predicateWriter += sizeof(size_t) * 2; //第二个为chunkmanager的index offset

	offset += iter->second->meta->length;
	offset2 += iter->second->meta->length;

	iter++;
	for (; iter != predicate_managers[1].end(); iter++)
	{
		bufferWriter = buffer->resize(iter->second->meta->length);
		startPos = bufferWriter + offset;
		// oboBuffer
		oboBufferWriter = oboBuffer->resize(iter->second->meta->length);
		startPos_so = oboBufferWriter + offset2;

		pageNoIter = iter->second->usedPages.begin();
		limit = iter->second->usedPages.end();
		for (; pageNoIter != limit; pageNoIter++)
		{
			size_t pageNo = *pageNoIter;
			memcpy(startPos, tempByO->get_address() + pageNo * MemoryBuffer::pagesize, MemoryBuffer::pagesize);
			startPos += MemoryBuffer::pagesize;

			//向oboBuffer写数据
			memcpy(startPos_so, tempByO->get_address() + pageNo * MemoryBuffer::pagesize, MemoryBuffer::pagesize);
			startPos_so = startPos_so + MemoryBuffer::pagesize;
		}

		*((ID *)predicateWriter) = iter->first;
		predicateWriter += sizeof(ID);
		*((SOType *)predicateWriter) = ORDERBYO;
		predicateWriter += sizeof(SOType);
		*((size_t *)predicateWriter) = offset;
		predicateWriter += sizeof(size_t) * 2;
		offset += iter->second->meta->length;
		offset2 += iter->second->meta->length;

		assert(iter->second->meta->length == iter->second->usedPages.size() * MemoryBuffer::pagesize);
	}
	buffer->flush();
	oboBuffer->flush();
	predicateBuffer->flush();

	predicateWriter = predicateBuffer->get_address();

	buffer->flush();
	oboBuffer->flush();
	tempByO->discard();
	delete tempByO;
	tempByO = NULL;
	//    logmapbuffer->templogbuffer->flush();

	if (buffer != NULL)
	{
		buffer->close();
		delete buffer;
		buffer = NULL;
	}
	if (oboBuffer != NULL)
	{
		oboBuffer->close();
		delete oboBuffer;
		oboBuffer = NULL;
	}

	predicateWriter = NULL;
	if (predicateBuffer != NULL)
	{
		predicateBuffer->close();
		delete predicateBuffer;
		predicateBuffer = NULL;
	}
}

void BitmapBuffer::save(int no)
{
	string filename = dir + "/BitmapBuffer";
	MMapBuffer *buffer;
	uchar *bufferWriter;
	//分order By S 和 Order By O两个存储数据
	string obsFile(filename);
	obsFile.append("_obs"+toStr(no));
	string oboFile(filename);
	oboFile.append("_obo"+toStr(no));
	MMapBuffer *obsBuffer, *oboBuffer;
	uchar *obsBufferWriter, *oboBufferWriter;
	//谓词索引
	string predicateFile(filename);
	predicateFile.append("_predicate"+toStr(no));
	MMapBuffer *predicateBuffer = new MMapBuffer(predicateFile.c_str(), predicate_managers[0].size() * (sizeof(ID) + sizeof(SOType) + sizeof(size_t) * 2) * 2);
	uchar *predicateWriter = predicateBuffer->get_address();

	map<ID, ChunkManager *>::const_iterator iter = predicate_managers[0].begin();
	size_t offset = 0;
	size_t offset2 = 0; //存order by o 的时候用的
	//之前的buffer
	buffer = new MMapBuffer(filename.c_str(), iter->second->meta->length);
	bufferWriter = buffer->get_address();
	// order by s 的buffer
	obsBuffer = new MMapBuffer(obsFile.c_str(), iter->second->meta->length);
	obsBufferWriter = obsBuffer->get_address();

	vector<size_t>::iterator pageNoIter = iter->second->usedPages.begin(), limit = iter->second->usedPages.end();
	int i = 0;
	for (; pageNoIter != limit; pageNoIter++)
	{
		size_t pageNo = *pageNoIter;
		//向buffer写数据
		memcpy(bufferWriter, tempByS->get_address() + pageNo * MemoryBuffer::pagesize, MemoryBuffer::pagesize);
		bufferWriter = bufferWriter + MemoryBuffer::pagesize;
		//向obsBuffer写数据
		memcpy(obsBufferWriter, tempByS->get_address() + pageNo * MemoryBuffer::pagesize, MemoryBuffer::pagesize);
		obsBufferWriter = obsBufferWriter + MemoryBuffer::pagesize;
	}

	*((ID *)predicateWriter) = iter->first;
	predicateWriter += sizeof(ID);
	*((SOType *)predicateWriter) = ORDERBYS;
	predicateWriter += sizeof(SOType);
	*((size_t *)predicateWriter) = offset;
	predicateWriter += sizeof(size_t) * 2; //第二个为chunkmanager的index offset

	offset += iter->second->meta->length;

	uchar *startPos = bufferWriter + offset;
	uchar *startPos_so = obsBufferWriter + offset;

	iter++;
	for (; iter != predicate_managers[0].end(); iter++)
	{
		//完整buffer
		bufferWriter = buffer->resize(iter->second->meta->length);
		startPos = bufferWriter + offset;
		// obsBuffer
		obsBufferWriter = obsBuffer->resize(iter->second->meta->length);
		startPos_so = obsBufferWriter + offset;

		pageNoIter = iter->second->usedPages.begin();
		limit = iter->second->usedPages.end();

		for (; pageNoIter != limit; pageNoIter++)
		{
			size_t pageNo = *pageNoIter;
			//向buffer写数据
			memcpy(startPos, tempByS->get_address() + pageNo * MemoryBuffer::pagesize, MemoryBuffer::pagesize);
			startPos += MemoryBuffer::pagesize;
			//向obsBuffer写数据
			memcpy(startPos_so, tempByS->get_address() + pageNo * MemoryBuffer::pagesize, MemoryBuffer::pagesize);
			startPos_so = startPos_so + MemoryBuffer::pagesize;
		}

		*((ID *)predicateWriter) = iter->first;
		predicateWriter += sizeof(ID);
		*((SOType *)predicateWriter) = ORDERBYS;
		predicateWriter += sizeof(SOType);
		*((size_t *)predicateWriter) = offset;
		predicateWriter += sizeof(size_t) * 2;
		offset += iter->second->meta->length;

		assert(iter->second->usedPages.size() * MemoryBuffer::pagesize == iter->second->meta->length);
	}

	buffer->flush();
	obsBuffer->flush();
	tempByS->discard();
	delete tempByS;
	tempByS = NULL;
	if (obsBuffer != NULL)
	{
		obsBuffer->close();
		delete obsBuffer;
		obsBuffer = NULL;
	}

	// oder bys end  ..  oder by o start
	iter = predicate_managers[1].begin();
	bufferWriter = buffer->resize(iter->second->meta->length);
	bufferWriter = bufferWriter + offset;
	// oder by o的buffer
	oboBuffer = new MMapBuffer(oboFile.c_str(), iter->second->meta->length);
	oboBufferWriter = oboBuffer->get_address();

	pageNoIter = iter->second->usedPages.begin();
	limit = iter->second->usedPages.end();

	for (; pageNoIter != limit; pageNoIter++)
	{

		size_t pageNo = *pageNoIter;
		//向buffer写数据

		memcpy(bufferWriter, tempByO->get_address() + pageNo * MemoryBuffer::pagesize, MemoryBuffer::pagesize);
		bufferWriter = bufferWriter + MemoryBuffer::pagesize;

		//向obsBuffer写数据
		memcpy(oboBufferWriter, tempByO->get_address() + pageNo * MemoryBuffer::pagesize, MemoryBuffer::pagesize);
		oboBufferWriter = oboBufferWriter + MemoryBuffer::pagesize;
	}

	*((ID *)predicateWriter) = iter->first;
	predicateWriter += sizeof(ID);
	*((SOType *)predicateWriter) = ORDERBYO;
	predicateWriter += sizeof(SOType);
	*((size_t *)predicateWriter) = offset;
	predicateWriter += sizeof(size_t) * 2; //第二个为chunkmanager的index offset

	offset += iter->second->meta->length;
	offset2 += iter->second->meta->length;

	iter++;
	for (; iter != predicate_managers[1].end(); iter++)
	{
		bufferWriter = buffer->resize(iter->second->meta->length);
		startPos = bufferWriter + offset;
		// oboBuffer
		oboBufferWriter = oboBuffer->resize(iter->second->meta->length);
		startPos_so = oboBufferWriter + offset2;

		pageNoIter = iter->second->usedPages.begin();
		limit = iter->second->usedPages.end();
		for (; pageNoIter != limit; pageNoIter++)
		{
			size_t pageNo = *pageNoIter;
			memcpy(startPos, tempByO->get_address() + pageNo * MemoryBuffer::pagesize, MemoryBuffer::pagesize);
			startPos += MemoryBuffer::pagesize;

			//向oboBuffer写数据
			memcpy(startPos_so, tempByO->get_address() + pageNo * MemoryBuffer::pagesize, MemoryBuffer::pagesize);
			startPos_so = startPos_so + MemoryBuffer::pagesize;
		}

		*((ID *)predicateWriter) = iter->first;
		predicateWriter += sizeof(ID);
		*((SOType *)predicateWriter) = ORDERBYO;
		predicateWriter += sizeof(SOType);
		*((size_t *)predicateWriter) = offset;
		predicateWriter += sizeof(size_t) * 2;
		offset += iter->second->meta->length;
		offset2 += iter->second->meta->length;

		assert(iter->second->meta->length == iter->second->usedPages.size() * MemoryBuffer::pagesize);
	}
	buffer->flush();
	oboBuffer->flush();
	predicateBuffer->flush();

	predicateWriter = predicateBuffer->get_address();

	buffer->flush();
	oboBuffer->flush();
	tempByO->discard();
	delete tempByO;
	tempByO = NULL;
	//    logmapbuffer->templogbuffer->flush();

	if (buffer != NULL)
	{
		buffer->close();
		delete buffer;
		buffer = NULL;
	}
	if (oboBuffer != NULL)
	{
		oboBuffer->close();
		delete oboBuffer;
		oboBuffer = NULL;
	}

	predicateWriter = NULL;
	if (predicateBuffer != NULL)
	{
		predicateBuffer->close();
		delete predicateBuffer;
		predicateBuffer = NULL;
	}
}

BitmapBuffer *BitmapBuffer::load(MMapBuffer *bitmapImage, MMapBuffer *bitmapPredicateImage)
{
	BitmapBuffer *buffer = new BitmapBuffer();
	uchar *predicateReader = bitmapPredicateImage->get_address();

	ID id;
	SOType soType;
	size_t offset = 0, indexOffset = 0, predicateOffset = 0;
	size_t sizePredicateBuffer = bitmapPredicateImage->get_length();

	while (predicateOffset < sizePredicateBuffer)
	{
		id = *(ID *)predicateReader;
		predicateReader += sizeof(ID); //取PID
		soType = *(SOType *)predicateReader;
		predicateReader += sizeof(SOType); //取soType
		offset = *(size_t *)predicateReader;
		predicateReader += sizeof(size_t); //取偏移
		indexOffset = *(size_t *)predicateReader;
		predicateReader += sizeof(size_t);
		if (soType == ORDERBYS)
		{
			ChunkManager *manager = ChunkManager::load(id, ORDERBYS, bitmapImage->get_address(), offset);	//加载chunkManager
			manager->chunkIndex = LineHashIndex::load(*manager, LineHashIndex::SUBJECT_INDEX, indexOffset); //新建索引
			buffer->predicate_managers[0][id] = manager;
		}
		else if (soType == ORDERBYO)
		{
			ChunkManager *manager = ChunkManager::load(id, ORDERBYO, bitmapImage->get_address(), offset);
			manager->chunkIndex = LineHashIndex::load(*manager, LineHashIndex::OBJECT_INDEX, indexOffset);
			buffer->predicate_managers[1][id] = manager;
		}
		predicateOffset += sizeof(ID) + sizeof(SOType) + sizeof(size_t) * 2;
	}

	return buffer;
}

BitmapBuffer *BitmapBuffer::load_so(MMapBuffer *bitmapImage_s, MMapBuffer *bitmapImage_o, MMapBuffer *bitmapPredicateImage)
{
	BitmapBuffer *buffer = new BitmapBuffer();
	uchar *predicateReader = bitmapPredicateImage->get_address();
	buffer->tempByS = bitmapImage_s; buffer->tempByO = bitmapImage_o;

	ID id;
	int firsto = 0;
	SOType soType;
	size_t offset = 0, offset1 = 0, offset2 = 0, indexOffset = 0, predicateOffset = 0;
	size_t sizePredicateBuffer = bitmapPredicateImage->get_length();

	while (predicateOffset < sizePredicateBuffer)
	{
		id = *(ID *)predicateReader;
		predicateReader += sizeof(ID); //取PID
		soType = *(SOType *)predicateReader;
		predicateReader += sizeof(SOType); //取soType
		offset = *(size_t *)predicateReader;
		predicateReader += sizeof(size_t); //取偏移
		indexOffset = *(size_t *)predicateReader;
		predicateReader += sizeof(size_t);
		if (soType == ORDERBYS)
		{
			offset1 = offset;
			ChunkManager *manager = ChunkManager::load(id, ORDERBYS, bitmapImage_s->get_address(), offset1); //加载chunkManager
			manager->chunkIndex = LineHashIndex::load(*manager, LineHashIndex::SUBJECT_INDEX, indexOffset);	 //新建索引
			manager->bitmapBuffer = buffer;
			buffer->predicate_managers[0][id] = manager;
		}
		else if (soType == ORDERBYO)
		{
			if (firsto == 0)
			{
				offset2 = offset;
				firsto = 1;
			}
			offset1 = offset - offset2;
			ChunkManager *manager = ChunkManager::load(id, ORDERBYO, bitmapImage_o->get_address(), offset1);
			manager->chunkIndex = LineHashIndex::load(*manager, LineHashIndex::OBJECT_INDEX, indexOffset);
			manager->bitmapBuffer = buffer;
			buffer->predicate_managers[1][id] = manager;
		}
		predicateOffset += sizeof(ID) + sizeof(SOType) + sizeof(size_t) * 2;
	}

	return buffer;
}



void BitmapBuffer::endUpdate(MMapBuffer *bitmapPredicateImage, MMapBuffer *bitmapOld)
{
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

	while (predicateOffset < sizePredicateBuffer)
	{
		bufferWriter = buffer->get_address() + offsetPage * MemoryBuffer::pagesize;
		lastoffsetPage = offsetPage;
		bufferWriterBegin = bufferWriter;

		id = *(ID *)predicateReader;
		predicateReader += sizeof(ID);
		soType = *(SOType *)predicateReader;
		predicateReader += sizeof(SOType);
		offset = *(size_t *)predicateReader;
		*((size_t *)predicateReader) = bufferWriterBegin - buffer->get_address();
		predicateReader += sizeof(size_t);
		predicateReader += sizeof(size_t); // skip the indexoffset

		startPtr = predicate_managers[soType][id]->getStartPtr();
		offsetId = 0;
		tableSize = predicate_managers[soType][id]->getChunkNumber();
		metaData = (MetaData *)startPtr;

		chunkBegin = startPtr - sizeof(ChunkManagerMeta);
		chunkManagerBegin = chunkBegin;
		memcpy(bufferWriter, chunkBegin, MemoryBuffer::pagesize);
		offsetPage++;
		metaDataNew = (MetaData *)(bufferWriterBegin + sizeof(ChunkManagerMeta));
		metaDataNew->NextPageNo = 0;

		while (metaData->NextPageNo != 0)
		{
			chunkBegin = TempMMapBuffer::getInstance().getAddress() + (metaData->NextPageNo - 1) * MemoryBuffer::pagesize;
			metaData = (MetaData *)chunkBegin;
			if (metaData->usedSpace == sizeof(MetaData))
				break;
			buffer->resize(MemoryBuffer::pagesize);
			bufferWriter = buffer->get_address() + offsetPage * MemoryBuffer::pagesize;
			memcpy(bufferWriter, chunkBegin, MemoryBuffer::pagesize);
			offsetPage++;
			metaDataNew = (MetaData *)bufferWriter;
			metaDataNew->NextPageNo = 0;
		}
		offsetId++;
		while (offsetId < tableSize)
		{
			buffer->resize(MemoryBuffer::pagesize);
			bufferWriter = buffer->get_address() + offsetPage * MemoryBuffer::pagesize;
			chunkBegin = chunkManagerBegin + offsetId * MemoryBuffer::pagesize;
			metaData = (MetaData *)chunkBegin;
			memcpy(bufferWriter, chunkBegin, MemoryBuffer::pagesize);
			offsetPage++;
			metaDataNew = (MetaData *)bufferWriter;
			metaDataNew->NextPageNo = 0;
			while (metaData->NextPageNo != 0)
			{
				chunkBegin = TempMMapBuffer::getInstance().getAddress() + metaData->NextPageNo * MemoryBuffer::pagesize;
				metaData = (MetaData *)chunkBegin;
				if (metaData->usedSpace == sizeof(MetaData))
					break;
				buffer->resize(MemoryBuffer::pagesize);
				bufferWriter = buffer->get_address() + offsetPage * MemoryBuffer::pagesize;
				memcpy(bufferWriter, chunkBegin, MemoryBuffer::pagesize);
				offsetPage++;
				metaDataNew = (MetaData *)bufferWriter;
				metaDataNew->NextPageNo = 0;
			}
			offsetId++;
		}

		bufferWriterEnd = buffer->get_address() + offsetPage * MemoryBuffer::pagesize;
		bufferWriterBegin = buffer->get_address() + lastoffsetPage * MemoryBuffer::pagesize;
		if (offsetPage == lastoffsetPage + 1)
		{
			ChunkManagerMeta *meta = (ChunkManagerMeta *)(bufferWriterBegin);
			MetaData *metaDataTemp = (MetaData *)(bufferWriterBegin + sizeof(ChunkManagerMeta));
			meta->usedSpace = metaDataTemp->usedSpace;
			meta->length = MemoryBuffer::pagesize;
		}
		else
		{
			ChunkManagerMeta *meta = (ChunkManagerMeta *)(bufferWriterBegin);
			MetaData *metaDataTemp = (MetaData *)(bufferWriterEnd - MemoryBuffer::pagesize);
			meta->usedSpace = bufferWriterEnd - bufferWriterBegin - sizeof(ChunkManagerMeta) - MemoryBuffer::pagesize + metaDataTemp->usedSpace;
			meta->length = bufferWriterEnd - bufferWriterBegin;
			assert(meta->length % MemoryBuffer::pagesize == 0);
		}
		buffer->flush();

		// not update the LineHashIndex
		predicateOffset += sizeof(ID) + sizeof(SOType) + sizeof(size_t) * 2;
	}

	predicateOffset = 0;
	predicateReader = bitmapPredicateImage->get_address();
	while (predicateOffset < sizePredicateBuffer)
	{
		id = *(ID *)predicateReader;
		predicateReader += sizeof(ID);
		soType = *(SOType *)predicateReader;
		predicateReader += sizeof(SOType);
		offset = *(size_t *)predicateReader;
		predicateReader += sizeof(size_t);
		predicateReader += sizeof(size_t);

#ifdef TTDEBUG
		cout << "id:" << id << " soType:" << soType << endl;
		cout << "offset:" << offset << " indexOffset:" << predicateOffset << endl;
#endif

		uchar *base = buffer->get_address() + offset;
		ChunkManagerMeta *meta = (ChunkManagerMeta *)base;
		meta->startPtr = base + sizeof(ChunkManagerMeta);
		meta->endPtr = meta->startPtr + meta->usedSpace;

		predicate_managers[soType][id]->meta = meta;
		//		predicate_managers[soType][id]->buildChunkIndex();
		// predicate_managers[soType][id]->updateChunkIndex();

		predicateOffset += sizeof(ID) + sizeof(SOType) + sizeof(size_t) * 2;
	}
	buffer->flush();

	string bitmapNameOld = dir + "/BitmapBuffer";
	//	MMapBuffer *bufferOld = new MMapBuffer(bitmapNameOld.c_str(), 0);
	bitmapOld->discard();
	if (rename(bitmapName.c_str(), bitmapNameOld.c_str()) != 0)
	{
		perror("rename bitmapName error!");
	}
}
MetaData *BitmapBuffer::get_Chunk_byChunkID(OrderByType SOType, ID pid, int ChunkID)
{
	auto temp = predicate_managers[SOType][pid];
	uchar *starPtr;
	starPtr = (uchar *)temp->getChunkManagerMeta();
	MetaData *result;
	if (ChunkID == 1)
	{
		result = (MetaData *)(starPtr + sizeof(ChunkManagerMeta));
	}
	else
	{
		result = (MetaData *)(starPtr + (ChunkID - 1) * MemoryBuffer::pagesize);
	}

	return result;
}

void BitmapBuffer::insert_new_data(ID subjectID, ID predicateID, ID objectID){
	predicate_managers[0][predicateID]->insert_new_data(ORDERBYS, subjectID, objectID);
	predicate_managers[1][predicateID]->insert_new_data(ORDERBYO, subjectID, objectID);
}



void BitmapBuffer::print(int f)
{
	for (const auto &e : predicate_managers[0])
	{
		e.second->print(f);
	}
}

MetaData* BitmapBuffer::getAppendChunkByPageNo(OrderByType soType, size_t pageno){
	if(soType==ORDERBYS){
		return (MetaData*)(appendByS->get_address()+(pageno-1)*MemoryBuffer::pagesize);
	}else{
		return (MetaData*)(appendByO->get_address()+(pageno-1)*MemoryBuffer::pagesize);
	}
}

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////

void getTempFilename(string &filename, unsigned pid, unsigned _type)
{
	filename.clear();
	filename.append(DATABASE_PATH);
	filename.append("temp_");
	char temp[5];
	sprintf(temp, "%d", pid);
	filename.append(temp);
	sprintf(temp, "%d", _type);
	filename.append(temp);
}

ChunkManager::ChunkManager(ID predicateID, OrderByType soType, BitmapBuffer *_bitmapBuffer) : bitmapBuffer(_bitmapBuffer)
{
	usedPages.resize(0);
	size_t pageNo = 0;
	meta = NULL;
	lastChunkStartPtr = bitmapBuffer->getPage(soType, pageNo);
	usedPages.push_back(pageNo);

	meta = (ChunkManagerMeta *)lastChunkStartPtr;
	memset((char *)meta, 0, sizeof(ChunkManagerMeta));
	meta->endPtr = meta->startPtr = lastChunkStartPtr + sizeof(ChunkManagerMeta);
	meta->length = usedPages.size() * MemoryBuffer::pagesize;
	meta->usedSpace = 0;
	meta->tripleCount = 0;
	meta->pid = predicateID;
	meta->soType = soType;

	pthread_mutex_init(&mutex, NULL);

	chunkIndex = NULL;
}

ChunkManager::~ChunkManager()
{
	lastChunkStartPtr = NULL;
	if (chunkIndex != NULL)
		delete chunkIndex;
	chunkIndex = NULL;
	pthread_mutex_destroy(&mutex);
}

void ChunkManager::writeT(uchar *&reader, ID time)
{
	Chunk::writeT(reader, time);
}

void ChunkManager::write(uchar *&reader, ID subjectID, ID objectID)
{
	Chunk::writeID(reader, subjectID);
	Chunk::writeID(reader, objectID);
}

void ChunkManager::read(const uchar *&reader, ID &subjectID, ID &objectID)
{
	Chunk::readID(reader, subjectID);
	Chunk::readID(reader, objectID);
}

void ChunkManager::deleteTriple(uchar *&reader)
{
	Chunk::deleteData(reader);
}

void ChunkManager::findObjectBySubjectAndPredicate(ID subjectID, set<vector<ID>> &results, vector<ID> &max_min, vector<ID> &tmp_max_min){
	// cout<<subjectID<<endl;
	//找到对应块
	uchar *start = (uchar*)meta, *limit = meta->endPtr, *reader = start;
	
	while(reader<limit){
		MetaData *datameta;
		if(reader == start){
			datameta = (MetaData*)meta->startPtr;
		}else{
			datameta = (MetaData*)reader;
		}
		if(datameta->min<=subjectID&&datameta->max>=subjectID){
			// cout<<datameta->min<<" "<<datameta->max<<endl;
			MetaData *appmeta = datameta;
			while(appmeta->NextPageNo!=-1){
				uchar* scan = (uchar*)appmeta+sizeof(MetaData), *scan_end = (uchar*)appmeta+appmeta->usedSpace;
				while(scan<scan_end){
					ID sID, oID;
					sID = *(ID*)scan;
					scan+=sizeof(ID);
					oID = *(ID*)scan;
					scan+=sizeof(ID);
					if(sID==subjectID){
						// cout<<oID<<endl;
						if(oID>=max_min[0]&&oID<=max_min[1]){
							results.insert({oID});
							tmp_max_min[0] = tmp_max_min[0]<oID?tmp_max_min[0]:oID;
							tmp_max_min[1] = tmp_max_min[1]>oID?tmp_max_min[1]:oID;
						}
					}
				}
				appmeta = bitmapBuffer->getAppendChunkByPageNo(ORDERBYS,appmeta->NextPageNo);
			}
			uchar* scan = (uchar*)appmeta+sizeof(MetaData), *scan_end = (uchar*)appmeta+appmeta->usedSpace;
			while(scan<scan_end){
				ID sID, oID;
				sID = *(ID*)scan;
				scan+=sizeof(ID);
				oID = *(ID*)scan;
				scan+=sizeof(ID);
				if(sID==subjectID){
					// cout<<oID<<endl;
					if(oID>=max_min[0]&&oID<=max_min[1]){
						results.insert({oID});
						tmp_max_min[0] = tmp_max_min[0]<oID?tmp_max_min[0]:oID;
						tmp_max_min[1] = tmp_max_min[1]>oID?tmp_max_min[1]:oID;
					}
				}
			}
			
		}else if(datameta->min>subjectID){
			break;
		}
		reader += MemoryBuffer::pagesize;
	}
}
void ChunkManager::findSubjectByObjectAndPredicate(ID objectID, set<vector<ID>> &results, vector<ID> &max_min, vector<ID> &tmp_max_min){
	// cout<<objectID<<endl;
	//找到对应块
	uchar *start = (uchar*)meta, *limit = meta->endPtr, *reader = start;
	
	while(reader<limit){
		MetaData *datameta;
		if(reader == start){
			datameta = (MetaData*)meta->startPtr;
		}else{
			datameta = (MetaData*)reader;
		}
		if(datameta->min<=objectID&&datameta->max>=objectID){
			// cout<<datameta->min<<" "<<datameta->max<<endl;
			MetaData *appmeta = datameta;
			while(appmeta->NextPageNo!=-1){
				uchar* scan = (uchar*)appmeta+sizeof(MetaData), *scan_end = (uchar*)appmeta+appmeta->usedSpace;
				while(scan<scan_end){
					ID sID, oID;
					sID = *(ID*)scan;
					scan+=sizeof(ID);
					oID = *(ID*)scan;
					scan+=sizeof(ID);
					if(oID==objectID){
						// cout<<oID<<endl;
						if(sID>=max_min[0]&&sID<=max_min[1]){
							results.insert({sID});
							tmp_max_min[0] = tmp_max_min[0]<sID?tmp_max_min[0]:sID;
							tmp_max_min[1] = tmp_max_min[1]>sID?tmp_max_min[1]:sID;
						}
					}
				}
				appmeta = bitmapBuffer->getAppendChunkByPageNo(ORDERBYO,appmeta->NextPageNo);
			}
			uchar* scan = (uchar*)appmeta+sizeof(MetaData), *scan_end = (uchar*)appmeta+appmeta->usedSpace;
			while(scan<scan_end){
				ID sID, oID;
				sID = *(ID*)scan;
				scan+=sizeof(ID);
				oID = *(ID*)scan;
				scan+=sizeof(ID);
				if(oID==objectID){
						// cout<<oID<<endl;
					if(sID>=max_min[0]&&sID<=max_min[1]){
						results.insert({sID});
						tmp_max_min[0] = tmp_max_min[0]<sID?tmp_max_min[0]:sID;
						tmp_max_min[1] = tmp_max_min[1]>sID?tmp_max_min[1]:sID;
					}
				}
			}
			
		}else if(datameta->min>objectID){
			break;
		}
		reader += MemoryBuffer::pagesize;
	}
}

void ChunkManager::findSubjectAndObjectByPredicate(set<vector<ID>> &results, vector<ID> &max_min1, vector<ID> &max_min2, vector<ID> &tmp_max_min1, vector<ID> &tmp_max_min2){
	uchar *start = (uchar*)meta, *limit = meta->endPtr, *reader = start;
	while(reader<limit){
		MetaData *datameta;
		if(reader == start){
			datameta = (MetaData*)meta->startPtr;
		}else{
			datameta = (MetaData*)reader;
		}
		//扫描附加块
		MetaData *appmeta = datameta;
		while(appmeta->NextPageNo!=-1){
			uchar* scan = (uchar*)appmeta+sizeof(MetaData), *scan_end = (uchar*)appmeta+appmeta->usedSpace;
			while(scan<scan_end){
				ID sID, oID;
				sID = *(ID*)scan;
				scan+=sizeof(ID);
				oID = *(ID*)scan;
				scan+=sizeof(ID);
				if(sID>=max_min1[0]&&sID<=max_min1[1]&&oID>=max_min2[0]&&oID<=max_min2[1]){
					results.insert({sID,oID});
					tmp_max_min1[0] = tmp_max_min1[0]<sID?tmp_max_min1[0]:sID;
					tmp_max_min1[1] = tmp_max_min1[1]>sID?tmp_max_min1[1]:sID;
					tmp_max_min2[0] = tmp_max_min2[0]<oID?tmp_max_min2[0]:oID;
					tmp_max_min2[1] = tmp_max_min2[1]>oID?tmp_max_min2[1]:oID;
				}
			}
			appmeta = bitmapBuffer->getAppendChunkByPageNo(ORDERBYO,appmeta->NextPageNo);
		}
		//扫描最后一个附加块
		uchar* scan = (uchar*)appmeta+sizeof(MetaData), *scan_end = (uchar*)appmeta+appmeta->usedSpace;
		while(scan<scan_end){
			ID sID, oID;
			sID = *(ID*)scan;
			scan+=sizeof(ID);
			oID = *(ID*)scan;
			scan+=sizeof(ID);
			if(sID>=max_min1[0]&&sID<=max_min1[1]&&oID>=max_min2[0]&&oID<=max_min2[1]){
				results.insert({sID,oID});
				tmp_max_min1[0] = tmp_max_min1[0]<sID?tmp_max_min1[0]:sID;
				tmp_max_min1[1] = tmp_max_min1[1]>sID?tmp_max_min1[1]:sID;
				tmp_max_min2[0] = tmp_max_min2[0]<oID?tmp_max_min2[0]:oID;
				tmp_max_min2[1] = tmp_max_min2[1]>oID?tmp_max_min2[1]:oID;
			}
		}
		reader += MemoryBuffer::pagesize;
	}
}

void ChunkManager::set_next_log_no(int chunk_no, int log_no)
{
	uchar *metastart;
	if (chunk_no == 1)
	{
		metastart = (uchar *)meta->startPtr;
	}
	else
	{
		metastart = (uchar *)(meta->startPtr - sizeof(ChunkManagerMeta) + (chunk_no - 1) * MemoryBuffer::pagesize);
	}
	MetaData *meta = (MetaData *)metastart;
	// meta->nextlogChunkID = log_no;
	// cout<<meta->ChunkID<<endl;
}

//向chunkManager插入一条数据<S,O,T>
void ChunkManager::insert(ID subjectID, ID objectID)
{
	uint len = sizeof(ID) * 2;
	uchar *writePtr;
	//需要申请新的块
	if (isChunkOverFlow(len) == true)
	{
		//重新申请块
		size_t pageNo;
		resize(pageNo);
		//写入数据
		writePtr = meta->endPtr + sizeof(MetaData);
		write(writePtr, subjectID, objectID);
		//初始化metaData
		MetaData *metaData = (MetaData *)(meta->endPtr);
		metaData->pageNo = -1; //最后一块是原始块
		metaData->PID = this->meta->pid;
		if(getChunkManagerMeta()->soType==ORDERBYS){
			metaData->max = subjectID;
			metaData->min = subjectID;
		}else if(getChunkManagerMeta()->soType==ORDERBYO){
			metaData->max = objectID;
			metaData->min = objectID;
		}

		metaData->NextPageNo = -1; //-1表示没有下一块
		metaData->usedSpace = sizeof(MetaData) + len;
		// metaData->snapshotNo = 0;
		// metaData->snapshotPageSize = 0;
		metaData->maxTime = 1;
		metaData->updateCount = 0;
		metaData->tripleCount = 1;
		//更新meta
		meta->endPtr = meta->endPtr + sizeof(MetaData) + len;
		meta->usedSpace = meta->length - (MemoryBuffer::pagesize - sizeof(MetaData) - len) - sizeof(ChunkManagerMeta);
	}
	//不需要申请新的块
	//第一次添加 需要初始化metadata
	else if (meta->usedSpace == 0)
	{
		//写入数据
		writePtr = meta->startPtr + sizeof(MetaData);
		write(writePtr, subjectID, objectID);
		//初始化metaData
		MetaData *metaData = (MetaData *)(meta->startPtr);
		metaData->NextPageNo = -1;
		metaData->pageNo = -1;
		metaData->PID = this->meta->pid;
		if(getChunkManagerMeta()->soType==ORDERBYS){
			metaData->max = subjectID;
			metaData->min = subjectID;
		}else if(getChunkManagerMeta()->soType==ORDERBYO){
			metaData->max = objectID;
			metaData->min = objectID;
		}

		metaData->updateCount = 0;
		metaData->usedSpace = sizeof(MetaData) + len;
		// metaData->snapshotNo = 0;
		// metaData->snapshotPageSize = 0;
		metaData->maxTime = 1;
		metaData->tripleCount = 1;
		//更新meta
		meta->endPtr = meta->endPtr + sizeof(MetaData) + len;
		meta->usedSpace = meta->usedSpace + sizeof(MetaData) + len;
	}
	//不需要申请新的块
	// 直接添加
	else
	{
		//写入数据
		writePtr = meta->endPtr;
		write(writePtr, subjectID, objectID);
		//更新metaDate
		MetaData *metaData;
		if (meta->length == MemoryBuffer::pagesize)
		{
			metaData = (MetaData *)(meta->endPtr - meta->usedSpace);
		}
		else
		{
			size_t usedPage = MemoryBuffer::pagesize - (meta->length - meta->usedSpace - sizeof(ChunkManagerMeta));
			metaData = (MetaData *)(meta->endPtr - usedPage);
		}

		metaData->usedSpace = metaData->usedSpace + len;
		metaData->tripleCount++;
		//更新meta
		meta->endPtr = meta->endPtr + len;
		meta->usedSpace = meta->usedSpace + len;
		if(getChunkManagerMeta()->soType==ORDERBYS){
			metaData->max = subjectID;
		}else if(getChunkManagerMeta()->soType==ORDERBYO){
			metaData->max = objectID;
		}
	}
}
void ChunkManager::insert(ID subjectID, ID objectID, ID time)
{
	uint len = sizeof(ID) * 2;
	uint len2 = sizeof(ID) + sizeof(ID);
	uchar *writePtr;
	//需要申请新的块
	if (isChunkOverFlow(len) == true)
	{
		//重新申请块
		size_t pageNo;
		resize(pageNo);
		//写入数据
		writePtr = meta->endPtr + sizeof(MetaData);
		writeT(writePtr, time);
		write(writePtr, subjectID, objectID);
		//初始化metaData
		MetaData *metaData = (MetaData *)(meta->endPtr);
		metaData->pageNo = -1; //最后一块是原始块
		metaData->PID = this->meta->pid;
		metaData->ChunkID = usedPages.size();
		metaData->max = 0;
		if (this->meta->soType == ORDERBYS)
		{
			metaData->min = subjectID;
			metaData->max = subjectID > metaData->max ? subjectID : metaData->max;
		}
		else if (this->meta->soType == ORDERBYO)
		{
			metaData->min = objectID;
			metaData->max = objectID > metaData->max ? objectID : metaData->max;
		}
		metaData->NextPageNo = -1; //-1表示没有下一块
		metaData->usedSpace = sizeof(MetaData) + len + len2;
		// metaData->snapshotNo = 0;
		// metaData->snapshotPageSize = 1;
		metaData->maxTime = 1;
		metaData->updateCount = 0;
		metaData->tripleCount = 1;
		// metaData->nextlogChunkID = -1;
		// metaData->nextsnapshotChunknno = -1;
		//更新meta
		meta->endPtr = meta->endPtr + sizeof(MetaData) + len + len2;
		meta->usedSpace = meta->length - (MemoryBuffer::pagesize - sizeof(MetaData) - len - len2) - sizeof(ChunkManagerMeta);
	}
	//不需要申请新的块
	//第一次添加 需要初始化metadata
	else if (meta->usedSpace == 0)
	{
		//写入数据
		writePtr = meta->startPtr + sizeof(MetaData);
		writeT(writePtr, time);
		write(writePtr, subjectID, objectID);
		//初始化metaData
		MetaData *metaData = (MetaData *)(meta->startPtr);
		metaData->NextPageNo = -1;
		metaData->pageNo = -1;
		metaData->PID = this->meta->pid;
		metaData->ChunkID = usedPages.size();
		metaData->max = 0;
		if (this->meta->soType == ORDERBYS)
		{
			metaData->min = subjectID;
			metaData->max = subjectID > metaData->max ? subjectID : metaData->max;
		}
		else if (this->meta->soType == ORDERBYO)
		{
			metaData->min = objectID;
			metaData->max = objectID > metaData->max ? objectID : metaData->max;
		}
		metaData->updateCount = 0;
		metaData->usedSpace = sizeof(MetaData) + len + len2;
		// metaData->snapshotNo = 0;
		// metaData->snapshotPageSize = 1;
		metaData->maxTime = 1;
		metaData->tripleCount = 1;
		// metaData->nextlogChunkID = -1;
		// metaData->nextsnapshotChunknno = -1;
		//更新meta
		meta->endPtr = meta->endPtr + sizeof(MetaData) + len + len2;
		meta->usedSpace = meta->usedSpace + sizeof(MetaData) + len + len2;
	}
	//不需要申请新的块
	// 直接添加
	else
	{
		//写入数据
		writePtr = meta->endPtr;
		write(writePtr, subjectID, objectID);
		//更新metaDate
		MetaData *metaData;
		if (meta->length == MemoryBuffer::pagesize)
		{
			metaData = (MetaData *)(meta->endPtr - meta->usedSpace);
		}
		else
		{
			size_t usedPage = MemoryBuffer::pagesize - (meta->length - meta->usedSpace - sizeof(ChunkManagerMeta));
			metaData = (MetaData *)(meta->endPtr - usedPage);
		}
		if (this->meta->soType == ORDERBYS)
		{
			metaData->max = subjectID > metaData->max ? subjectID : metaData->max;
		}
		else if (this->meta->soType == ORDERBYO)
		{
			metaData->max = objectID > metaData->max ? objectID : metaData->max;
		}
		metaData->usedSpace = metaData->usedSpace + len;
		metaData->tripleCount++;
		//更新meta
		meta->endPtr = meta->endPtr + len;
		meta->usedSpace = meta->usedSpace + len;
	}
}
Status ChunkManager::resize(size_t &pageNo)
{
	lastChunkStartPtr = bitmapBuffer->getPage(meta->soType, pageNo);
	usedPages.push_back(pageNo);
	meta->length = usedPages.size() * MemoryBuffer::pagesize;
	meta->endPtr = lastChunkStartPtr;

	return OK;
}

bool ChunkManager::isChunkOverFlow(uint len)
{
	return sizeof(ChunkManagerMeta) + meta->usedSpace + len > meta->length;
}

void ChunkManager::setMetaDataMin(MetaData *metaData, ID x, double y)
{
	if (meta->soType == ORDERBYS)
	{
		metaData->min = x;
		metaData->max = x;
	}
	else if (meta->soType == ORDERBYO)
	{
		metaData->min = y;
		metaData->max = y;
	}
}

size_t ChunkManager::getChunkNumber()
{
	return (meta->length) / (MemoryBuffer::pagesize);
}
int ChunkManager::getChunkID(int orderType, ID sid, ID oid)
{
	ID id;
	if (orderType == 0)
	{
		id = sid;
	}
	else if (orderType == 1)
	{
		id = oid;
	}
	int ChunkNo = 0;
	if (this != NULL)
	{
		ChunkManagerMeta *meta = this->getChunkManagerMeta();
		MetaData *metadata;
		int pagesize = this->getChunkNumber();
		int low = 0;
		int high = pagesize - 1;
		int middle = (low + high) / 2;
		while (low <= high)
		{
			middle = (low + high) / 2;
			// cout<<low<<" "<<middle<<" "<<high<<endl;
			if (middle == 0)
			{
				metadata = (MetaData *)((uchar *)meta + sizeof(ChunkManagerMeta));
			}
			else
			{
				metadata = (MetaData *)((uchar *)meta + middle * MemoryBuffer::pagesize);
			}
			if ((id >= metadata->min && id <= metadata->max))
			{
				ChunkNo = metadata->ChunkID;
				break;
			}
			else if (id < metadata->min)
			{
				high = middle-1;
			}
			else if (id > metadata->max)
			{
				low = middle+1;
			}
		}
	
		if(ChunkNo==0){
			ChunkNo = pagesize;
		}
		// for (int i = 0; i < pagesize; i++)
		// {
		// 	if (i == 0)
		// 	{
		// 		metadata = (MetaData *)((uchar *)meta + sizeof(ChunkManagerMeta));
		// 	}
		// 	else
		// 	{
		// 		metadata = (MetaData *)((uchar *)meta + i * MemoryBuffer::pagesize);
		// 	}
		// 	if ((id >= metadata->min && id <= metadata->max) || i == pagesize - 1)
		// 	{
		// 		ChunkNo = metadata->ChunkID;
		// 		break;
		// 	}
		// 	else
		// 	{
		// 		// continue;
		// 		// ChunkNo = i + 1;
		// 	}
		// }
	}
	else
	{
		cout << "没有pid" << endl;
		return -1;
	}
	return ChunkNo;
}

void ChunkManager::insert_new_data(OrderByType orderType, ID subjectID, ID objectID){
	uchar *reader = meta->startPtr, *limit = meta->endPtr;
	MetaData *metadata = (MetaData*)reader, *lastmetadata=NULL;
	uchar *writePtr;
	bool flag = 0;
	if(orderType==ORDERBYS){
		while(reader<limit){
			if(metadata->min<=subjectID&&metadata->max>=subjectID){
				flag = 1;
				break;
			}else if(lastmetadata&&subjectID>lastmetadata->max&&subjectID<metadata->min){
				flag = 1;
				break;
			}
			else if(metadata->max<subjectID){

			}else if(metadata->min>subjectID){
				
			}
			
			if(metadata==(MetaData*)meta->startPtr){
				reader += (MemoryBuffer::pagesize-sizeof(ChunkManagerMeta));
			}else{
				reader += MemoryBuffer::pagesize;
			}
			lastmetadata = metadata;
			metadata = (MetaData*)reader;
		}
		if(flag==0){
			reader -= MemoryBuffer::pagesize;
			if(reader!=(meta->startPtr-sizeof(ChunkManagerMeta))){
				metadata = (MetaData*)reader;
			}else{
				metadata = (MetaData*)meta->startPtr;
			}
			if(subjectID<metadata->min){
				// cout<<subjectID<<endl;
				metadata = (MetaData*)meta->startPtr;
				
			}
		}
// ((uchar*)metadata!=(meta->startPtr-sizeof(ChunkManagerMeta))&&metadata->usedSpace+len>MemoryBuffer::pagesize)||((uchar*)metadata==(meta->startPtr-sizeof(ChunkManagerMeta))&&metadata->usedSpace+len+sizeof(ChunkManagerMeta)>MemoryBuffer::pagesize)
		// cout<<"metadata->ChunkID "<<metadata->ChunkID<<endl;
		// cout<<"metadata->max "<<metadata->max<<endl;
		// cout<<"metadata->maxTime "<<metadata->maxTime<<endl;
		// cout<<"metadata->min "<<metadata->min<<endl;
		// cout<<"metadata->NextPageNo "<<metadata->NextPageNo<<endl;
		// cout<<"metadata->pageNo "<<metadata->pageNo<<endl;
		// cout<<"metadata->PID "<<metadata->PID<<endl;
		// cout<<"metadata->tripleCount "<<metadata->tripleCount<<endl;
		// cout<<"metadata->updateCount "<<metadata->updateCount<<endl;
		// cout<<"metadata->usedSpace "<<metadata->usedSpace<<endl;
		// cout<<"==============================="<<endl;
		uint len = sizeof(ID) * 2;
		if(((uchar*)metadata!=(meta->startPtr)&&metadata->usedSpace+len>MemoryBuffer::pagesize)||((uchar*)metadata==(meta->startPtr)&&metadata->usedSpace+len+sizeof(ChunkManagerMeta)>MemoryBuffer::pagesize)){
			MetaData *writermeta = metadata;
			
			if(metadata->NextPageNo==-1){
				bitmapBuffer->usedPageByaS++;
				size_t writepageno = bitmapBuffer->usedPageByaS;
				metadata->NextPageNo = writepageno;
				if(writepageno*MemoryBuffer::pagesize>bitmapBuffer->appendByS->getSize()){
					bitmapBuffer->appendByS->resize(MemoryBuffer::pagesize);
				}
				writermeta = bitmapBuffer->getAppendChunkByPageNo(orderType, writepageno);
				writermeta->pageNo = writepageno;
				writermeta->NextPageNo=-1;
				writermeta->usedSpace = sizeof(MetaData);
				writermeta->min = metadata->min;
				writermeta->max = metadata->max;
				writermeta->PID = metadata->PID;

			}else{
				//如果有下一块，找到最后一块
				if(writermeta->NextPageNo!=-1){
					while(writermeta->NextPageNo!=-1){
						writermeta = bitmapBuffer->getAppendChunkByPageNo(orderType, writermeta->NextPageNo);
					}
				}
				
				//如果最后一块满了，新申请块，在获得最后一块
				if(writermeta->usedSpace+len>MemoryBuffer::pagesize){
					bitmapBuffer->usedPageByaS++;
					size_t writepageno = bitmapBuffer->usedPageByaS;
					writermeta->NextPageNo = writepageno;
					if(writepageno*MemoryBuffer::pagesize>bitmapBuffer->appendByS->getSize()){
						bitmapBuffer->appendByS->resize(MemoryBuffer::pagesize);
					}
					writermeta = bitmapBuffer->getAppendChunkByPageNo(orderType, writepageno);
					writermeta->pageNo = writepageno;
					writermeta->NextPageNo=-1;
					writermeta->usedSpace = sizeof(MetaData);
					writermeta->min = metadata->min;
					writermeta->max = metadata->max;
					writermeta->PID = metadata->PID;
				}
				
				
				
			}
			// cout<<"metadata->pageNo "<<writermeta->pageNo<<endl;
			// cout<<"metadata->PID "<<writermeta->PID<<endl;
			// cout<<"metadata->tripleCount "<<writermeta->tripleCount<<endl;
			// cout<<"metadata->updateCount "<<writermeta->updateCount<<endl;
			// cout<<"metadata->usedSpace "<<writermeta->usedSpace<<endl;
			// cout<<"==============================="<<endl;
			writePtr = (uchar*)writermeta+writermeta->usedSpace;
			write(writePtr,subjectID,objectID);
			writermeta->max = subjectID>writermeta->max?subjectID:writermeta->max;
			writermeta->min = subjectID<writermeta->min?subjectID:writermeta->min;
			writermeta->usedSpace += len;
			writermeta->tripleCount++;
			metadata->max = subjectID>metadata->max?subjectID:metadata->max;
			metadata->min = subjectID<metadata->min?subjectID:metadata->min;
			
		}else{
			writePtr = meta->endPtr;
			write(writePtr, subjectID, objectID);
			metadata->usedSpace = metadata->usedSpace + len;
			metadata->tripleCount++;
			// 更新meta
			meta->endPtr = meta->endPtr + len;
			meta->usedSpace = meta->usedSpace + len;
			metadata->max = subjectID>metadata->max?subjectID:metadata->max;
			metadata->min = subjectID<metadata->min?subjectID:metadata->min;
		}
		

	}
	int kkk=0;
	if(orderType==ORDERBYO){
		while(reader<limit){
			if(metadata->min<=objectID&&metadata->max>=objectID){
				flag = 1;
				break;
			}else if(lastmetadata&&objectID>lastmetadata->max&&objectID<metadata->min){
				flag = 1;
				break;
			}
			else if(metadata->max<objectID){

			}else if(metadata->min>objectID){
				
			}
			
			if(metadata==(MetaData*)meta->startPtr){
				reader += (MemoryBuffer::pagesize-sizeof(ChunkManagerMeta));
			}else{
				reader += MemoryBuffer::pagesize;
			}
			lastmetadata = metadata;
			metadata = (MetaData*)reader;
		}
		if(flag==0){
			reader -= MemoryBuffer::pagesize;
			if(reader!=(meta->startPtr-sizeof(ChunkManagerMeta))){
				metadata = (MetaData*)reader;
			}else{
				metadata = (MetaData*)meta->startPtr;
			}
			if(objectID<metadata->min){
				
				metadata = (MetaData*)meta->startPtr;
				
			}
		}

		uint len = sizeof(ID) * 2;
		if(((uchar*)metadata!=(meta->startPtr)&&metadata->usedSpace+len>MemoryBuffer::pagesize)||((uchar*)metadata==(meta->startPtr)&&metadata->usedSpace+len+sizeof(ChunkManagerMeta)>MemoryBuffer::pagesize)){
			MetaData *writermeta = metadata;
			
			if(metadata->NextPageNo==-1){
				bitmapBuffer->usedPageByaO++;
				size_t writepageno = bitmapBuffer->usedPageByaO;
				metadata->NextPageNo = writepageno;
				if(writepageno*MemoryBuffer::pagesize>bitmapBuffer->appendByO->getSize()){
					bitmapBuffer->appendByO->resize(MemoryBuffer::pagesize);
				}
				writermeta = bitmapBuffer->getAppendChunkByPageNo(orderType, writepageno);
				writermeta->pageNo = writepageno;
				writermeta->NextPageNo=-1;
				writermeta->usedSpace = sizeof(MetaData);
				writermeta->min = metadata->min;
				writermeta->max = metadata->max;
				writermeta->PID = metadata->PID;

			}else{
				//如果有下一块，找到最后一块
				if(writermeta->NextPageNo!=-1){
					while(writermeta->NextPageNo!=-1){
						writermeta = bitmapBuffer->getAppendChunkByPageNo(orderType, writermeta->NextPageNo);
						// cout<<writermeta->NextPageNo<<endl;
					}
				}
				
				//如果最后一块满了，新申请块，在获得最后一块
				if(writermeta->usedSpace+len>MemoryBuffer::pagesize){
					bitmapBuffer->usedPageByaO++;
					size_t writepageno = bitmapBuffer->usedPageByaO;
					writermeta->NextPageNo = writepageno;
					if(writepageno*MemoryBuffer::pagesize>bitmapBuffer->appendByO->getSize()){
						bitmapBuffer->appendByO->resize(MemoryBuffer::pagesize);
					}
					writermeta = bitmapBuffer->getAppendChunkByPageNo(orderType, writepageno);
					writermeta->pageNo = writepageno;
					writermeta->NextPageNo=-1;
					writermeta->usedSpace = sizeof(MetaData);
					writermeta->min = metadata->min;
					writermeta->max = metadata->max;
					writermeta->PID = metadata->PID;
				}
				
				
				
			}

			writePtr = (uchar*)writermeta+writermeta->usedSpace;
			write(writePtr,subjectID,objectID);
			writermeta->max = objectID>writermeta->max?objectID:writermeta->max;
			writermeta->min = objectID<writermeta->min?objectID:writermeta->min;
			writermeta->usedSpace += len;
			writermeta->tripleCount++;
			metadata->max = objectID>metadata->max?objectID:metadata->max;
			metadata->min = objectID<metadata->min?objectID:metadata->min;
			
		}else{
			writePtr = meta->endPtr;
			write(writePtr, subjectID, objectID);
			metadata->usedSpace = metadata->usedSpace + len;
			metadata->tripleCount++;
			// 更新meta
			meta->endPtr = meta->endPtr + len;
			meta->usedSpace = meta->usedSpace + len;
			metadata->max = objectID>metadata->max?objectID:metadata->max;
			metadata->min = objectID<metadata->min?objectID:metadata->min;
		}
		
	}
}

void ChunkManager::readTripleID(ID *&S, ID &P, ID *&O, ID *&T, uchar *&reader)
{
	S = (ID *)reader;
	reader += sizeof(ID);
	O = (ID *)reader;
	reader += sizeof(ID);
	T = (ID *)reader;
	reader += sizeof(ID);
	P = getChunkManagerMeta()->pid;
}

Status ChunkManager::updateEdge(ID *origin, ID *new_id, int updatepos, TempFile &fact)
{
	ID *S, P, *O, *T;
	uchar *reader;
	uchar *limit;
	bool isupdate = 0;
	reader = getChunkManagerMeta()->startPtr;
	limit = getChunkManagerMeta()->endPtr;
	reader = reader + sizeof(MetaData);
	ChunkManagerMeta *cmeta = getChunkManagerMeta();
	MetaData *dmeta = (MetaData *)cmeta->startPtr;

	while (reader < limit)
	{
		//换页
		if ((reader - (uchar *)cmeta) % MemoryBuffer::pagesize == 0)
		{
			reader += sizeof(MetaData);
		}
		//第一个page有4B的空
		if (reader - (uchar *)cmeta == 4092)
		{
			reader = reader + 4 + sizeof(MetaData);
		}
		readTripleID(S, P, O, T, reader);
		if (*S == origin[0] && P == origin[1] && *O == origin[2])
		{
			cout << *S << " " << P << " " << *O << " " << *T << " 开始更新" << endl;
			switch (updatepos)
			{
			case 1:
				*S = new_id[0];
				isupdate = 1;
				cout << "已经更改S" << endl;
				break;
			case 3:
				*O = new_id[2];
				isupdate = 1;
				cout << "已经更改O" << endl;
				break;

			default:
				break;
			}
		}
		else
		{
			// cout<<*S<<" "<<P<<" "<<*O<<" "<<*T<<"写入facts"<<endl;
			//
		}
		// cout<<S<<" "<<P<<" "<<O<<" "<<T<<endl;
	}
	if (!isupdate)
	{
		return Fail;
	}
	return OK;
}

ChunkManager *ChunkManager::load(ID predicateID, bool soType, uchar *buffer, size_t &offset)
{
	ChunkManagerMeta *meta = (ChunkManagerMeta *)(buffer + offset);
	if (meta->pid != predicateID || meta->soType != soType)
	{
		MessageEngine::showMessage("load chunkmanager error: check meta info", MessageEngine::ERROR);
		cout << meta->pid << ": " << meta->soType << endl;
		return NULL;
	}

	ChunkManager *manager = new ChunkManager();
	uchar *base = buffer + offset + sizeof(ChunkManagerMeta);
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
	if (f == 1)
	{
		//打开日志文件
		ofstream out;
		out.open("chunkmanager1log.txt", ios::out | ios::app); //写   追加
		if (out.is_open())
		{
			// so副本
			if (meta->soType == ORDERBYS)
			{
				const uchar *reader;
				const uchar *limit;
				int writenum;
				out << "P:" << meta->pid << endl;
				bool f = true;
				MetaData *metaData;
				ID s, o;
				for (const auto &e : usedPages)
				{
					//数据块开始的位置
					reader = bitmapBuffer->tempByS->get_address() + e * MemoryBuffer::pagesize;
					if (f)
					{
						f = false;
						reader += sizeof(ChunkManagerMeta);
					}
					metaData = (MetaData *)(reader);
					out << "metaData->pageNo: " << metaData->pageNo << endl;
					out << "metaData->NextPageNo: " << metaData->NextPageNo << endl;
					out << "metaData->tripleCount: " << metaData->tripleCount << endl;
					out << "metaData->usedSpace: " << metaData->usedSpace << endl;
					limit = reader + metaData->usedSpace;
					reader = reader + sizeof(MetaData);
					writenum = 10;
					while (reader < limit)
					{
						read(reader, s, o);
						if (writenum)
						{
							out << s << " " << o << " " << endl;
							writenum--;
						}
					}
				}
			}
			// os副本
			else if (meta->soType == ORDERBYO)
			{
			}
			else
			{
				cout << "error" << endl;
			}
			//关闭文件
			out.close();
		}
		else
		{
			cout << "open file error" << endl;
		}
	}

	//插入
	if (f == 2)
	{
		//打开日志文件
		ofstream out;
		out.open("chunkmanager2log.txt", ios::out | ios::app); //写 追加
		if (out.is_open())
		{
			// so副本
			if (meta->soType == ORDERBYS)
			{
				const uchar *reader;
				const uchar *limit;
				int writenum;
				out << "P:" << meta->pid << endl;
				int chunkNum = meta->length / MemoryBuffer::pagesize;
				out << "Yuanshikuai shumu:" << chunkNum << endl;
				bool f = true;
				MetaData *metaData;
				ID s, o, t;
				int end;
				for (int i = 0; i < chunkNum; i++)
				{
					out << "yuanshikuai:" << i << endl;
					if (i == 0)
					{
						reader = meta->startPtr;
					}
					else
					{
						reader = meta->startPtr - sizeof(ChunkManagerMeta) + i * MemoryBuffer::pagesize;
					}
					metaData = (MetaData *)(reader);
					out << "metaData->pageNo: " << metaData->pageNo << endl;
					end = metaData->pageNo;
					out << "metaData->NextPageNo: " << metaData->NextPageNo << endl;
					out << "metaData->tripleCount: " << metaData->tripleCount << endl;
					out << "metaData->usedSpace: " << metaData->usedSpace << endl;
					limit = reader + metaData->usedSpace;
					reader = reader + sizeof(MetaData);
					writenum = 10;
					while (reader < limit)
					{
						read(reader, s, o);
						if (writenum)
						{
							out << s << " " << o << " " << endl;
							writenum--;
						}
					}
					if (end != -1)
					{
						while (metaData->NextPageNo != -1)
						{
							reader = reinterpret_cast<uchar *>(TempMMapBuffer::getInstance().getAddress()) + (metaData->NextPageNo) * MemoryBuffer::pagesize;
							metaData = (MetaData *)(reader);
							out << "metaData->pageNo: " << metaData->pageNo << endl;
							out << "metaData->NextPageNo: " << metaData->NextPageNo << endl;
							out << "metaData->tripleCount: " << metaData->tripleCount << endl;
							out << "metaData->usedSpace: " << metaData->usedSpace << endl;
							limit = reader + metaData->usedSpace;
							reader = reader + sizeof(MetaData);
							writenum = 10;
							while (reader < limit)
							{
								read(reader, s, o);
								if (writenum)
								{
									out << s << " " << o << " " << endl;
									writenum--;
								}
							}
							if (metaData->pageNo == end)
							{
								break;
							}
						}
					}
				}
			}
			// os副本
			else if (meta->soType == ORDERBYO)
			{
			}
			else
			{
				cout << "error" << endl;
			}
			//关闭文件
			out.close();
		}
		else
		{
			cout << "open file error" << endl;
		}
	}
}

/////////////////////////////////////////////////////////////////////////////////////////////////////////
Chunk::~Chunk()
{
	// TODO Auto-generated destructor stub
}

void Chunk::deleteData(uchar *&reader)
{
	Chunk::writeID(reader, 0);
}

uint Chunk::getLen(char dataType)
{
	int len;
	switch (dataType)
	{
	// case BOOL:
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

const uchar *Chunk::skipData(const uchar *reader, char dataType)
{
	return reader + getLen(dataType);
}

Status Chunk::getObjTypeStatus(const uchar *&reader, uint &moveByteNum)
{
	char objType = *(char *)reader;
	switch (objType)
	{
	/*case NONE:
		return DATA_NONE;
	case BOOL:*/
	case CHAR:
		moveByteNum = sizeof(char) + sizeof(char);
		reader += moveByteNum;
		return DATA_EXSIT;
	// case BOOL_DELETE:
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
const uchar *Chunk::skipForward(const uchar *reader, const uchar *endPtr, OrderByType soType)
{
	if (soType == ORDERBYS)
	{
		while (reader + sizeof(ID) < endPtr && (*(ID *)reader == 0))
		{
			reader += sizeof(ID);
			if (reader + sizeof(char) < endPtr && *(char *)reader != NONE)
			{
				char objType = *(char *)reader;
				reader += sizeof(char);
				if (reader + getLen(objType) >= endPtr)
				{
					return endPtr;
				}
			}
			else
			{
				return endPtr;
			}
		}
		if (reader + sizeof(ID) >= endPtr)
		{
			return endPtr;
		}
		else if (*(ID *)reader != 0)
		{
			return reader;
		}
	}
	else if (soType == ORDERBYO)
	{
		uint moveByteNum = 0;
		int status;
		while (reader + sizeof(char) < endPtr && (*(char *)reader != NONE))
		{
			status = getObjTypeStatus(reader, moveByteNum);
			if (status == DATA_NONE)
			{
				return endPtr;
			}
			else if (status == DATA_EXSIT)
			{
				if ((reader += sizeof(ID)) <= endPtr)
				{
					return reader - moveByteNum;
				}
				else
				{
					return endPtr;
				}
			}
			else if (status == DATA_DELETE)
			{
				reader += sizeof(ID);
			}
		}
		return endPtr;
	}
	return endPtr;
}

const uchar *Chunk::skipBackward(const uchar *reader, const uchar *endPtr, OrderByType soType)
{
	const uchar *temp = reader + sizeof(MetaData);
	reader = temp;
	uint len = 0;
	while (reader < endPtr)
	{
		len = reader - temp;
		reader = Chunk::skipForward(temp, endPtr, soType);
	}
	if (len)
	{
		return temp - len;
	}
	return endPtr;
}
