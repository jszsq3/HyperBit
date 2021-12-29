/*
 * ChunkManager.h
 *
 *  Created on: 2010-4-12
 *      Author: liupu
 */

#ifndef CHUNKMANAGER_H_
#define CHUNKMANAGER_H_

class MemoryBuffer;
class MMapBuffer;
class Chunk;
class ChunkManager;

#include "TripleBit.h"
#include "LineHashIndex.h"
#include "ThreadPool.h"
#include "util/atomic.hpp"

///////////////////////////////////////////////////////////////////////////////////////////////
///// class BitmapBuffer
///////////////////////////////////////////////////////////////////////////////////////////////
class BitmapBuffer {
public:
	map<ID, ChunkManager*> predicate_managers[2]; //基于subject和object排序的每个predicate对应的ChunkManager
	const string dir; //位图矩阵存储的路径
	MMapBuffer *tempByS, *tempByO; //存储以subject或object排序的ID编码后三元组信息
	size_t usedPageByS, usedPageByO; //以subject或object排序存储三元组已使用的Chunk数量
public:
	BitmapBuffer() {
	}
	BitmapBuffer(const string _dir);
	virtual ~BitmapBuffer();
	//加载predicate对应的ChunkManager信息与对应的索引信息
	static BitmapBuffer* load(MMapBuffer* bitmapImage, MMapBuffer* bitmapPredicateImage);
	//插入一条predicate信息，创建以subject和以object排序的ChunkManager，并确认predicate对应object数据类型
	Status insertPredicate(ID predicateID, OrderByType soType);
	//插入一条三元组信息，根据object数据类型确定插入object所占字节
	Status insertTriple(ID predicateID, ID subjectID, ID objectID, ID timeID, OrderByType soType);
	//根据predicate与SO排序方式获取对应的ChunkManager
	ChunkManager* getChunkManager(ID predicateID, OrderByType soType);
	//获取数据库中所有三元组总数
	size_t getTripleCount();
	//根据SO排序类型增大临时文件一个Chunk大小，并修改临时文件中ChunkManagerMeta的尾指针地址，返回添加Chunk的初始地址
	uchar* getPage(bool soType, size_t& pageNo);
	//将缓冲区文件写入文件中
	void flush();
	//将临时文件数据存储到数据库三元组存储文件BitmapBuffer中，建立谓词相关信息
	void save();

	void endUpdate(MMapBuffer *bitmapPredicateImage, MMapBuffer *bitmapOld);

	void print(int f);
};

/*
struct timeStamp {
	int tm_sec;   // 秒，正常范围从 0 到 59，但允许至 61
	int tm_min;   // 分，范围从 0 到 59
	int tm_hour;  // 小时，范围从 0 到 23
	int tm_mday;  // 一月中的第几天，范围从 1 到 31
	int tm_mon;   // 月，范围从 0 到 11
	int tm_year;  // 自 1900 年起的年数
	int tm_wday;  // 一周中的第几天，范围从 0 到 6，从星期日算起
	int tm_yday;  // 一年中的第几天，范围从 0 到 365，从 1 月 1 日算起
	int tm_isdst; // 夏令时
};

enum opCode {
	ins, del
};

struct logManagerMeta {
	uint pid; //predicate id
	size_t usedSpace; //已使用空间
	triplebit::atomic<size_t> logCount; //该logManager中操作记录的总数
};

//一个logChunk块的元数据信息，存储在每个logChunk块的头部
struct logChunkMeta {
	size_t pageNo;//logChunk块号
	size_t usedSpace; //logChunk已使用空间大小
	timeStamp minT;//logChunk的起始时间戳
	uint count; //logChunk中操作记录的总数
};

class logManager {
private:
	logManagerMeta* meta; //logManager的元数据信息
	vector<size_t> usedPages; //logManager所占用的页号集合
	pthread_mutex_t mutex;

public:
	logManager() {
		pthread_mutex_init(&mutex, NULL);
	}
	virtual ~logManager();
	//向logManager中插入更新操作
	void write(timeStamp t, opCode op, ID subjectID, ID objectID);
	//读取操作
	void read(const uchar*& reader, timeStamp& t, opCode& op, ID& subjectID, ID& objectID);
	//获取新的logChunk
	Status resize(size_t& pageNo);
	//判断添加len长度数据后logChunk是否溢出
	bool isChunkOverFlow(uint len);
	//获取logChunk总数
	size_t getChunkNumber();
	//获取元数据
	ChunkManagerMeta* getChunkManagerMeta() {
		return meta;
	}
	//获取操作总数
	int getTripleCount() {
		return meta->count;
	}
	//获取pid
	ID getPredicateID() const {
		return meta->pid;
	}
	//打印
	void print();

};

*/
/////////////////////////////////////////////////////////////////////////////////////////////
///////// class ChunkManager
/////////////////////////////////////////////////////////////////////////////////////////////
struct ChunkManagerMeta {
	uint pid; //predicate id
	bool soType; // SO排序类型,false:s;true:o
	size_t length; //已申请的Chunk空间
	size_t usedSpace; //数据区已使用空间，包括MetaData头部信息，不包含ChunkManagerMeta存储空间
	DataType objType; //predicate对应的object数据类型
	triplebit::atomic<size_t> tripleCount; //该ChunkManager中triple的总数
	uchar* startPtr; //该ChunkManager中数据区的起始位置，即每个Chunk中MetaData的位置
	uchar* endPtr; //该ChunkManager中数据区的结束位置
};

struct MetaData {
	int pageNo;//如果是原始块，pageNo表示的是最后一块的块号，-1表示最后一块是原始块。
	           //如果是追加块，pageNo为追加块的块号，块号从零开始。
	ID min, max; //以S（O）排序的Chunk中以S（O）的最小值与最大值
	size_t usedSpace; //Chunk已使用空间大小
	uint tripleCount; //该Chunk中triple的总数
	int NextPageNo; //下一个Chunk的Chunk号，-1表示没有下一块。
	size_t updateCount;//更新标记，用来检查是否需要重新排序
};

class ChunkManager {
private:
	//logManager* log;//对应的log
	ChunkManagerMeta* meta; //每个ChunkManager的元数据信息
	BitmapBuffer* bitmapBuffer; //BitmapBuffer
	LineHashIndex* chunkIndex; //以S（O）排序存储的每个predicate存储块的索引信息
	uchar* lastChunkStartPtr; //以S（O）排序存储的每个predicate存储块最后一个Chunk的起始位置
	vector<size_t> usedPages; //ChunkManager所占用的页号集合
	pthread_mutex_t mutex;
public:
	friend class BuildSortTask;
	friend class BuildMergeTask;
	friend class HashIndex;
	friend class BitmapBuffer;
	friend class PartitionMaster;

public:
	ChunkManager() {
		pthread_mutex_init(&mutex, NULL);
	}
	ChunkManager(ID predicateID, OrderByType soType, BitmapBuffer* _bitmapBuffer);
	virtual ~ChunkManager();
	//为Chunk建立索引信息
//	Status buildChunkIndex();
	//更新Chunk索引信息
//	Status updateChunkIndex();
	//向chunkManager中插入subjectID， objectID
	void insert(ID subjectID, ID objectID,ID timeID);
	//向指定位置写入数据
	void write(uchar*& reader, ID subjectID, ID objectID,ID timeID);
	//读取subjectID、objectID
	void read(const uchar*& reader, ID& subjectID, ID& objectID,ID& timeID);
	//删除一条三元组
	void deleteTriple(uchar*& reader);
	//获取新的Chunk
	Status resize(size_t& pageNo);
	//判断添加len长度数据后Chunk是否溢出
	bool isChunkOverFlow(uint len);
	//获取Chunk总数
	size_t getChunkNumber();

	Status updateTripleCount(longlong varSize) {
		pthread_mutex_lock(&mutex);
		if (varSize < 0 && abs(varSize) > meta->tripleCount) {
			pthread_mutex_unlock(&mutex);
			return ERROR;
		}
		meta->tripleCount += varSize;
		pthread_mutex_unlock(&mutex);
		return OK;
	}

	ChunkManagerMeta* getChunkManagerMeta() {
		return meta;
	}

	LineHashIndex* getChunkIndex() {
		return chunkIndex;
	}
	int getTripleCount() {
		return meta->tripleCount;
	}
	ID getPredicateID() const {
		return meta->pid;
	}
	uchar* getStartPtr() {
		return meta->startPtr;
	}
	uchar* getEndPtr() {
		return meta->endPtr;
	}
	void setMetaDataMin(MetaData *metaData, ID x, double y);
	//加载ChunkManager相关信息
	static ChunkManager* load(ID predicateID, bool soType, uchar* buffer, size_t& offset);

	void print(int f);

};

///////////////////////////////////////////////////////////////////////////////////////////////////////

class Chunk {
public:
	Chunk();
	virtual ~Chunk();
	//在指定位置写入ID数据
	static void writeID(uchar*& writer, ID data){
		memcpy(writer, &data, sizeof(ID));
		writer += sizeof(ID);
	}
	//在指定位置读取ID数据
	static void readID(const uchar* &reader, ID& data) {
		memcpy(&data, reader, sizeof(ID));
		reader += sizeof(ID);
	}
	//根据数据类型删除在指定位置数据，删除将该位置0
	static void deleteData(uchar*& reader);
	///根据object数据类型获取一对数据的字节长度
	static uint getLen(char dataType = STRING);
	//根据objType判定是否有数据存储、已删除、无数据,若有数据或数据已删除则返回数据类型或已删除数据类型, 并将修改reader指向地址为该条数据object后面的地址
	static Status getObjTypeStatus(const uchar*& reader, uint& moveByteNum);
	/// Skip a s or o
	static const uchar* skipData(const uchar* reader, char dataType = STRING);
	//根据object数据类型从reader位置向前跳至第一对x-y值
	static const uchar* skipForward(const uchar* reader, const uchar* endPtr, OrderByType soType);
	//根据object数据类型在endPtr位置向后跳至最后一对x-y值, reader为MetaData位置
	static const uchar* skipBackward(const uchar* reader, const uchar* endPtr, OrderByType soType);
	static Status tripleCountAdd(MetaData *metaData) {
		metaData->tripleCount++;
		return OK;
	}
	static Status tripleCountDecrease(MetaData *metaData) {
		assert(metaData->tripleCount > 0);
		metaData->tripleCount--;
		return OK;
	}
	static Status updateTripleCount(MetaData *metaData, longlong varSize) {
		if (varSize < 0 && abs(varSize) > metaData->tripleCount) {
			return ERROR;
		}
		metaData->tripleCount += varSize;
		return OK;
	}
};
#endif /* CHUNKMANAGER_H_ */
