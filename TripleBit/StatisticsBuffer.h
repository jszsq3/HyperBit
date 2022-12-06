/*
 * StatisticsBuffer.h
 *
 *  Created on: Aug 31, 2010
 *      Author: root
 */

#ifndef STATISTICSBUFFER_H_
#define STATISTICSBUFFER_H_

#include "TripleBit.h"
#include "MemoryBuffer.h"
#include "MMapBuffer.h"
#include "BitmapBuffer.h"
#include <leveldb/db.h>
#include <leveldb/write_batch.h>

class MMapBuffer;

//#define MYDEBUG
//SP、OP统信息类，存储结构：s-p-spcount、o-p-opcount

template<typename T>
uchar *writeData(uchar *writer, T data)
{
	memcpy(writer, &data, sizeof(T));
	return writer + sizeof(T);
}

template<typename T>
uchar *writeData(uchar *writer, T data, char objType)
{
	char c;
	int i;
	float f;
	longlong ll;
	double d;
	uint ui;
	switch (objType)
	{
	case BOOL:
	case CHAR:
		c = (char) data;
		*(char *) writer = c;
		writer += sizeof(char);
		break;
	case INT:
		i = (int) data;
		*(int *) writer = i;
		writer += sizeof(int);
		break;
	case FLOAT:
		f = (float) data;
		*(float *) writer = f;
		writer += sizeof(float);
		break;
	case LONGLONG:
		ll = (longlong) data;
		*(longlong *) writer = ll;
		writer += sizeof(longlong);
		break;
	case DATE:
	case DOUBLE:
		d = data;
		*(double *) writer = d;
		writer += sizeof(double);
		break;
	case UNSIGNED_INT:
	case STRING:
	default:
		ui = (uint) data;
		*(uint *) writer = ui;
		writer += sizeof(uint);

		break;
	}
	return writer;
}
template<typename T>
const uchar *readData(const uchar *reader, T &data)
{
	memcpy(&data, reader, sizeof(T));
	return reader + sizeof(T);
}

class STInfo
{
private:
	string stInfoDir;
	leveldb::DB *db;
	leveldb::Options options;
	leveldb::Status status;
	leveldb::WriteBatch batch;
	boost::mutex mutex;

public:
	STInfo(string stInfoDir) :
		stInfoDir(stInfoDir)
	{
		options.create_if_missing = true;
		status = leveldb::DB::Open(options, stInfoDir, &db);
		assert(status.ok());
		//		if (status.ok()) {
		//			std::cout << "leveldb open success!" << std::endl;
		//		}
	}

	~STInfo()
	{
		if (db != NULL)
		{
			delete db;
			db = NULL;
		}
	}

	void reBuildSTInfo()
	{
		delete db;
		db = NULL;
		string order = "rm -r ";
		order.append(stInfoDir);
		std::system(order.c_str());
		status = leveldb::DB::Open(options, stInfoDir, &db);
		if (status.ok())
		{
			std::cout << "leveldb open success!" << std::endl;
		}
	}

	//存储s对应的p，修改sp的count，若已存在，则叠加计算
	template<typename T>
	bool put(T so, char soType, ID predicateID, longlong count)
	{
		string spKey, spCount;
		spKey = to_string(so);
		spKey.append("_").append(to_string(soType)).append("_").append(to_string(predicateID));
		status = db->Get(leveldb::ReadOptions(), spKey, &spCount);
		longlong c;
		if (status.ok())
		{
			c = stoll(spCount);
			c += count;

		}
		else if (status.IsNotFound())
		{
			//添加s对应的p信息
			c = count;
			string sKey = to_string(so);
			sKey.append("_").append(to_string(soType));

			string pValue;
			status = db->Get(leveldb::ReadOptions(), sKey, &pValue);
			// if (status.ok()) {
			pValue.append(to_string(predicateID)).append("_");
			// } else if (status.IsNotFound()) {
			// 	pValue = to_string(predicateID);
			// 	pValue.append("_");
			// }
			db->Put(leveldb::WriteOptions(), sKey, pValue);
		}
		spCount = to_string(c);
		status = db->Put(leveldb::WriteOptions(), spKey, spCount);
		return status.ok() ? true : false;
	}

	bool put(const string &so_sotype, ID predicateID, longlong count)
	{
		string spKey = so_sotype, spCount;
		spKey.append("_").append(to_string(predicateID));
		status = db->Get(leveldb::ReadOptions(), spKey, &spCount);
		longlong c;
		if (status.ok())
		{
			c = stoll(spCount);
			c += count;

		}
		else if (status.IsNotFound())
		{
			//添加s对应的p信息
			c = count;
			string sKey = so_sotype;
			string pValue;
			status = db->Get(leveldb::ReadOptions(), sKey, &pValue);
			// if (status.ok()) {
			pValue.append(to_string(predicateID)).append("_");
			// } else if (status.IsNotFound()) {
			// 	pValue = to_string(predicateID);
			// 	pValue.append("_");
			// }
			db->Put(leveldb::WriteOptions(), sKey, pValue);
		}
		spCount = to_string(c);
		status = db->Put(leveldb::WriteOptions(), spKey, spCount);
		return status.ok() ? true : false;
	}

	template<typename T>
	bool put_sync(T so, char soType, ID predicateID, longlong count)
	{
		// cout << (int)soType << endl;
		//
		string spKey, spCount;
		spKey = to_string(so);
		spKey.append("_").append(to_string(soType)).append("_").append(to_string(predicateID));
		mutex.lock();
		status = db->Get(leveldb::ReadOptions(), spKey, &spCount);
		longlong c;
		if (status.ok())
		{
			if (spCount.empty())
			{
				cout << "spCount is null\n";
			}
			c = stoll(spCount);
			c += count;
			if (c < 0)
			{
				MessageEngine::showMessage("delete is not legal", MessageEngine::ERROR);
			}
		}
		else if (status.IsNotFound())
		{
			//添加s对应的p信息
			c = count;
			string sKey = to_string(so);
			sKey.append("_").append(to_string(soType));

			string pValue;
			status = db->Get(leveldb::ReadOptions(), sKey, &pValue);
			// if (status.ok()) {
			pValue.append(to_string(predicateID)).append("_");
			// } else if (status.IsNotFound()) {
			// 	pValue = to_string(predicateID);
			// 	pValue.append("_");
			// }
			db->Put(leveldb::WriteOptions(), sKey, pValue);
		}
		spCount = to_string(c);
		status = db->Put(leveldb::WriteOptions(), spKey, spCount);
		mutex.unlock();
		if (status.ok())
		{
			return true;
		}
		return false;
	}

	template<typename T>
	bool put(T so, char soType, string &predicates)
	{
		string sKey, spount;
		sKey = to_string(so);
		sKey.append("_").append(to_string(soType));
		status = db->Put(leveldb::WriteOptions(), sKey, predicates);
		return status.ok() ? true : false;
	}

	void batch_Put_start()
	{
		batch.Clear();
	}

	template<typename T>
	void batch_Put(T so, char soType, string &predicates)
	{
		string sKey, spount;
		sKey = to_string(so);
		sKey.append("_").append(to_string(soType));
		batch.Put(sKey, predicates);
	}

	template<typename T>
	void batch_Put(T so, char soType, ID predicateID, longlong count)
	{
		string spKey, spCount;
		spKey = to_string(so);
		spKey.append("_").append(to_string(soType)).append("_").append(to_string(predicateID));
		spCount = to_string(count);
		batch.Put(spKey, spCount);
		if (batch.ApproximateSize() > 100000)
		{
			status = db->Write(leveldb::WriteOptions(), &batch);
			batch.Clear();
		}
	}

	bool batch_Put_end()
	{
		status = db->Write(leveldb::WriteOptions(), &batch);
		return status.ok() ? true : false;
	}

	template<typename T>
	bool delet(T so, char soType, ID predicateID)
	{
		string sKey = to_string(so);
		sKey.append("_").append(to_string(soType));
		string spKey = sKey;
		spKey.append("_").append(to_string(predicateID));
		db->Delete(leveldb::WriteOptions(), spKey);
		string pValue, pv = to_string(predicateID).append("_");
		db->Get(leveldb::ReadOptions(), sKey, &pValue);
		int index;
		bool fund = false;
		while ((index = pValue.find_first_of(pv)) != string::npos)
		{
			if (index == 0)
			{
				pValue = pValue.substr(pv.size());
				fund = true;
				break;
			}
			else if (pValue[index - 1] == '_')
			{
				string value = pValue.substr(0, index);
				value.append(pValue.substr(index + pv.size()));
				pValue = value;
				fund = true;
				break;
			}
			else
			{
				pValue = pValue.substr(index + pv.size());
			}
		}
		if (fund)
		{
			db->Put(leveldb::WriteOptions(), sKey, pValue);
		}
		return true;
	}

	template<typename T>
	bool delet(T so, char soType)
	{
		string sKey = to_string(so);
		sKey.append("_").append(to_string(soType));
		vector<ID> predicateIDs;
		get(so, soType, predicateIDs);
		for (auto pid : predicateIDs)
		{
			string spKey = sKey;
			spKey.append("_").append(to_string(pid));
			db->Delete(leveldb::WriteOptions(), spKey);
		}
		status = db->Delete(leveldb::WriteOptions(), sKey);
		return status.ok() ? true : false;
	}

	template<typename T>
	bool get(T so, char soType, ID predicateID, longlong &count)
	{
		string spKey, spCount;
		spKey = to_string(so);
		spKey.append("_").append(to_string(soType)).append("_").append(to_string(predicateID));
		status = db->Get(leveldb::ReadOptions(), spKey, &spCount);
		if (status.ok())
		{
			count = stoll(spCount);
			return true;
		}
		count = 0;
		return false;
	}

	template<typename T>
	bool get(T so, char soType, vector<ID> &predicateIDs)
	{
		string sKey = to_string(so);
		sKey.append("_").append(to_string(soType));
		string pValue, spValue;
		string spKey;
		status = db->Get(leveldb::ReadOptions(), sKey, &pValue);
		if (status.ok())
		{
			int start = 0, index = 0;
			while (index < pValue.size())
			{
				start = index;
				while (pValue[index++] != '_' && index < pValue.size())
					;
				spKey = sKey;
				spKey.append("_").append(pValue.substr(start, index - start - 1));
				status = db->Get(leveldb::ReadOptions(), sKey, &spValue);
				if (status.ok() && stoll(spValue) > 0)
				{
					predicateIDs.push_back(stoul(pValue.substr(start, index - start - 1)));
				}
			}
			return true;
		}

		return false;
	}

	template<typename T>
	bool get(T so, char soType, map<ID, longlong> &pCount)
	{
		vector<ID> predicateIDs;
		bool result = get(so, soType, predicateIDs);
		if (result)
		{
			longlong count;
			for (auto predicateID : predicateIDs)
			{
				get(so, soType, predicateID, count);
				pCount[predicateID] = count;
			}
		}
		return result;
	}

	template<typename T>
	bool get(T so, char soType, longlong &count)
	{
		map<ID, longlong> pCount;
		get(so, soType, pCount);
		for (auto pair_ : pCount)
		{
			count += pair_.second;
		}
		return count;
	}

};

class StatisticsBuffer
{
public:
	struct Triple
	{
		double soValue;
		ID predicateID;
		uint count;
	};
private:
	StatisticsType statType;
	MMapBuffer *buffer;
	uchar *writer;

	Triple *index;

	uint usedSpace;
	uint indexPos, indexSize;

	Triple triples[3 * 4096];
	Triple *pos, *posLimit;
	bool first;
public:
	StatisticsBuffer(const string path, StatisticsType statType);
	~StatisticsBuffer();
	//插入一条SP或OP的统计信息,加入OP统计信息需指定objType
	template<typename T>
	Status addStatis(T soValue, ID predicateID, size_t count, char objType = STRING)
	{
		unsigned len = Chunk::getLen(objType) + sizeof(ID) + sizeof(size_t);
		if (statType == OBJECTPREDICATE_STATIS)
		{
			len += sizeof(char);
		}
		if (first || usedSpace + len > buffer->getSize())
		{
			usedSpace = writer - (uchar *) buffer->getBuffer();
			buffer->resize(STATISTICS_BUFFER_INCREMENT_PAGE_COUNT * MemoryBuffer::pagesize);	//加大空间
			writer = (uchar *) buffer->getBuffer() + usedSpace;

			if ((indexPos + 1) >= indexSize)
			{
				index = (Triple *) realloc(index, indexSize * sizeof(Triple) + MemoryBuffer::pagesize * sizeof(Triple));
				if (index == NULL)
				{
					cout << "realloc StatisticsBuffer error" << endl;
					return ERR;
				}
				indexSize += MemoryBuffer::pagesize;
			}

			index[indexPos].soValue = soValue;
			index[indexPos].predicateID = predicateID;
			index[indexPos].count = usedSpace; //record offset，可以得出实体——谓词所在的块号

			indexPos++;
			first = false;
		}

		if (statType == SUBJECTPREDICATE_STATIS)
		{
			writer = writeData(writer, soValue, objType);
		}
		else if (statType == OBJECTPREDICATE_STATIS)
		{
			writer = writeData(writer, objType); //OP统计信息O前需加objType
			writer = writeData(writer, soValue, objType);
		}

		writer = writeData(writer, predicateID);
		writer = writeData(writer, count);

		usedSpace = writer - (uchar *) buffer->getBuffer();
		return OK;
	}
	//获取一条SP或OP的统计信息
	Status getStatis(double soValue, ID predicateID, size_t &count, char objType = STRING);
	//根据SP（OP）统计信息获取S（O）出现的次数
	template<typename T>
	Status getStatisBySO(T soValue, size_t &count, char objType = STRING)
	{
		count = 0;
		pos = index, posLimit = index + indexPos;
		findLocation(soValue);
		if (pos->soValue >= soValue)
		{
			pos--;
		}

		uint start = pos->count;
		while (pos <= posLimit && pos->soValue <= soValue)
		{
			pos++;
		}

		uint end = pos->count; // count is usedspace
		if (pos == (index + indexPos))
			end = usedSpace;

		const uchar *begin = (uchar *) buffer->getBuffer() + start, *limit = (uchar *) buffer->getBuffer() + end;
		decodeStatis(begin, limit, soValue, count, objType);

		if (count != 0)
		{
			return OK;
		}

		return NOT_FOUND;
	}

	template<typename T>
	Status findAllPredicateBySO(T soValue, vector<ID> &pids, char objType = STRING)
	{
		pids.clear();
		pos = index, posLimit = index + indexPos;
		findLocation(soValue);
		if (pos->soValue >= soValue)
		{
			pos--;
		}

		uint start = pos->count;
		while (pos <= posLimit && pos->soValue <= soValue)
		{
			pos++;
		}

		uint end = pos->count; // count is usedspace
		if (pos == (index + indexPos))
			end = usedSpace;

		const uchar *begin = (uchar *) buffer->getBuffer() + start, *limit = (uchar *) buffer->getBuffer() + end;
		findAllPredicateBySO(begin, limit, soValue, pids, objType);

		if (pids.size() != 0)
		{
			return OK;
		}
		return NOT_FOUND;
	}
	//定位SP或OP初始位置,pos: the start address of the first triple;posLimit: the end address of last triple;
	bool findLocation(ID predicateID, double soValue);
	//定位S或O初始位置,pos: the start address of the first triple;posLimit: the end address of last triple;
	bool findLocation(double soValue);
	//建立统计信息的索引
	Status save(MMapBuffer *&indexBuffer);
	//加载统计信息的索引
	static StatisticsBuffer *load(StatisticsType statType, const string path, uchar *&indxBuffer);
private:
	/// decode a statistics chunk
	void decodeStatis(const uchar *begin, const uchar *end, double soValue, ID predicateID, size_t &count,
	                  char objType = STRING);
	void decodeStatis(const uchar *begin, const uchar *end, double soValue, size_t &count, char objType = STRING);
	void findAllPredicateBySO(const uchar *begin, const uchar *end, double soValue, vector<ID> &pids, char objType =
	                              STRING);
};
#endif /* STATISTICSBUFFER_H_ */
