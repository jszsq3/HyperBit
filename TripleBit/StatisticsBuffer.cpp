/*
 * StatisticsBuffer.cpp
 *
 *  Created on: Aug 31, 2010
 *      Author: root
 */

#include "StatisticsBuffer.h"
#include "BitmapBuffer.h"
#include "URITable.h"

//#define MYDEBUG

StatisticsBuffer::StatisticsBuffer(const string path, StatisticsType statType) :
		statType(statType) {
	buffer = new MMapBuffer(path.c_str(), STATISTICS_BUFFER_INIT_PAGE_COUNT * MemoryBuffer::pagesize);
	writer = (uchar*) buffer->getBuffer();
	usedSpace = 0;
	indexPos = 0;
	indexSize = 0; //MemoryBuffer::pagesize;
	index = NULL;
	first = true;
}

StatisticsBuffer::~StatisticsBuffer() {
	if (buffer != NULL) {
		delete buffer;
	}
	buffer = NULL;
	index = NULL;
}

/**
 * 获取SP或OP的统计信息
 * SP：直接解压SP范围内的SP进行比较
 * OP：除了OP比较外，还需比较O的数据类型
 */
void StatisticsBuffer::decodeStatis(const uchar* begin, const uchar* end, double soValue, ID predicateID, size_t& count, char objType) {
	ID tempPredicateID;
	size_t tempCount = 0;
	if (statType == SUBJECTPREDICATE_STATIS) {
		ID subjectID;
		while (begin + sizeof(ID) < end) {
			begin = readData(begin, subjectID);
			if (subjectID && begin + sizeof(ID) < end) {
				begin = readData(begin, tempPredicateID);
				if (tempPredicateID && begin + sizeof(size_t) <= end) {
					begin = readData(begin, tempCount);
					if (subjectID == soValue && tempPredicateID == predicateID) {
						count = tempCount;
						return;
					} else if (subjectID > soValue) {
						break;
					}
				} else {
					break;
				}
			} else {
				break;
			}
		}
	} 
	/*
	else if (statType == OBJECTPREDICATE_STATIS) {
		char tempObjType;
		double tempObject;
		uint moveByteNum = 0;
		int status;
		while (begin + sizeof(char) < end) {
			status = Chunk::getObjTypeStatus(begin, moveByteNum);
			if (status == DATA_EXSIT) {
				begin -= moveByteNum;
				begin = readData(begin, tempObjType);
				if (tempObjType != NONE && begin + Chunk::getLen(tempObjType) < end) {
					begin = Chunk::read(begin, tempObject, tempObjType);
					if (begin + sizeof(ID) < end) {
						begin = readData(begin, tempPredicateID);
						if (tempPredicateID && begin + sizeof(size_t) <= end) {
							begin = readData(begin, tempCount);
							if (tempObject == soValue && tempPredicateID == predicateID && objType == tempObjType) {
								count = tempCount;
								return;
							} else if (soValue < tempObject || (soValue == tempObject && objType < tempObjType)) {
								break;
							}
						} else {
							break;
						}
					} else {
						break;
					}
				} else {
					break;
				}
			} else {
				break;
			}
		}
	}*/
}

void StatisticsBuffer::decodeStatis(const uchar* begin, const uchar* end, double soValue, size_t& count, char objType) {
	ID predicateID;
	size_t tempCount = 0;
	if (statType == SUBJECTPREDICATE_STATIS) {
		ID subjectID;
		while (begin + sizeof(ID) < end) {
			begin = readData(begin, subjectID);
			if (subjectID && begin + sizeof(ID) < end) {
				begin = readData(begin, predicateID);
				if (predicateID && begin + sizeof(size_t) <= end) {
					begin = readData(begin, tempCount);
					if (subjectID == soValue) {
						count += tempCount;
					} else if (subjectID > soValue) {
						break;
					}
				} else {
					break;
				}
			} else {
				break;
			}
		}
	} 
	/*else if (statType == OBJECTPREDICATE_STATIS) {
		char tempObjType;
		double object;
		uint moveByteNum;
		int status;
		while (begin + sizeof(char) < end) {
			status = Chunk::getObjTypeStatus(begin, moveByteNum);
			if (status == DATA_EXSIT) {
				begin -= moveByteNum;
				begin = readData(begin, tempObjType);
				if (tempObjType != NONE && begin + Chunk::getLen(tempObjType) < end) {
					begin = Chunk::read(begin, object, tempObjType);
					if (begin + sizeof(ID) < end) {
						begin = readData(begin, predicateID);
						if (predicateID && begin + sizeof(size_t) <= end) {
							begin = readData(begin, tempCount);
							if (soValue == object && objType == tempObjType) {
								count += tempCount;
							} else if (soValue < object || (soValue == object && objType < tempObjType)) {
								break;
							}
						} else {
							break;
						}
					} else {
						break;
					}
				} else {
					break;
				}
			} else {
				break;
			}
		}
	}*/
}

void StatisticsBuffer::findAllPredicateBySO(const uchar* begin, const uchar* end, double soValue, vector<ID>& pids, char objType) {
	ID predicateID;
	size_t tempCount = 0;
	if (statType == SUBJECTPREDICATE_STATIS) {
		ID subjectID;
		while (begin + sizeof(ID) < end) {
			begin = readData(begin, subjectID);
			if (subjectID && begin + sizeof(ID) < end) {
				begin = readData(begin, predicateID);
				if (predicateID && begin + sizeof(size_t) <= end) {
					begin = readData(begin, tempCount);
					if (subjectID == soValue && tempCount != 0) {
						pids.push_back(predicateID);
					} else if (subjectID > soValue) {
						break;
					}
				} else {
					break;
				}
			} else {
				break;
			}
		}
	} /*else if (statType == OBJECTPREDICATE_STATIS) {
		char tempObjType;
		double object;
		uint moveByteNum;
		int status;
		while (begin + sizeof(char) < end) {
			status = Chunk::getObjTypeStatus(begin, moveByteNum);
			if (status == DATA_EXSIT) {
				begin -= moveByteNum;
				begin = readData(begin, tempObjType);
				if (begin + Chunk::getLen(tempObjType) < end) {
					begin = Chunk::read(begin, object, tempObjType);
					if (begin + sizeof(ID) < end) {
						begin = readData(begin, predicateID);
						if (predicateID && begin + sizeof(size_t) <= end) {
							begin = readData(begin, tempCount);
							if (soValue == object && objType == tempObjType && tempCount != 0) {
								pids.push_back(predicateID);
							} else if (soValue < object || (soValue == object && objType < tempObjType)) {
								break;
							}
						} else {
							break;
						}
					} else {
						break;
					}
				} else {
					break;
				}
			} else {
				break;
			}
		}
	}*/
}

static inline bool greaterCouple(ID a1, double a2, ID b1, double b2) {
	return (a2 > b2) || ((a2 == b2) && (a1 > b1));
}

bool StatisticsBuffer::findLocation(ID predicateID, double soValue) {
	int left = 0, right = posLimit - pos;
	int middle;

	while (left != right) {
		middle = left + ((right - left) / 2);

		if (greaterCouple(predicateID, soValue, pos[middle].predicateID, pos[middle].soValue)) {
			left = middle + 1;
		} else if ((!middle) || greaterCouple(predicateID, soValue, pos[middle - 1].predicateID, pos[middle - 1].soValue)) {
			break;
		} else {
			right = middle;
		}
	}

	if (left == right) {
		return false;
	} else {
		pos = &pos[middle]; // value1 and value2 is between middle-1 and middle
		return true;
	}
}

bool StatisticsBuffer::findLocation(double soValue) {
	int left = 0, right = posLimit - pos;
	int middle;

	while (left != right) {
		middle = left + ((right - left) / 2);

		if (soValue > pos[middle].soValue) {
			left = middle + 1;
		} else if ((!middle) || soValue > pos[middle - 1].soValue) {
			break;
		} else {
			right = middle;
		}
	}

	if (left == right) {
		return false;
	} else {
		pos = &pos[middle]; // value1 and value2 is between middle-1 and middle
		return true;
	}
}

Status StatisticsBuffer::getStatis(double soValue, ID predicateID, size_t& count, char objType) {
	pos = index, posLimit = index + indexPos;
	findLocation(predicateID, soValue); // get index location, that is pos
	if (greaterCouple(pos->predicateID, pos->soValue, predicateID, soValue))
		pos--;

	uint start = pos->count;
	while (pos <= posLimit && !greaterCouple(pos->predicateID, pos->soValue, predicateID, soValue)) {
		pos++;
	}
	uint end = pos->count; // count is usedspace
	if (pos == (index + indexPos))
		end = usedSpace;

	const uchar* begin = (uchar*) buffer->getBuffer() + start, *limit = (uchar*) buffer->getBuffer() + end;
	decodeStatis(begin, limit, soValue, predicateID, count, objType);
	if (count) {
		return OK;
	}

	return NOT_FOUND;
}

Status StatisticsBuffer::save(MMapBuffer*& indexBuffer) {
	uchar* writer;
	if (indexBuffer == NULL) {
		indexBuffer = MMapBuffer::create(string(string(DATABASE_PATH) + "/statIndex").c_str(), indexPos * sizeof(Triple) + 2 * sizeof(unsigned));
		writer = indexBuffer->get_address();
	} else {
		size_t size = indexBuffer->getSize();
		indexBuffer->resize(indexPos * sizeof(Triple) + 2 * sizeof(unsigned));
		writer = indexBuffer->get_address() + size;
	}

	writer = writeData(writer, usedSpace);
	writer = writeData(writer, indexPos);

	memcpy(writer, (char*) index, indexPos * sizeof(Triple));
	free(index);

	return OK;
}

StatisticsBuffer* StatisticsBuffer::load(StatisticsType statType, const string path, uchar*& indexBuffer) {
	StatisticsBuffer* statBuffer = new StatisticsBuffer(path, statType);

	indexBuffer = (uchar*) readData(indexBuffer, statBuffer->usedSpace);
	indexBuffer = (uchar*) readData(indexBuffer, statBuffer->indexPos);
#ifdef DEBUG
	cout<<__FUNCTION__<<"indexPos: "<<statBuffer->indexPos<<endl;
#endif
	// load index;
	statBuffer->index = (Triple*) indexBuffer;
	indexBuffer = indexBuffer + statBuffer->indexPos * sizeof(Triple);

	return statBuffer;
}
