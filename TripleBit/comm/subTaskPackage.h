/*
 * subTaskPackage.h
 *
 *  Created on: 2013-8-20
 *      Author: root
 */

#ifndef SUBTASKPACKAGE_H_
#define SUBTASKPACKAGE_H_

#include "../TripleBit.h"
#include "../TripleBitQueryGraph.h"
#include "../util/BufferManager.h"
#include "../util/PartitionBufferManager.h"
#include "../EntityIDBuffer.h"

using namespace std;
using namespace boost;

class SubTaskPackageForDelete {
public:
	size_t referenceCount;
	TripleBitQueryGraph::OpType operationType;
	ID sID;
	bool constSubject; //subject是否已知
	bool constObject; //subject是否已知
	double object;
	char objType;
	map<ID, MidResultBuffer*> tempBuffer;
	pthread_mutex_t subTaskMutex;
public:
	SubTaskPackageForDelete() {
	}
	SubTaskPackageForDelete(size_t reCount, TripleBitQueryGraph::OpType opType, ID sID, bool constSubject, bool constObject) :
			referenceCount(reCount), operationType(opType), sID(sID), constSubject(constSubject), constObject(constObject) {
		pthread_mutex_init(&subTaskMutex, NULL);
	}
	SubTaskPackageForDelete(size_t reCount, TripleBitQueryGraph::OpType opType, double object, char objType, bool constSubject, bool constObject) :
			referenceCount(reCount), operationType(opType), object(object), objType(objType), constSubject(constSubject), constObject(constObject) {
		pthread_mutex_init(&subTaskMutex, NULL);
	}
	~SubTaskPackageForDelete() {
		tempBuffer.clear();
		pthread_mutex_destroy(&subTaskMutex);
	}

	bool completeSubTask(ID chunkID, MidResultBuffer* result) {
		pthread_mutex_lock(&subTaskMutex);
		tempBuffer[chunkID] = result;
		referenceCount--;

		if (referenceCount == 0) {
			pthread_mutex_unlock(&subTaskMutex);
			return true;
		} else {
			pthread_mutex_unlock(&subTaskMutex);
			return false;
		}
	}

	MidResultBuffer *getTaskResult() {
		map<ID, MidResultBuffer*>::iterator iter = tempBuffer.begin();
		size_t totalSize = 0;
		for (iter = tempBuffer.begin(); iter != tempBuffer.end(); iter++) {
			totalSize += iter->second->getUsedSize();
		}
		MidResultBuffer *resultBuffer = NULL;
		if (totalSize != 0) {
			iter = tempBuffer.begin();
			resultBuffer = new MidResultBuffer((*iter).second->getResultType());
			resultBuffer->resize(totalSize);
			for (; iter != tempBuffer.end(); iter++) {
				resultBuffer->appendBuffer(iter->second);
				delete iter->second;
				iter->second = NULL;
			}
		}
		return resultBuffer;
	}
};

class subTaskPackage {
public:
	size_t referenceCount;
	TripleBitQueryGraph::OpType operationType;
	ID sourceWorkerID;
	ID minID;
	ID maxID;
	ID updateID;
	map<ID, EntityIDBuffer*> xTempBuffer;
	map<ID, EntityIDBuffer*> xyTempBuffer;

	PartitionBufferManager* partitionBuffer;
	pthread_mutex_t subTaskMutex;

public:
	subTaskPackage() {
		pthread_mutex_init(&subTaskMutex, NULL);
	}
	subTaskPackage(size_t reCount, TripleBitQueryGraph::OpType opType, ID sourceID, ID minid, ID maxid, ID deleteid, ID updateid, PartitionBufferManager*& buffer) :
			referenceCount(reCount), operationType(opType), sourceWorkerID(sourceID), minID(minid), maxID(maxid), updateID(updateid), partitionBuffer(buffer) {
		pthread_mutex_init(&subTaskMutex, NULL);
	}
	virtual ~subTaskPackage() {
		partitionBuffer = NULL;
		xTempBuffer.clear();
		xyTempBuffer.clear();
		pthread_mutex_destroy(&subTaskMutex);
	}

	bool completeSubTask(ID chunkID, EntityIDBuffer* result) {
		pthread_mutex_lock(&subTaskMutex);
		xTempBuffer[chunkID] = result;
		referenceCount--;

		if (referenceCount == 0) {
			pthread_mutex_unlock(&subTaskMutex);
			return true;
		} else {
			pthread_mutex_unlock(&subTaskMutex);
			return false;
		}
	}

	size_t getTotalSize() {
		map<ID, EntityIDBuffer*>::iterator iter;
		size_t totalSize = 0;
		for (iter = xTempBuffer.begin(); iter != xTempBuffer.end(); iter++) {
			totalSize += iter->second->getUsedSize();
		}
		for (iter = xyTempBuffer.begin(); iter != xyTempBuffer.end(); iter++) {
			totalSize += iter->second->getUsedSize();
		}
		return totalSize;
	}

	void getAppendBuffer(int beg, int end, int index, EntityIDBuffer *buffer) {
		if (index == 1) {
			for (int i = beg; i < end; i++) {
				buffer->appendBuffer(xTempBuffer[i]);
			}
		} else {
			for (int i = beg; i < end; i++) {
				buffer->appendBuffer(xyTempBuffer[i]);
			}
		}
	}

	EntityIDBuffer *getTaskResult() {
		EntityIDBuffer *resultXBuffer = new EntityIDBuffer;
		EntityIDBuffer *resultXYBuffer = new EntityIDBuffer;
		resultXBuffer->empty();
		resultXYBuffer->empty();

		map<ID, EntityIDBuffer*>::iterator iter = xTempBuffer.begin();
		if (iter != xTempBuffer.end()) {
			resultXBuffer->setIDCount(iter->second->getIDCount());
			resultXBuffer->setSortKey(iter->second->getSortKey());
		}
		size_t totalSize = 0;
		for (iter = xTempBuffer.begin(); iter != xTempBuffer.end(); iter++) {
			totalSize += iter->second->getUsedSize();
		}
		resultXBuffer->resize(totalSize);
		for (iter = xTempBuffer.begin(); iter != xTempBuffer.end(); iter++) {
			resultXBuffer->appendBuffer(iter->second);
			delete iter->second;
			iter->second = NULL;
		}

		iter = xyTempBuffer.begin();
		if (iter != xyTempBuffer.end()) {
			resultXBuffer->setIDCount(iter->second->getIDCount());
			resultXBuffer->setSortKey(iter->second->getSortKey());
		}
		totalSize = 0;
		for (iter = xyTempBuffer.begin(); iter != xyTempBuffer.end(); iter++) {
			totalSize += iter->second->getUsedSize();
		}
		resultXYBuffer->resize(totalSize);
		for (iter = xyTempBuffer.begin(); iter != xyTempBuffer.end(); iter++) {
			resultXYBuffer->appendBuffer(iter->second);
			delete iter->second;
			iter->second = NULL;
		}

		EntityIDBuffer* resultBuf = NULL;
		if (operationType == TripleBitQueryGraph::QUERY) {
			resultBuf = BufferManager::getInstance()->getNewBuffer();
		} else {
			resultBuf = partitionBuffer->getNewBuffer();
		}
		resultBuf->empty();
		resultBuf->setSortKey(0);
		if (resultXBuffer->getIDCount() == 1 && resultXYBuffer->getIDCount() == 1) {
			resultBuf->setIDCount(1);
		} else {
			resultBuf->setIDCount(2);
		}
		resultBuf->mergeBuffer(resultXBuffer, resultXYBuffer);

		delete resultXBuffer;
		delete resultXYBuffer;
		return resultBuf;
	}

	bool isRight() {
		int IDCount = 1;
		if (xTempBuffer.size() > 0) {
			IDCount = xTempBuffer.begin()->second->getIDCount();
		} else if (xyTempBuffer.size() > 0) {
			IDCount = xyTempBuffer.begin()->second->getIDCount();
		} else {
			return false;
		}

		map<ID, EntityIDBuffer*>::iterator iterX, iterXEnd, iterXY, iterXYEnd;
		iterX = xTempBuffer.begin();
		iterXEnd = xTempBuffer.end();
		iterXY = xyTempBuffer.begin();
		iterXYEnd = xyTempBuffer.end();

		register size_t posX = 0, posXY = 0;
		register size_t lenX = 0, lenXY = 0;
		ID *bufferX, *bufferXY;

		if (iterXY != iterXYEnd) {
			bufferXY = iterXY->second->getBuffer();
			lenXY = iterXY->second->getSize();
		}
		if (iterX != iterXEnd) {
			bufferX = iterX->second->getBuffer();
			lenX = iterX->second->getSize();
		}

		if (IDCount == 1) {
			ID key, idX, idXY;
			key = 0;
			while (iterX != iterXEnd && iterXY != iterXYEnd) {
				if (posXY == lenXY) {
					iterXY++;
					if (iterXY != iterXYEnd) {
						bufferXY = iterXY->second->getBuffer();
						lenXY = iterXY->second->getSize();
						posXY = 0;
						continue;
					} else {
						break;
					}
				}
				if (posX == lenX) {
					iterX++;
					if (iterX != iterXEnd) {
						bufferX = iterX->second->getBuffer();
						lenX = iterX->second->getSize();
						posX = 0;
						continue;
					} else {
						break;
					}
				}
				idXY = bufferXY[posXY];
				idX = bufferX[posX];
				if (idXY < key || idX < key) {
					return false;
				} else {
					key = idXY < idX ? idXY : idX;
				}
				if (idXY < idX) {
					posXY++;
				} else {
					posX++;
				}
			}
			while (iterX != iterXEnd) {
				if (posX == lenX) {
					iterX++;
					if (iterX != iterXEnd) {
						bufferX = iterX->second->getBuffer();
						lenX = iterX->second->getSize();
						posX = 0;
						continue;
					} else {
						break;
					}
				}
				idX = bufferX[posX];
				if (idX < key) {
					return false;
				} else
					key = idX;

				posX++;
			}
			while (iterXY != iterXYEnd) {
				if (posXY == lenXY) {
					iterXY++;
					if (iterXY != iterXYEnd) {
						bufferXY = iterXY->second->getBuffer();
						lenXY = iterXY->second->getSize();
						posXY = 0;
						continue;
					} else {
						break;
					}
				}
				idXY = bufferXY[posXY];
				if (idXY < key) {
					return false;
				} else
					key = idXY;

				posXY++;
			}
		} else if (IDCount == 2) {
			ID keyX, keyY, idX1, idY1, idX2, idY2;
			keyX = 0;
			keyY = 0;

			while (iterX != iterXEnd && iterXY != iterXYEnd) {
				if (posX == lenX) {
					iterX++;
					if (iterX != iterXEnd) {
						bufferX = iterX->second->getBuffer();
						lenX = iterX->second->getSize();
						posX = 0;
						continue;
					} else {
						break;
					}
				}
				if (posXY == lenXY) {
					iterXY++;
					if (iterXY != iterXYEnd) {
						bufferXY = iterXY->second->getBuffer();
						lenXY = iterXY->second->getSize();
						posXY = 0;
						continue;
					} else {
						break;
					}
				}
				idX1 = bufferX[posX * IDCount];
				idY1 = bufferX[posX * IDCount + 1];
				idX2 = bufferXY[posXY * IDCount];
				idY2 = bufferXY[posXY * IDCount + 1];
				if ((idX1 < keyX || (idX1 == keyX && idY1 < keyY)) || (idX2 < keyX || (idX2 == keyX && idY2 < keyY))) {
					return false;
				}
				if (idX1 < idX2 || (idX1 == idX2 && idY1 < idY2)) {
					keyX = idX1;
					keyY = idY1;
					posX++;
				} else {
					keyX = idX2;
					keyY = idY2;
					posXY++;
				}
			}
			while (iterXY != iterXYEnd) {
				if (posXY == lenXY) {
					iterXY++;
					if (iterXY != iterXYEnd) {
						bufferXY = iterXY->second->getBuffer();
						lenXY = iterXY->second->getSize();
						posXY = 0;
						continue;
					} else {
						break;
					}
				}
				idX2 = bufferXY[posXY * IDCount];
				idY2 = bufferXY[posXY * IDCount + 1];
				if (idX2 < keyX || (idX2 == keyX && idY2 < keyY)) {
					return false;
				} else {
					keyX = idX2;
					keyY = idY2;
				}
				posXY++;
			}
			while (iterX != iterXEnd) {
				if (posX == lenX) {
					iterX++;
					if (iterX != iterXEnd) {
						bufferX = iterX->second->getBuffer();
						lenX = iterX->second->getSize();
						posX = 0;
						continue;
					} else {
						break;
					}
				}
				idX1 = bufferX[posX * IDCount];
				idY1 = bufferX[posX * IDCount + 1];
				if (idX1 < keyX || (idX1 == keyX && idY1 < keyY)) {
					return false;
				} else {
					keyX = idX1;
					keyY = idY1;
				}
				posX++;
			}
		}
		return true;
	}

	void printEntityIDBuffer(EntityIDBuffer *buffer) {
		size_t size = buffer->getSize();
		int count = 0;
		ID *idBuffer = buffer->getBuffer();
		int IDCount = buffer->getIDCount();

		for (size_t i = 0; i < size; i++) {
			if (IDCount == 1) {
				cout << "ID:" << idBuffer[i] << ' ';
			} else if (IDCount == 2) {
				cout << "ID:" << idBuffer[i * IDCount] << " ID:" << idBuffer[i * IDCount + 1] << " ";
			}
			count++;
			if (count % 10 == 0)
				cout << endl;
		}
		cout << endl;
	}

	void print() {
		map<ID, EntityIDBuffer*>::iterator iter;
		int k = 0;
		for (iter = xTempBuffer.begin(); iter != xTempBuffer.end(); iter++) {
			cout << "xTempBuffer " << k++ << " :" << endl;
			printEntityIDBuffer(iter->second);
		}
		for (iter = xyTempBuffer.begin(), k = 0; iter != xyTempBuffer.end(); iter++, k++) {
			cout << "xyTempBuffer " << k << " :" << endl;
			printEntityIDBuffer(iter->second);
		}
	}
};

#endif /* SUBTASKPACKAGE_H_ */
