/*
 * ResultIDBuffer.h
 *
 *  Created on: 2013-12-18
 *      Author: root
 */

#ifndef RESULTIDBUFFER_H_
#define RESULTIDBUFFER_H_

#include "TripleBit.h"
#include "EntityIDBuffer.h"
#include "comm/subTaskPackage.h"

using namespace boost;

class ResultIDBuffer {
private:
	bool isEntityID;
	EntityIDBuffer *buffer;
	std::tr1::shared_ptr<subTaskPackage> taskPackage;
	int IDCount;

public:
	ResultIDBuffer() {
	}
	ResultIDBuffer(std::tr1::shared_ptr<subTaskPackage> package);
	~ResultIDBuffer();
	bool isEntityIDBuffer() {
		return isEntityID;
	}
	std::tr1::shared_ptr<subTaskPackage> getTaskPackage() {
		return taskPackage;
	}
	EntityIDBuffer *getEntityIDBuffer();
	void transForEntityIDBuffer();
	void setEntityIDBuffer(EntityIDBuffer *buf);
	void setTaskPackage(std::tr1::shared_ptr<subTaskPackage> package);

	void getMinMax(ID &min, ID &max);
	int getIDCount();
	size_t getSize();
	Status sort(int sortKey);
	ID *getBuffer();
};

#endif /* RESULTIDBUFFER_H_ */
