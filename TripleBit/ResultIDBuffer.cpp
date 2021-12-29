/*
 * ResultIDBuffer.cpp
 *
 *  Created on: 2013-12-18
 *      Author: root
 */

#include "ResultIDBuffer.h"

ResultIDBuffer::ResultIDBuffer(std::tr1::shared_ptr<subTaskPackage> package) :
		isEntityID(false), buffer(NULL), taskPackage(package) {
	if (taskPackage->xTempBuffer.size() > 0) {
		IDCount = taskPackage->xTempBuffer.begin()->second->getIDCount();
	} else {
		IDCount = taskPackage->xyTempBuffer.begin()->second->getIDCount();
	}
}

ResultIDBuffer::~ResultIDBuffer() {
	if (buffer != NULL) {
		buffer = NULL;
	}
}

EntityIDBuffer *ResultIDBuffer::getEntityIDBuffer() {
	transForEntityIDBuffer();
	return buffer;
}

void ResultIDBuffer::transForEntityIDBuffer() {
	if (!isEntityID) {
		isEntityID = true;
		buffer = taskPackage->getTaskResult();
	}
}

void ResultIDBuffer::getMinMax(ID &min, ID &max) {
	transForEntityIDBuffer();
	buffer->getMinMax(min, max);
}

int ResultIDBuffer::getIDCount() {
	return IDCount;
}

void ResultIDBuffer::setTaskPackage(std::tr1::shared_ptr<subTaskPackage> package) {
	if (buffer != NULL) {
		delete buffer;
		buffer = NULL;
	}
	taskPackage = package;
	isEntityID = false;
}

void ResultIDBuffer::setEntityIDBuffer(EntityIDBuffer *buf) {
	buffer = buf;
	isEntityID = true;
}

Status ResultIDBuffer::sort(int sortKey) {
	if (isEntityID) {
		buffer->sort(sortKey);
	} else {
		map<ID, EntityIDBuffer*>::iterator iter, iterEnd;
		iter = taskPackage->xTempBuffer.begin();
		iterEnd = taskPackage->xTempBuffer.end();
		for (; iter != iterEnd; iter++) {
			iter->second->sort(sortKey);
		}
		iter = taskPackage->xyTempBuffer.begin();
		iterEnd = taskPackage->xyTempBuffer.end();
		for (; iter != iterEnd; iter++) {
			iter->second->sort(sortKey);
		}
	}
	return OK;
}

size_t ResultIDBuffer::getSize() {
	if (isEntityID) {
		return buffer->getSize();
	} else {
		map<ID, EntityIDBuffer*>::iterator iter;
		size_t totalSize = 0;
		for (iter = taskPackage->xTempBuffer.begin(); iter != taskPackage->xTempBuffer.end(); iter++) {
			totalSize += iter->second->getSize();
		}
		for (iter = taskPackage->xyTempBuffer.begin(); iter != taskPackage->xyTempBuffer.end(); iter++) {
			totalSize += iter->second->getSize();
		}
		return totalSize;
	}
}

