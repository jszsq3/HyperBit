/*
 * Tasks.h
 *
 *  Created on: 2014-3-6
 *      Author: root
 */

#ifndef TASKS_H_
#define TASKS_H_

#include "Tools.h"
#include "../TripleBit.h"
#include "../TripleBitQueryGraph.h"
#include "subTaskPackage.h"
#include "IndexForTT.h"

class SubTrans: private Uncopyable {
public:
	struct timeval transTime;
	ID sourceWorkerID;
	ID minID;
	ID maxID;
	TripleBitQueryGraph::OpType operationType;
	TripleNode triple;
	SubTrans *next;

	std::tr1::shared_ptr<IndexForTT> indexForTT; //无用

	SubTrans(timeval &transtime, ID sWorkerID, ID miID, ID maID, TripleBitQueryGraph::OpType &opType, TripleNode &trip) :
			transTime(transtime), sourceWorkerID(sWorkerID), minID(miID), maxID(maID), operationType(opType), triple(
					trip) {
		next = NULL;
	}

	SubTrans(timeval &transtime, ID sWorkerID, ID miID, ID maID,
				TripleBitQueryGraph::OpType &opType,
				TripleNode &trip, std::tr1::shared_ptr<IndexForTT> index_forTT) :
				transTime(transtime), sourceWorkerID(sWorkerID), minID(miID), maxID(
						maID), operationType(opType), triple(
						trip) , indexForTT(index_forTT){
		next = NULL;
		}

	SubTrans(){
		next = NULL;
	}

};

class ChunkTask: private Uncopyable {
public:
	timeval chunkTaskTime;
	TripleBitQueryGraph::OpType operationType;
	ChunkTriple Triple;
	std::tr1::shared_ptr<SubTaskPackageForDelete> taskPackageForDelete;
	std::tr1::shared_ptr<subTaskPackage> taskPackage;
	std::tr1::shared_ptr<IndexForTT> indexForTT;
	ID updateSubjectID;
	ID updateObjectID;
	bool opSO;
	ID chunkID;

	ChunkTask *next;
	ChunkTask() {
		next = NULL;
	}

	//用于测试insert data任务完成计数
	int time;

	ChunkTask(timeval &chunkTaskTime, TripleBitQueryGraph::OpType opType, ID subjectID, ID predicateID, ID objectID,
			TripleNode::Op operation, bool opSO, ID chunkID) :
			chunkTaskTime(chunkTaskTime), operationType(opType), Triple( { subjectID, predicateID, objectID,
					operation }), opSO(opSO), chunkID(chunkID) {
		next = NULL;
	}

	//用于测试insert data任务完成计数
	/*ChunkTask(timeval &chunkTaskTime, TripleBitQueryGraph::OpType opType, ID subjectID, ID predicateID, ID objectID, TripleNode::Op operation, OrderByType opSO, ID chunkID, int& time) :
	 chunkTaskTime(chunkTaskTime), operationType(opType), Triple( { subjectID, predicateID, objectID, operation }), opSO(opSO), chunkID(chunkID), time(time) {
	 }*/

	ChunkTask(timeval& chunkTaskTime, TripleBitQueryGraph::OpType opType, ID subjectID, ID predicateID, ID objectID,
		TripleNode::Op operation, std::tr1::shared_ptr<SubTaskPackageForDelete> task_Package,bool opSO, ID chunkID) :
		chunkTaskTime(chunkTaskTime), operationType(opType), Triple({ subjectID, predicateID, objectID,operation }), 
		taskPackageForDelete(task_Package), opSO(opSO), chunkID(chunkID) {
	}

	ChunkTask(timeval& chunkTaskTime, TripleBitQueryGraph::OpType opType, ID subjectID, ID predicateID, ID objectID,
		TripleNode::Op operation, std::tr1::shared_ptr<subTaskPackage> task_Package, bool opSO, ID chunkID) :
		chunkTaskTime(chunkTaskTime), operationType(opType), Triple({ subjectID, predicateID, objectID,operation }),
		taskPackage(task_Package), opSO(opSO), chunkID(chunkID) {
	}

	ChunkTask(timeval& chunkTaskTime, TripleBitQueryGraph::OpType opType, ID subjectID, ID predicateID, ID objectID, 
		ID updateSubjectID, ID updateObjectID, TripleNode::Op operation,
		std::tr1::shared_ptr<SubTaskPackageForDelete> task_Package, bool opSO, ID chunkID) :
		chunkTaskTime(chunkTaskTime), operationType(opType), Triple({ subjectID, predicateID, objectID,operation }),
		updateSubjectID(updateSubjectID), updateObjectID(updateObjectID),
		taskPackageForDelete(task_Package), opSO(opSO), chunkID(chunkID) {
	}

	ChunkTask(TripleBitQueryGraph::OpType opType, ID subjectID, ID predicateID, ID objectID,
		TripleNode::Op operation, std::tr1::shared_ptr<subTaskPackage> task_Package,
		std::tr1::shared_ptr<IndexForTT> index_ForTT) :
		operationType(opType), Triple({ subjectID, predicateID, objectID, operation }),
		taskPackage(task_Package), indexForTT(index_ForTT) {
	} //partitionmaster

	ChunkTask(TripleBitQueryGraph::OpType opType, ID subjectID, ID predicateID, ID objectID, 
		TripleNode::Op operation, std::tr1::shared_ptr<SubTaskPackageForDelete> task_Package,
		std::tr1::shared_ptr<IndexForTT> index_ForTT) :
		operationType(opType), Triple({ subjectID, predicateID, objectID,operation }), 
		taskPackageForDelete(task_Package), indexForTT(index_ForTT) {
	} //partitionmaster

	ChunkTask(TripleBitQueryGraph::OpType opType, ID subjectID, ID predicateID, ID objectID, 
		ID updateSubjectID, ID updateObjectID, TripleNode::Op operation,
		std::tr1::shared_ptr<SubTaskPackageForDelete> task_Package, std::tr1::shared_ptr<IndexForTT> index_ForTT) :
		operationType(opType), Triple({ subjectID, predicateID, objectID, operation }), 
		updateSubjectID(updateSubjectID), updateObjectID(updateObjectID), taskPackageForDelete(task_Package) {
	} //partitionmaster

	~ChunkTask() {
	}

	void setChunkTask(timeval &chunkTaskTime, TripleBitQueryGraph::OpType opType, TripleNode &tripleNode, bool opSO, ID chunkID) {
		this->chunkTaskTime = chunkTaskTime;
		this->operationType = opType;
		this->Triple.subjectID = tripleNode.subjectID;
		this->Triple.predicateID = tripleNode.predicateID;
		this->Triple.objectID = tripleNode.objectID;
		this->Triple.operation = tripleNode.scanOperation;
		this->opSO = opSO;
		this->chunkID = chunkID;
	}
};

#endif /* TASKS_H_ */
