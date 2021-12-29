/*
 * ComplexTripleBuffer.h
 *
 *  Created on: 2017年6月18日
 *      Author: XuQingQing
 */

#include <TripleBit.h>
#include "MMapBuffer.h"

//所有S对应的E，或者所有T对应的E
class NodeEdgeBuffer {
public:
	struct Couple { //索引
		double stValue; //S、T
		size_t offset; //偏移量
	};
	struct NodeEdge {
		double stValue;
		vector<ID> edgeIDs;
	};
private:
	NODEEDGETYPE stType;
	MMapBuffer* buffer;
	uchar* writer;
	Couple* index;
	uint usedSpace;
	uint indexPos, indexSize;
	Couple* pos, *posLimit;
	bool first;

public:
	NodeEdgeBuffer(const string path, NODEEDGETYPE stType);
	virtual ~NodeEdgeBuffer();
	template<typename T>
	Status addNodeEdge(T stValue, const vector<ID>& edges, char objType = STRING);
	template<typename T>
	Status getNodeEdge(T stValue, vector<ID>& edges, char objType = STRING);
	bool findLocation(double stValue);
	Status save(MMapBuffer*& indexBuffer);
	static NodeEdgeBuffer* load(NODEEDGETYPE stType, const string path, uchar*& indexBuffer);
private:
	void decodeBuffer(const uchar* begin, const uchar* end, double stValue, vector<ID>& edges, char objType = STRING);
};

class EdgeStartTargetBuffer {
public:
	struct Couple {
		ID edgeID;
		size_t offset;
	};
	struct Triple {
		ID edge;
		ID start;
		double target;
	};
private:
	MMapBuffer* buffer;
	uchar* writer;
	Couple* index;
	uint usedSpace;
	uint indexPos, indexSize;
	Couple* pos, *posLimit;
	bool first;
public:
	EdgeStartTargetBuffer(const string path);
	virtual ~EdgeStartTargetBuffer();
	Status addTriple(ID edgeID, ID startID, double target, char objType = STRING);
	bool findLocation(ID edgeID);
	Status save(MMapBuffer*& indexBuffer);
	static EdgeStartTargetBuffer* load(const string path, uchar*& indexBuffer);
private:
	void decodeBuffer(const uchar* begin, const uchar* end, ID edgeID);
};

