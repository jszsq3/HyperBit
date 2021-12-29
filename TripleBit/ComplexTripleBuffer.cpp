/*
 * ComplexTripleBuffer.cpp
 *
 *  Created on: 2017年6月18日
 *      Author: XuQingQing
 */

#include "ComplexTripleBuffer.h"
#include "MMapBuffer.h"
#include "MemoryBuffer.h"
#include "BitmapBuffer.h"

template<typename T>
extern uchar* writeData(uchar* writer, T data);
template<typename T>
extern const uchar* readData(const uchar* reader, T& data);

NodeEdgeBuffer::NodeEdgeBuffer(const string path, NODEEDGETYPE neType) :
		stType(neType) {
	buffer = new MMapBuffer(path.c_str(), COMPLEX_BUFFER_INIT_PAGE_COUNT * MemoryBuffer::pagesize);
	writer = (uchar*) buffer->getBuffer();
	usedSpace = 0;
	indexPos = 0;
	indexSize = 0; //MemoryBuffer::pagesize;
	index = NULL;
	first = true;
}

NodeEdgeBuffer::~NodeEdgeBuffer() {
	if (buffer != NULL) {
		buffer->close();
		delete buffer;
	}
	buffer = NULL;
	if (index != NULL) {
		delete index;
	}
	index = NULL;
}

template<typename T>
Status NodeEdgeBuffer::addNodeEdge(T stValue, const vector<ID>& edges, char objType) {
	unsigned len = sizeof(T) + sizeof(size_t) + sizeof(ID) * edges.size();

	if (first || usedSpace + len > buffer->getSize()) {
		usedSpace = writer - (uchar*) buffer->getBuffer();
		buffer->resize(COMPLEX_BUFFER_INCREMENT_PAGE_COUNT * MemoryBuffer::pagesize); //加大空间
		writer = (uchar*) buffer->getBuffer() + usedSpace;

		if ((indexPos + 1) >= indexSize) {
#ifdef DEBUF
			cout<<"indexPos: "<<indexPos<<" indexSize: "<<indexSize<<endl;
#endif
			index = (Couple*) realloc(index, indexSize * sizeof(Couple) + MemoryBuffer::pagesize * sizeof(Couple));
			if (index == NULL) {
				cout << "realloc StatisticsBuffer error" << endl;
				return ERR;
			}
			indexSize += MemoryBuffer::pagesize;
		}

		index[indexPos].stValue = stValue;
		index[indexPos].offset = usedSpace; //record offset，可以得出实体——谓词所在的块号

		indexPos++;
		first = false;
	}

	if (stType == STARTEDGE) {

	} else if (stType == TARGETEDGE) {
		writer = writeData(writer, objType); //OP统计信息O前需加objType
	}
	writer = writeData(writer, stValue);
	writer = writeData(writer, edges.size());
	copy(edges.begin(), edges.end(), writer); //未修改writer指针位置
	writer += sizeof(ID) * edges.size();
	usedSpace = writer - (uchar*) buffer->getBuffer();
	return OK;
}

template<typename T>
Status NodeEdgeBuffer::getNodeEdge(T stValue, vector<ID>& edges, char objType) {
	pos = index, posLimit = index + indexPos;
	findLocation(stValue); // get index location, that is pos
	if (pos->stValue > stValue)
		pos--;

	uint start = pos->offset;
	while (pos <= posLimit && pos->stValue <= stValue) {
		pos++;
	}

	uint end = pos->offset;
	if (pos == (index + indexPos))
		end = usedSpace;

	const uchar* begin = (uchar*) buffer->getBuffer() + start, *limit = (uchar*) buffer->getBuffer() + end;
	decodeBuffer(begin, limit, stValue, edges, objType);
	if (edges.size() != 0) {
		return OK;
	}
	return NOT_FOUND;
}
bool NodeEdgeBuffer::findLocation(double stValue) {
	int left = 0, right = posLimit - pos;
	int middle;

	while (left != right) {
		middle = left + ((right - left) / 2);
		if (stValue > pos[middle].stValue) {
			left = middle + 1;
		} else if ((!middle) || stValue > pos[middle - 1].stValue) {
			break;
		} else {
			right = middle;
		}
	}

	if (left == right) {
		return false;
	} else {
		pos = &pos[middle];
		return true;
	}

}
Status NodeEdgeBuffer::save(MMapBuffer*& indexBuffer) {
	uchar* writer;
	if (indexBuffer == NULL) {
		indexBuffer = MMapBuffer::create(string(string(DATABASE_PATH) + "/complexIndex").c_str(), indexPos * sizeof(Couple) + 2 * sizeof(unsigned));
		writer = indexBuffer->get_address();
	} else {
		size_t size = indexBuffer->getSize();
		indexBuffer->resize(indexPos * sizeof(Couple) + 2 * sizeof(unsigned));
		writer = indexBuffer->get_address() + size;
	}

	writer = writeData(writer, usedSpace);
	writer = writeData(writer, indexPos);

	memcpy(writer, (char*) index, indexPos * sizeof(Couple));
	free(index);
	indexBuffer->close();
	delete indexBuffer;
	indexBuffer = NULL;

	return OK;
}
NodeEdgeBuffer* NodeEdgeBuffer::load(NODEEDGETYPE stType, const string path, uchar*& indexBuffer) {
	NodeEdgeBuffer* stBuffer = new NodeEdgeBuffer(path, stType);

	indexBuffer = (uchar*) readData(indexBuffer, stBuffer->usedSpace);
	indexBuffer = (uchar*) readData(indexBuffer, stBuffer->indexPos);
#ifdef DEBUG
	cout<<__FUNCTION__<<"indexPos: "<<statBuffer->indexPos<<endl;
#endif
	// load index;
	stBuffer->index = (Couple*) indexBuffer;
	indexBuffer = indexBuffer + stBuffer->indexPos * sizeof(Couple);

	return stBuffer;
}
void NodeEdgeBuffer::decodeBuffer(const uchar* begin, const uchar* end, double stValue, vector<ID>& edges, char objType) {
	ID edgeID;
	size_t count;
	if (stType == STARTEDGE) {
		ID subjectID;
		while (begin + sizeof(ID) < end) {
			begin = readData(begin, subjectID);
			if (subjectID && begin + sizeof(size_t) <= end) {
				begin = readData(begin, count);
				if (subjectID < stValue) {
					if (begin + sizeof(ID) * count <= end) {
						begin += sizeof(ID) * count;
						continue;
					} else {
						return;
					}
				} else if (subjectID > stValue) {
					return;
				} else {
					while (count-- && begin + sizeof(ID) <= end) {
						begin = readData(begin, edgeID);
						edges.push_back(edgeID);
					}
				}
			} else {
				break;
			}
		}
	} /*else if (stType == TARGETEDGE) {
		char tempObjType;
		double tempObject;
		uint moveByteNum;
		int status;
		while (begin + sizeof(char) < end) {
			status = Chunk::getObjTypeStatus(begin, moveByteNum);
			if (status == DATA_EXSIT) {
				begin -= moveByteNum;
				begin = readData(begin, tempObjType);
				if (begin + Chunk::getLen(tempObjType) < end) {
					begin = Chunk::read(begin, tempObject, tempObjType);
					if (begin + sizeof(size_t) <= end) {
						begin = readData(begin, count);
						if (tempObject < stValue) {
							if (begin + sizeof(ID) * count <= end) {
								begin += sizeof(ID) * count;
								continue;
							} else {
								return;
							}
						} else if (tempObject > stValue) {
							return;
						} else if (tempObject == stValue && tempObjType == objType) {
							while (count-- && begin + sizeof(ID) <= end) {
								begin = readData(begin, edgeID);
								edges.push_back(edgeID);
							}
						} else {
							begin += sizeof(ID) * count;
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

EdgeStartTargetBuffer::EdgeStartTargetBuffer(const string path) {
	// TODO Auto-generated constructor stub
	buffer = new MMapBuffer(path.c_str(), COMPLEX_BUFFER_INIT_PAGE_COUNT * MemoryBuffer::pagesize);
	writer = (uchar*) buffer->getBuffer();
	usedSpace = 0;
	indexPos = 0;
	indexSize = 0; //MemoryBuffer::pagesize;
	index = NULL;
	first = true;
}

EdgeStartTargetBuffer::~EdgeStartTargetBuffer() {
	// TODO Auto-generated destructor stub
	if (buffer != NULL) {
		delete buffer;
	}
	buffer = NULL;
	if (index != NULL) {
		delete index;
	}
	index = NULL;
}

Status EdgeStartTargetBuffer::addTriple(ID edgeID, ID startID, double target, char objType) {
	unsigned len = sizeof(ID) + sizeof(ID) + Chunk::getLen(objType);

	if (first || usedSpace + len > buffer->getSize()) {
		usedSpace = writer - (uchar*) buffer->getBuffer();
		buffer->resize(COMPLEX_BUFFER_INCREMENT_PAGE_COUNT * MemoryBuffer::pagesize); //加大空间
		writer = (uchar*) buffer->getBuffer() + usedSpace;

		if ((indexPos + 1) >= indexSize) {
#ifdef DEBUF
			cout<<"indexPos: "<<indexPos<<" indexSize: "<<indexSize<<endl;
#endif
			index = (Couple*) realloc(index, indexSize * sizeof(Couple) + MemoryBuffer::pagesize * sizeof(Couple));
			if (index == NULL) {
				cout << "realloc StatisticsBuffer error" << endl;
				return ERR;
			}
			indexSize += MemoryBuffer::pagesize;
		}

		index[indexPos].edgeID = edgeID;
		index[indexPos].offset = usedSpace; //record offset，可以得出实体——谓词所在的块号

		indexPos++;
		first = false;
	}

	writer = writeData(writer, edgeID);
	writer = writeData(writer, startID);
	uchar* temp = writer;
	//Chunk::write(temp, target, objType);
	writer = (uchar*) temp;
	usedSpace = writer - (uchar*) buffer->getBuffer();
	return OK;
}
bool EdgeStartTargetBuffer::findLocation(ID edgeID) {
	int left = 0, right = posLimit - pos;
	int middle;

	while (left != right) {
		middle = left + ((right - left) / 2);

		if (edgeID > pos[middle].edgeID) {
			left = middle + 1;
		} else if ((!middle) || edgeID > pos[middle - 1].edgeID) {
			break;
		} else {
			right = middle;
		}
	}

	if (left == right) {
		return false;
	} else {
		pos = &pos[middle];
		return true;
	}
}
Status EdgeStartTargetBuffer::save(MMapBuffer*& indexBuffer) {

	uchar* writer;
	if (indexBuffer == NULL) {
		indexBuffer = MMapBuffer::create(string(string(DATABASE_PATH) + "/complexIndex").c_str(), indexPos * sizeof(Couple) + 2 * sizeof(unsigned));
		writer = indexBuffer->get_address();
	} else {
		size_t size = indexBuffer->getSize();
		indexBuffer->resize(indexPos * sizeof(Couple) + 2 * sizeof(unsigned));
		writer = indexBuffer->get_address() + size;
	}

	writer = writeData(writer, usedSpace);
	writer = writeData(writer, indexPos);

	memcpy(writer, (char*) index, indexPos * sizeof(Couple));
	free(index);

	return OK;
}
EdgeStartTargetBuffer* EdgeStartTargetBuffer::load(const string path, uchar*& indexBuffer) {
	EdgeStartTargetBuffer* stBuffer = new EdgeStartTargetBuffer(path);

	indexBuffer = (uchar*) readData(indexBuffer, stBuffer->usedSpace);
	indexBuffer = (uchar*) readData(indexBuffer, stBuffer->indexPos);
#ifdef DEBUG
	cout<<__FUNCTION__<<"indexPos: "<<statBuffer->indexPos<<endl;
#endif
	// load index;
	stBuffer->index = (Couple*) indexBuffer;
	indexBuffer = indexBuffer + stBuffer->indexPos * sizeof(Couple);

	return stBuffer;
}
void EdgeStartTargetBuffer::decodeBuffer(const uchar* begin, const uchar* end, ID edgeID) {

}
