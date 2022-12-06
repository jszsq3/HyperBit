#ifndef _TEMPFILE_H_
#define _TEMPFILE_H_

#include "TripleBit.h"
#include <fstream>
#include <string>
#include "util/atomic.hpp"
//---------------------------------------------------------------------------
#if defined(_MSC_VER)
typedef unsigned __int64 uint64_t;
#else
#include <stdint.h>
#endif
//---------------------------------------------------------------------------
/// A temporary file
class TempFile {
private:
	/// The next id
	static triplebit::atomic<size_t> id;

	/// The base file name
	std::string baseName;
	/// The file name
	std::string fileName;
	/// The output
	std::ofstream out;

	/// The buffer size
	static const uint bufferSize = 16384;
	/// The write buffer
	char writeBuffer[bufferSize];
	/// The write position
	uint writePos;

	/// Construct a new suffix
	static std::string newSuffix();

public:
	/// Constructor
	TempFile(const std::string& baseName);
	/// Destructor
	~TempFile();

	/// Get the base file name
	const std::string& getBaseFile() const {
		return baseName;
	}
	/// Get the file name
	const std::string& getFile() const {
		return fileName;
	}

	/// Flush the file
	void flush();
	/// Close the file
	void close();
	/// Discard the file
	void discard();

	void memcpy(const uchar* startPtr, size_t size);

	void writeID(ID id);
	void writeTS(TimeStamp timestamp);
	void writeIDID(ID id1,ID id2);
	void writeIDIDID(ID id1, ID id2, ID id3);
	void writeIDIDIDTS(ID id1, ID id2, ID id3, TimeStamp timestamp);

	static void readID(const uchar*& reader, ID& id);
	static void readTS(const uchar*& reader, TimeStamp& id);
	static void readIDID(const uchar*& reader, ID& id1, ID& id2);
	static void readIDIDID(const uchar* &reader, ID& id1, ID& id2, ID& id3);
	static void readIDIDIDTS(const uchar* &reader, ID& id1, ID& id2, ID& id3, TimeStamp& t);

	static const uchar* skipID(const uchar* reader);
	static const uchar* skipIDID(const uchar* reader);
	static const uchar* skipIDIDID(const uchar* reader);

	static int compare12(const uchar* left, const uchar* right);
	static int compare21(const uchar* left, const uchar* right);

	void write(unsigned len, const uchar* data);


	static inline int cmpOne(ID l, ID r) {
		return (l < r) ? -1 : ((l > r) ? 1 : 0);
	}
	static inline int cmpTwo(ID l1, ID l2, ID r1, ID r2) {
		int c = cmpOne(l1, r1);
		if (c)
			return c;
		return cmpOne(l2, r2);
	}
	static inline int cmpThree(ID l1, ID l2, ID l3, ID r1, ID r2, ID r3) {
		int c = cmpOne(l1, r1);
		if (c)
			return c;
		c = cmpOne(l2, r2);
		if (c)
			return c;
		return cmpOne(l3, r3);
	}
};

//----------------------------------------------------------------------------
/// Maps a file read-only into memory
class MemoryMappedFile {
private:
	/// os dependent data
	struct Data;

	/// os dependen tdata
	Data* data;
	/// Begin of the file
	const uchar* begin;
	/// End of the file
	const uchar* end;

public:
	/// Constructor
	MemoryMappedFile();
	/// Destructor
	~MemoryMappedFile();

	/// Open
	bool open(const char* name);
	/// Close
	void close();

	/// Get the begin
	const uchar* getBegin() const {
		return begin;
	}
	/// Get the end
	const uchar* getEnd() const {
		return end;
	}

	/// Ask the operating system to prefetch a part of the file
	void prefetch(const uchar* start, const uchar* end);
};
//---------------------------------------------------------------------------
#endif
