#include "TempFile.h"
#include <sstream>
#include <cassert>
#include <cstring>
//---------------------------------------------------------------------------
// RDF-3X
// (c) 2008 Thomas Neumann. Web site: http://www.mpi-inf.mpg.de/~neumann/rdf3x
//
// This work is licensed under the Creative Commons
// Attribution-Noncommercial-Share Alike 3.0 Unported License. To view a copy
// of this license, visit http://creativecommons.org/licenses/by-nc-sa/3.0/
// or send a letter to Creative Commons, 171 Second Street, Suite 300,
// San Francisco, California, 94105, USA.
//---------------------------------------------------------------------------
using namespace std;
//---------------------------------------------------------------------------
/// The next id
triplebit::atomic<size_t> TempFile::id(0);
//---------------------------------------------------------------------------
string TempFile::newSuffix()
// Construct a new suffix
{
	stringstream buffer;
	buffer << '.' << (id++);
	return buffer.str();
}

TempFile::TempFile(const string& baseName) : baseName(baseName), fileName(baseName + newSuffix()), out(fileName.c_str(), ios::out | ios::binary | ios::trunc), writePos(0)
// Constructor
{
}

TempFile::~TempFile()
// Destructor
{
//	discard();
	if(id == ULONG_LONG_MAX){
		id = 0;
	}
}

void TempFile::flush()
// Flush the file
{
	if (writePos) {
		out.write(writeBuffer, writePos);
		writePos = 0;
	}
	out.flush();
}

void TempFile::close()
// Close the file
{
	flush();
	out.close();
}

void TempFile::discard()
// Discard the file
{
	close();
	remove(fileName.c_str());
}

void TempFile::memcpy(const uchar *startPtr, size_t size){
	out.write((const char *)startPtr, size);
}

void TempFile::writeID(ID id) {
	if (writePos + sizeof(ID) > bufferSize) {
		out.write(writeBuffer, writePos);
		writePos = 0;
	}
	::memcpy(writeBuffer + writePos, &id, sizeof(ID));
	writePos += sizeof(ID);
}

void TempFile::writeTS(ID id) {
	if (writePos + sizeof(TimeStamp) > bufferSize) {
		out.write(writeBuffer, writePos);
		writePos = 0;
	}
	::memcpy(writeBuffer + writePos, &id, sizeof(TimeStamp));
	writePos += sizeof(TimeStamp);
}

void TempFile::writeIDID(ID id1, ID id2){
	writeID(id1); writeID(id2);
}
void TempFile::writeIDIDID(ID id1, ID id2, ID id3){
	writeID(id1); writeID(id2); writeID(id3);
}
void TempFile::writeIDIDIDTS(ID id1, ID id2, ID id3, TimeStamp timestamp){
	writeID(id1); writeID(id2); writeID(id3); writeTS(timestamp);
}

void TempFile::readID(const uchar*& reader, ID& id){
	::memcpy(&id, reader, sizeof(ID));
	reader += sizeof(ID);
}

void TempFile::readTS(const uchar*& reader, TimeStamp& id){
	::memcpy(&id, reader, sizeof(TimeStamp));
	reader += sizeof(TimeStamp);
}

void TempFile::readIDID(const uchar*& reader, ID& id1, ID& id2){
	readID(reader, id1); 
	readID(reader, id2);
}
void TempFile::readIDIDID(const uchar*& reader, ID& id1, ID& id2, ID& id3){
	readID(reader, id1); 
	readID(reader, id2); 
	readID(reader, id3);
}
void TempFile::readIDIDIDTS(const uchar*& reader, ID& id1, ID& id2, ID& id3, TimeStamp& t){
	readID(reader, id1); 
	readID(reader, id2); 
	readID(reader, id3);
	readTS(reader, t);
}

const uchar* TempFile::skipID(const uchar* reader) {
	return reader + sizeof(ID);
}
const uchar* TempFile::skipIDID(const uchar* reader) {
	return reader + sizeof(ID)*2;
}
const uchar* TempFile::skipIDIDID(const uchar* reader) {
	return reader + sizeof(ID) * 3;
}

int TempFile::compare12(const uchar* left, const uchar* right) {
	ID l1,l2, r1,r2;
	readIDID(left, l1, l2);
	readIDID(right, r1, r2);
	return cmpTwo(l1, l2, r1, r2);
}
int TempFile::compare21(const uchar* left, const uchar* right) {
	ID l1, l2, r1, r2;
	readIDID(left, l1, l2);
	readIDID(right, r1, r2);
	return cmpTwo(l2, l1, r2, r1);
}

//---------------------------------------------------------------------------
//��������д���ļ�
void TempFile::write(unsigned len, const uchar* data)
// Raw write
{
	// Fill the buffer
	if (writePos + len > bufferSize) {
		unsigned remaining = bufferSize - writePos;
		::memcpy(writeBuffer + writePos, data, remaining);
		out.write(writeBuffer, bufferSize);
		writePos = 0;
		len -= remaining;
		data += remaining;
	}
	// Write big chunks if any
	if (writePos + len > bufferSize) {
		assert(writePos == 0);
		unsigned chunks = len / bufferSize;
		out.write((const char*)data, chunks * bufferSize);
		len -= chunks * bufferSize;
		data += chunks * bufferSize;
	}
	// And fill the rest
	::memcpy(writeBuffer + writePos, data, len);
	writePos += len;
}
//---------------------------------------------------------------------------
////////////////////////////////////////////////////////////////////////////////////////

#if defined(WIN32)||defined(__WIN32__)||defined(_WIN32)
#define CONFIG_WINDOWS
#endif
#ifdef CONFIG_WINDOWS
#include <windows.h>
#else
#include <sys/mman.h>
#include <sys/types.h>
#include <fcntl.h>
#include <unistd.h>
#endif

#include <stdio.h>
//---------------------------------------------------------------------------
// RDF-3X
// (c) 2008 Thomas Neumann. Web site: http://www.mpi-inf.mpg.de/~neumann/rdf3x
//
// This work is licensed under the Creative Commons
// Attribution-Noncommercial-Share Alike 3.0 Unported License. To view a copy
// of this license, visit http://creativecommons.org/licenses/by-nc-sa/3.0/
// or send a letter to Creative Commons, 171 Second Street, Suite 300,
// San Francisco, California, 94105, USA.
//----------------------------------------------------------------------------
// OS dependent data
struct MemoryMappedFile::Data {
#ifdef CONFIG_WINDOWS
	/// The file
	HANDLE file;
	/// The mapping
	HANDLE mapping;
#else
	/// The file
	int file;
	/// The mapping
	void* mapping;
#endif
};
//----------------------------------------------------------------------------
MemoryMappedFile::MemoryMappedFile() :
		data(0), begin(0), end(0)
// Constructor
{
}
//----------------------------------------------------------------------------
MemoryMappedFile::~MemoryMappedFile()
// Destructor
{
	close();
}
//----------------------------------------------------------------------------
bool MemoryMappedFile::open(const char* name)
// Open
		{
	if (!name)
		return false;
	close();

#ifdef CONFIG_WINDOWS
	HANDLE file = CreateFile(name, GENERIC_READ, FILE_SHARE_READ, 0,
	OPEN_EXISTING, 0, 0);
	if (file == INVALID_HANDLE_VALUE)
		return false;
	DWORD size = GetFileSize(file, 0);
	HANDLE mapping = CreateFileMapping(file, 0, PAGE_READONLY, 0, size, 0);
	if (mapping == INVALID_HANDLE_VALUE) {
		CloseHandle(file);
		return false;
	}
	begin = static_cast<char*>(MapViewOfFile(mapping, FILE_MAP_READ, 0, 0, size));
	if (!begin) {
		CloseHandle(mapping);
		CloseHandle(file);
		return false;
	}
	end = begin + size;
#else
	int file=::open(name,O_RDONLY);
	if (file<0) return false;
	size_t size=lseek(file,0,SEEK_END);
	if (!(~size)) {::close(file); return false;}
	void* mapping=mmap(0,size,PROT_READ,MAP_PRIVATE,file,0);
	if (mapping == MAP_FAILED) {
		::close(file);
		return false;
	}
	begin=static_cast<uchar*>(mapping);
	end=begin+size;
#endif
	data = new Data();
	data->file = file;
	data->mapping = mapping;

	return true;
}
//----------------------------------------------------------------------------
void MemoryMappedFile::close()
// Close
{
	if (data) {
#ifdef CONFIG_WINDOWS
		UnmapViewOfFile(const_cast<uchar*>(begin));
		CloseHandle(data->mapping);
		CloseHandle(data->file);
#else
		munmap(data->mapping,end-begin);
		::close(data->file);
#endif
		delete data;
		data = 0;
		begin = end = 0;
	}
}
unsigned sumOfItAll;
//----------------------------------------------------------------------------
void MemoryMappedFile::prefetch(const uchar* start, const uchar* end)
// Ask the operating system to prefetch a part of the file
		{
	if ((end < start) || (!data))
		return;

#ifdef CONFIG_WINDOWS
	// XXX todo
#else
	posix_fadvise(data->file,start-begin,end-start+1,POSIX_FADV_WILLNEED);
#endif
}
//----------------------------------------------------------------------------
