/*
 * MMapBuffer.cpp
 *
 *  Created on: Oct 6, 2010
 *      Author: root
 */
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <sys/mman.h>
#include <sys/uio.h>
#include <stdio.h>
#include <errno.h>
#include "MMapBuffer.h"

MMapBuffer::MMapBuffer(const char *_filename) : filename(_filename)
{
	// fd = open(filename.c_str(), O_RDONLY, 0666);
	fd = open(filename.c_str(), O_RDWR, 0666);
	if (fd < 0)
	{
		perror(_filename);
		MessageEngine::showMessage("Open file error", MessageEngine::ERROR);
	}

	size = lseek(fd, 0, SEEK_END);
	if (lseek(fd, 0, SEEK_SET) != 0)
	{
		perror(_filename);
		MessageEngine::showMessage("lseek file error", MessageEngine::ERROR);
	}

	mmap_addr = (char volatile *)mmap(NULL, size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
	if (mmap_addr == MAP_FAILED)
	{
		perror(_filename);
		cout << "size: " << size << endl;
		MessageEngine::showMessage("map file to memory error", MessageEngine::ERROR);
	}
}

MMapBuffer::MMapBuffer(const char *_filename, size_t initSize) : filename(_filename)
{
	// TODO Auto-generated constructor stub
	fd = open(filename.c_str(), O_CREAT | O_RDWR, 0666);
	if (fd < 0)
	{
		perror(_filename);
		MessageEngine::showMessage("Create map file error", MessageEngine::ERROR);
	}

	size = lseek(fd, 0, SEEK_END);
	if (size < initSize)
	{
		size = initSize;
		if (ftruncate(fd, initSize) != 0)
		{
			perror(_filename);
			MessageEngine::showMessage("ftruncate file error", MessageEngine::ERROR);
		}
	}
	if (lseek(fd, 0, SEEK_SET) != 0)
	{
		perror(_filename);
		MessageEngine::showMessage("lseek file error", MessageEngine::ERROR);
	}

	mmap_addr = (char volatile *)mmap(NULL, size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
	if (mmap_addr == MAP_FAILED)
	{
		perror(_filename);
		cout << "size: " << size << endl;
		MessageEngine::showMessage("map file to memory error", MessageEngine::ERROR);
	}
}

MMapBuffer::~MMapBuffer()
{
	// TODO Auto-generated destructor stub
}

void MMapBuffer::close()
{
	// TODO Auto-generated destructor stub
	flush();
	munmap((char *)mmap_addr, size);
	::close(fd);
}

Status MMapBuffer::flush()
{
	if (msync((char *)mmap_addr, size, MS_SYNC) == 0)
	{
		return OK;
	}

	return ERROR;
}

uchar *MMapBuffer::resize(size_t incrementSize)
{
	size_t newsize = size + incrementSize;

	// cout<<filename.c_str()<<": "<<__FUNCTION__<<" begin: "<<size<<" : "<<newsize<<endl;

	uchar *new_addr = NULL;
	if (munmap((char *)mmap_addr, size) != 0)
	{
		MessageEngine::showMessage("resize-munmap error!", MessageEngine::ERROR);
		return NULL;
	}

	if (ftruncate(fd, newsize) != 0)
	{
		MessageEngine::showMessage("resize-ftruncate file error!", MessageEngine::ERROR);
		return NULL;
	}

	if ((new_addr = (uchar *)mmap(NULL, newsize, PROT_READ | PROT_WRITE, MAP_FILE | MAP_SHARED, fd, 0)) == (uchar *)MAP_FAILED)
	{
		MessageEngine::showMessage("mmap buffer resize error!", MessageEngine::ERROR);
		return NULL;
	}

	// cout<<filename.c_str()<<": "<<__FUNCTION__<<" begin: "<<size<<" : "<<newsize<<endl;
	mmap_addr = (char volatile *)new_addr;

	::memset((uchar *)mmap_addr + size, 0, incrementSize);

	// cout<<filename.c_str()<<": "<<__FUNCTION__<<" end: "<<size<<" : "<<newsize<<endl;
	size = newsize;
	return (uchar *)mmap_addr;
}

void MMapBuffer::discard()
{
	munmap((char *)mmap_addr, size);
	::close(fd);
	unlink(filename.c_str());
}

uchar *MMapBuffer::getBuffer()
{
	return (uchar *)mmap_addr;
}

uchar *MMapBuffer::getBuffer(int pos)
{
	return (uchar *)mmap_addr + pos;
}

Status MMapBuffer::resize(size_t new_size, bool clear)
{
	// size_t newsize = size + incrementSize;
	uchar *new_addr = NULL;
	if (munmap((char *)mmap_addr, size) != 0 || ftruncate(fd, new_size) != 0 || (new_addr = (uchar *)mmap(NULL, new_size, PROT_READ | PROT_WRITE, MAP_FILE | MAP_SHARED, fd, 0)) == (uchar *)MAP_FAILED)
	{
		MessageEngine::showMessage("mmap buffer resize error!", MessageEngine::ERROR);
		return ERROR;
	}

	mmap_addr = (char volatile *)new_addr;

	::memset((uchar *)mmap_addr + size, 0, new_size - size);
	size = new_size;
	return OK;
}

void MMapBuffer::memset(char value)
{
	::memset((char *)mmap_addr, value, size);
}

MMapBuffer *MMapBuffer::create(const char *filename, size_t initSize)
{
	MMapBuffer *buffer = new MMapBuffer(filename, initSize);
	return buffer;
}
