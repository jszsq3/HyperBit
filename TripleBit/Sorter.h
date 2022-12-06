/*
 * Sorter.h
 * Create on: 2011-3-15
 * 	  Author: liupu
 */
#ifndef SORTER_H
#define SORTER_H
#include "TripleBit.h"

class TempFile;

/// Sort a temporary file
class Sorter {
public:
	/// Sort a file
	static void sort(TempFile& in, TempFile& out, const uchar* (*skip)(const uchar*), int (*compare)(const uchar*, const uchar*), bool eliminateDuplicates = false);
};
#endif /*SOTER_H*/
