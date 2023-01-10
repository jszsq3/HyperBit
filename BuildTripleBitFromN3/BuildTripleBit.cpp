/*
 * BuildTripleBit.cpp
 *
 *  Created on: Apr 12, 2011
 *      Author: root
 */

#include "../TripleBit/TripleBitBuilder.h"
#include "../TripleBit/OSFile.h"

char* DATABASE_PATH;
int main(int argc, char* argv[])
{
	if (argc != 3) {
		fprintf(stderr, "Usage: %s <N3 file name> <Database Directory>\n", argv[0]);
		return -1;
	}

	if (OSFile::directoryExists(argv[2]) == false) {
		OSFile::mkdir(argv[2]);
	}

	DATABASE_PATH = argv[2];

	timeval start, end;
	TripleBitBuilder* builder = new TripleBitBuilder(argv[2]);

	gettimeofday(&start, 0);
	builder->startBuildN3(argv[1]);
	gettimeofday(&end, 0);
	cout << "build time: " << ((end.tv_sec - start.tv_sec) * 1000000 + end.tv_usec - start.tv_usec) / 1000000.0 << "s" << endl;
	
	// builder->print(1);


	builder->endBuild();

	

	delete builder;

	return 0;
}
