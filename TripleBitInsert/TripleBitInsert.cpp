/*
 * TripleBitQuery.cpp
 *
 *  Created on: Apr 12, 2011
 *      Author: root
 */

#include "../TripleBit/TripleBit.h"
#include "../TripleBit/TripleBitRepository.h"
#include "../TripleBit/OSFile.h"
#include "../TripleBit/MMapBuffer.h"

char *DATABASE_PATH;
char *QUERY_PATH;
int gthread, cthread, tripleSize, windowSize;

int main(int argc, char *argv[])
{
	// if (argc < 4)
	// {
	// 	fprintf(stderr, "Usage: %s <TripleBit Directory> <Query files Directory>\n", argv[0]); // argv[1] = database/  argv[2] = dataset
	// 	return -1;
	// }

	DATABASE_PATH = argv[1];
	QUERY_PATH = argv[2];
	timeval start, end;
	gettimeofday(&start, NULL);
	TripleBitRepository *repo = TripleBitRepository::create(argv[1]);

	if (repo == NULL)
	{
		return -1;
	}

	repo->insertData(argv[2]);

	gettimeofday(&end, NULL);

	cout << "build time: " << ((end.tv_sec - start.tv_sec) * 1000000 + end.tv_usec - start.tv_usec) / 1000000.0 << "s" << endl;


	delete repo;
	repo = NULL;

	return 0;
}
