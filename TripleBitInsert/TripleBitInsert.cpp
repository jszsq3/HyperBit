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

char* DATABASE_PATH;
char* QUERY_PATH;
int gthread, cthread, tripleSize, windowSize;

int main(int argc, char* argv[]) {
	if (argc < 4) {
		fprintf(stderr, "Usage: %s <TripleBit Directory> <Query files Directory>\n", argv[0]);//argv[1] = database/  argv[2] = dataset 
		return -1;
	}

	DATABASE_PATH = argv[1];
	QUERY_PATH = argv[2];

	timeval start, end;
	gettimeofday(&start, NULL);
	TripleBitRepository* repo = TripleBitRepository::create(argv[1]);
	gettimeofday(&end, NULL);

	if (repo == NULL) {
		return -1;
	}

	gthread = atoi(argv[3]);//�����߳���
	cthread = atoi(argv[4]);//��ѯ�߳���
	windowSize = atoi(argv[5]);
	repo->insertData(argv[2]);


//
	/*if (argc == 6) {
	 //repo->cmd_line_sm(stdin, stdout, argv[2]);
	 repo->cmd_line_sm(stdin, stdout, argv[2], argv[3]);
	 }*//*else if (argc == 5 && strcmp(argv[4], "--cold") == 0) {
repo->cmd_line_cold(stdin, stderr, argv[3]);
} else if (argc == 5 && strcmp(argv[4], "--warm") == 0) {
repo->cmd_line_warm(stdin, stderr, argv[3]);
}*/

	delete repo;
	repo = NULL;


	return 0;
}
