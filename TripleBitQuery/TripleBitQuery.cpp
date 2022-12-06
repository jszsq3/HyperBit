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
	// if (argc < 4) {
	// 	fprintf(stderr, "Usage: %s <TripleBit Directory> <Query files Directory>\n", argv[0]);
	// 	return -1;
	// }

	DATABASE_PATH = argv[1];
	QUERY_PATH = argv[2];



	timeval start, end;
	gettimeofday(&start, NULL);
	cout<<"loading......"<<endl;
	TripleBitRepository* repo = TripleBitRepository::create(argv[1]);
	
	
	gettimeofday(&end, NULL);
	cout << "load time: " << ((end.tv_sec - start.tv_sec) * 1000000 + end.tv_usec - start.tv_usec) / 1000000.0 << "s" << endl;
	
	string qufi;
	cout<<">>";
	while(cin>>qufi){
		string queryfile = argv[2]+qufi;
		gettimeofday(&start, NULL);
		repo->testquery(queryfile);
		gettimeofday(&end, NULL);
		cout << "query time: " << ((end.tv_sec - start.tv_sec) * 1000000 + end.tv_usec - start.tv_usec) / 1000000.0 << "s" << endl;
		cout<<">>";
	}
	
	
	delete repo;
	repo = NULL;
	DATABASE_PATH = NULL;
	QUERY_PATH = NULL;

	return 0;
}
