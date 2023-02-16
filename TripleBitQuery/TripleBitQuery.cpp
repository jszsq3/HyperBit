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
#include <iomanip>


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
	vector<vector<float>> timess(5,vector<float>(7));
	string qufi;
	cout<<">>";
	/*===standard====*/
	// while(cin>>qufi){
	// 	string queryfile = argv[2]+qufi;
	// 	gettimeofday(&start, NULL);
	// 	repo->testquery(queryfile);
	// 	gettimeofday(&end, NULL);
	// 	cout << "query time: " << ((end.tv_sec - start.tv_sec) * 1000000 + end.tv_usec - start.tv_usec) / 1000000.0 << "s" << endl;
	// 	cout<<">>";
	// }
	/*===test====*/
	float sumtime = 0;
	for(int i = 1 ; i < 6; i++){
		for(int j = 1; j < 8 ; j++){
			string queryfile = argv[2]+toStr(j)+toStr(i);
			cout<<queryfile<<endl;
			gettimeofday(&start, NULL);
			repo->testquery(queryfile);
			gettimeofday(&end, NULL);
			float tmp = ((end.tv_sec - start.tv_sec) * 1000000 + end.tv_usec - start.tv_usec) / 1000000.0;
			timess[i-1][j-1] = tmp;
			cout << "query time: " << ((end.tv_sec - start.tv_sec) * 1000000 + end.tv_usec - start.tv_usec) / 1000000.0 << "s" << endl;
			sumtime += tmp;
		}
	}
	
	for(auto &i:timess){
		for(auto &j:i){
			cout<<fixed<<setprecision(4)<<j<<"\t";
		}
		cout<<endl;
	}
	cout<<"sumtime : "<<sumtime<<endl;
		
		
	
	
	
	delete repo;
	repo = NULL;
	DATABASE_PATH = NULL;
	QUERY_PATH = NULL;

	return 0;
}
