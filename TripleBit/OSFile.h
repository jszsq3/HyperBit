/*
 * OSFile.h
 *
 *  Created on: Oct 8, 2010
 *      Author: root
 */

#ifndef OSFILE_H_
#define OSFILE_H_

#include "TripleBit.h"
#include <fcntl.h>
#include<dirent.h>

class OSFile {
public:
	OSFile();
	virtual ~OSFile();
	static bool fileExists(const string filename);
	static bool directoryExists(const string dir);
	static bool mkdir(const string dir);
	static size_t fileSize(const string filename);

	static void getSplitFile(const char* src, const char* prefix, vector<string> &spiltfile) {
		DIR *dp;
		struct dirent *dirp;

		if ((dp = opendir(src)) == NULL) {
			perror("opendir error");
			exit(1);
		}

		int len = sizeof(prefix) / sizeof(prefix[0]) - 1;
		bool flag;

		while ((dirp = readdir(dp)) != NULL) {
			if ((strcmp(dirp->d_name, ".") == 0) || (strcmp(dirp->d_name, "..") == 0))
				continue;
			flag = true;
			for (int index = 0; index < len && dirp->d_name[index] != '\0'; index++) {
				if (dirp->d_name[index] != prefix[index]) {
					flag = false;
				}
			}
			if (flag) {
				spiltfile.push_back(string(src).append("/").append(dirp->d_name));
			}

		}
	}

	static string GetStdoutFromCommand(string cmd) {

		string data;
		FILE * stream;
		const int max_buffer = 256;
		char buffer[max_buffer];
		cmd.append(" 2>&1");

		stream = popen(cmd.c_str(), "r");
		if (stream) {
			while (!feof(stream))
				if (fgets(buffer, max_buffer, stream) != NULL)
					data.append(buffer);
			pclose(stream);
		}
		return data;
	}
	static void split(const char* src, int threadNUM, const char* prefix) {
		string cmd = "wc -l ";
		cmd.append(src);
		size_t lines = stoull(GetStdoutFromCommand(cmd));
		int perline = (lines % threadNUM == 0) ? lines / threadNUM : (lines / threadNUM + 1);
		if (directoryExists(prefix)) {
			remove(prefix);
		}
		mkdir(prefix);
		cmd = "split -l ";
		cmd.append(to_string(perline)).append(" -a 3 -d ").append(src).append(" ").append(prefix).append("/").append(
		    prefix);
		// cout << cmd << endl;
		std::system(cmd.c_str());
	}

	static void remove(const char* filename) {
		string order = "rm -rf ";
		order.append(filename);
		std::system(order.c_str());
	}
};

#endif /* OSFILE_H_ */
