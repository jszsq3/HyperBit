/*
 * OSFile.cpp
 *
 *  Created on: Oct 8, 2010
 *      Author: root
 */

#include "OSFile.h"

#include <stdio.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <sys/uio.h>
#include<dirent.h>

OSFile::OSFile() {
}

OSFile::~OSFile() {
}

bool OSFile::fileExists(const string filename) {
	struct stat sbuff;
	if (stat(filename.c_str(), &sbuff) == 0) {
		if (S_ISREG(sbuff.st_mode))
			return true;
	}
	return false;
}

bool OSFile::directoryExists(const string path) {
	struct stat sbuff;
	if (stat(path.c_str(), &sbuff) == 0) {
		if (S_ISDIR(sbuff.st_mode))
			return true;
	}
	return false;
}

bool OSFile::mkdir(const string path) {
	return (::mkdir(path.c_str(), S_IRWXU | S_IRWXG | S_IRWXO) == 0);
}

size_t OSFile::fileSize(std::string filename) {
	struct stat buf;
	stat(filename.c_str(), &buf);
	return buf.st_size;
}

void getSplitFile(const char* src, const char* prefix, vector<string> &spiltfile) {
	DIR *dp;
	struct dirent *dirp;
	spiltfile.clear();

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
			spiltfile.push_back(dirp->d_name);
		}

	}
}
