/*
 * Properties.h
 *
 *  Created on: 2017年12月29日
 *      Author: XuQingQing
 */

#ifndef PROPERTIES_H_
#define PROPERTIES_H_

#include <string>
#include <map>
#include <fstream>

class Properties {
public:
	Properties() {
	}
	~Properties() {
	}
	static void parse(const std::string &db, std::map<std::string, std::string> &properties) {
		std::string propertiespath = db + "properties";
//		std::cout << "load properties: " << propertiespath << std::endl;
		std::ifstream in(propertiespath.c_str());
		if(!in.is_open()){
			return;
		}
		std::string line;
		while (!in.eof()) {
			getline(in, line);
			int index = 0;
			if (!line.empty()) {
				while ((index = line.find(' ', index)) != (int)std::string::npos) {
					line.erase(index, 1);
				}
				index = line.find_first_of('=');
				std::string key = line.substr(0, index);
				std::string value = line.substr(index + 1);
				properties[key] = value;
			}
		}
		in.close();
	}

	static void persist(const std::string &db, std::map<std::string, std::string> &properties) {
		std::string propertiespath = db + "properties";
//		std::cout << "persist properties: " << propertiespath << std::endl;
		std::ofstream out(propertiespath.c_str());
		if(!out.is_open()){
			std::cerr << "write properties fail !" << std::endl;
			return;
		}

		for(auto &m: properties){
			out << m.first << "=" << m.second <<std::endl;
		}
		out.close();
		properties.clear();
	}
};


class ShiXu {
public:
	ShiXu() {
	}
	~ShiXu() {
	}
	static void parse(const std::string& db, std::map < std::string, std::map<std::string, vector<std::string>>>& shixu) {
		std::cout << "jiazai" << std::endl;
		std::string propertiespath = db + "sx";
		//		std::cout << "load properties: " << propertiespath << std::endl;
		std::ifstream in(propertiespath.c_str());
		if (!in.is_open()) {
			return;
		}
		std::string line;
		std::string s, p, to;
		while (!in.eof()) {
			getline(in, line);
			int index = 0;
			if (!line.empty()) {
				index = line.find_first_of('=');
				s = line.substr(0, index);
				line = line.substr(index + 1);

				index = line.find_first_of('=');
				p = line.substr(0, index);

				to = line.substr(index + 1);

				shixu[s][p].push_back(to);
				std::cout << s << "    " << p << "    " << to << std::endl;
			}
		}
		in.close();
	}

	static void persist(const std::string& db, std::map<std::string, std::map<std::string,vector<std::string>>>& shixu) {
		std::cout << "chijiuhua" << std::endl;
		std::string propertiespath = db + "sx";
		//		std::cout << "persist properties: " << propertiespath << std::endl;
		std::ofstream out(propertiespath.c_str());
		if (!out.is_open()) {
			std::cerr << "write properties fail !" << std::endl;
			return;
		}

		for(auto& m : shixu) {
			string s = m.first;
			for (auto& n : m.second) {
				string p = n.first;
				for (string to : n.second) {
					out << s << "=" << p << "=" << to << std::endl;
					std::cout << s << "    " << p << "    " << to << std::endl;
				}
			}
		}
		out.close();
		shixu.clear();
	}
};




#endif /* PROPERTIES_H_ */
