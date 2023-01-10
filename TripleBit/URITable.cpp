/*
 * URITable.cpp
 *
 *  Created on: Apr 17, 2010
 *      Author: root
 */

#include "URITable.h"

#include <string.h>

#include "StringIDSegment.h"

URITable::URITable() {
  SINGLE.assign("single");
  prefix_segment = NULL;
  suffix_segment = NULL;
}

URITable::URITable(const string dir) : SINGLE("single") {
  // TODO Auto-generated constructor stub
  prefix_segment = StringIDSegment::create(dir, "uri_prefix");
  suffix_segment = StringIDSegment::create(dir, "uri_suffix");

  prefix_segment->addStringToSegment(SINGLE);
}

URITable::~URITable() {
  // TODO Auto-generated destructor stub
#ifdef DEBUG
  cout << "destroy URITable" << endl;
#endif
  if (prefix_segment != NULL) delete prefix_segment;
  prefix_segment = NULL;

  if (suffix_segment != NULL) delete suffix_segment;
  suffix_segment = NULL;
}

Status URITable::getIdByURI(const char* URI, ID& id) {
  LengthString prefix, suffix, searchLen;
  getPrefix(URI, prefix, suffix);
  string searchStr;
  if (prefix.equals(SINGLE.c_str())) {
    searchStr.insert(searchStr.begin(), 2);
    searchStr.append(suffix.str, suffix.length);
    searchLen.str = searchStr.c_str();
    searchLen.length = searchStr.length();
    if (suffix_segment->findIdByString(id, &searchLen) == false)
      return URI_NOT_FOUND;
  } else {
    char temp[10];
    ID prefixId;
    if (prefix_segment->findIdByString(prefixId, &prefix) == false) {
      return URI_NOT_FOUND;
    } else {
      sprintf(temp, "%d", prefixId);
      searchStr.assign(suffix.str, suffix.length);
      for (size_t i = 0; i < strlen(temp); i++) {
#ifdef USE_C_STRING
        searchStr.insert(searchStr.begin() + i, temp[i] - '0' + 1);
#else
        searchStr.insert(searchStr.begin() + i, temp[i] - '0');
#endif
      }

      searchLen.str = searchStr.c_str();
      searchLen.length = searchStr.length();
      if (suffix_segment->findIdByString(id, &searchLen) == false)
        return URI_NOT_FOUND;
    }
  }

  return URI_FOUND;
}

Status URITable::getPrefix(const char* URI, LengthString& prefix,
                           LengthString& suffix) {
  size_t size = strlen(URI);
  int i;
  for (i = size - 2; i >= 0; i--) {
    if (URI[i] == '/') break;
  }

  if (i == -1) {
    prefix.str = SINGLE.c_str();
    prefix.length = SINGLE.length();
    suffix.str = URI;
    suffix.length = size;
  } else {
    prefix.str = URI;
    prefix.length = i;
    suffix.str = URI + i + 1;
    suffix.length = size - i - 1;
  }

  return OK;
}

Status URITable::insertTable(const char* URI, ID& id) {
  LengthString prefix, suffix, searchLen;
  getPrefix(URI, prefix, suffix);
  char temp[20];
  ID prefixId;

  prefixId = 1;
  if (prefix_segment->findIdByString(prefixId, &prefix) == false)
    prefixId = prefix_segment->addStringToSegment(&prefix);
  sprintf(temp, "%d", prefixId);

  string searchStr;
  searchStr.assign(suffix.str, suffix.length);
  for (size_t i = 0; i < strlen(temp); i++) {
#ifdef USE_C_STRING
    searchStr.insert(
        searchStr.begin() + i,
        temp[i] - '0' +
            1);  // suffix.insert(suffix.begin() + i, temp[i] - '0');
#else
    searchStr.insert(searchStr.begin() + i, temp[i] - '0');
#endif
  }

  searchLen.str = searchStr.c_str();
  searchLen.length = searchStr.length();
  id = suffix_segment->addStringToSegment(&searchLen);
  return OK;
}

Status URITable::insertTable_sync(const char* URI, ID& id) {
  LengthString prefix, suffix, searchLen;
  getPrefix(URI, prefix, suffix);
  char temp[20];
  ID prefixId;

  prefixId = 1;
  lock.lock();
  if (prefix_segment->findIdByString(prefixId, &prefix) == false)
    prefixId = prefix_segment->addStringToSegment(&prefix);
  sprintf(temp, "%d", prefixId);

  string searchStr;
  searchStr.assign(suffix.str, suffix.length);
  for (size_t i = 0; i < strlen(temp); i++) {
#ifdef USE_C_STRING
    searchStr.insert(
        searchStr.begin() + i,
        temp[i] - '0' +
            1);  // suffix.insert(suffix.begin() + i, temp[i] - '0');
#else
    searchStr.insert(searchStr.begin() + i, temp[i] - '0');
#endif
  }

  searchLen.str = searchStr.c_str();
  searchLen.length = searchStr.length();
  id = suffix_segment->addStringToSegment(&searchLen);
  lock.unlock();
  return OK;
}

Status URITable::getURIById(string& URI, ID id) {
  URI.clear();
  LengthString prefix_, suffix_;
  if (suffix_segment->findStringById(&suffix_, id) == false) {
    return URI_NOT_FOUND;
  }

  char temp[10];
  memset(temp, 0, 10);
  const char* ptr = suffix_.str;

  int i;
#ifdef USE_C_STRING
  for (i = 0; i < 10; i++) {
    if (ptr[i] > 10) break;
    temp[i] = (ptr[i] - 1) + '0';
  }
#else
  for (i = 0; i < 10; i++) {
    if (ptr[i] > 9) break;
    temp[i] = ptr[i] + '0';
  }
#endif

  ID prefixId = atoi(temp);
  if (prefixId == 1)
    URI.assign(suffix_.str + 1, suffix_.length - 1);
  else {
    if (prefix_segment->findStringById(&prefix_, prefixId) == false) {
      return URI_NOT_FOUND;
    }

    URI.assign(prefix_.str, prefix_.length);
    URI.append("/");
    URI.append(suffix_.str + i, suffix_.length - i);
  }
  return OK;
}

URITable* URITable::load(const string dir) {
  URITable* uriTable = new URITable();
  uriTable->prefix_segment = StringIDSegment::load(dir, "uri_prefix");
  uriTable->suffix_segment = StringIDSegment::load(dir, "uri_suffix");
  return uriTable;
}

void URITable::dump() {
  prefix_segment->dump();
  suffix_segment->dump();
}
