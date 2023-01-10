/*
 * PredicateTable.cpp
 *
 *  Created on: Mar 11, 2010
 *      Author: root
 */

#include "PredicateTable.h"
#include "StringIDSegment.h"

PredicateTable::PredicateTable(const string dir) :
        SINGLE("single") {
    // TODO Auto-generated constructor stub
    prefix_segment = StringIDSegment::create(dir, "predicate_prefix");
    suffix_segment = StringIDSegment::create(dir, "predicate_suffix");

    prefix_segment->addStringToSegment(SINGLE);
}

PredicateTable::~PredicateTable() {
    // TODO Auto-generated destructor stub
    if (prefix_segment != NULL)
        delete prefix_segment;
    prefix_segment = NULL;

    if (suffix_segment != NULL)
        delete suffix_segment;
    suffix_segment = NULL;
}

Status PredicateTable::getPrefix(const char *URI, LengthString &prefix, LengthString &suffix) {
    size_t size = strlen(URI);
    int i;
    for (i = size - 2; i >= 0; i--) {
        if (URI[i] == '/')
            break;
    }

    if (i == -1) {
        prefix.str = SINGLE.c_str();
        prefix.length = SINGLE.length();
        suffix.str = URI;
        suffix.length = size;
    } else {
        //prefix.assign(URI.begin(), URI.begin() + size);
        //suffix.assign(URI.begin() + size + 1, URI.end());
        prefix.str = URI;
        prefix.length = i;
        suffix.str = URI + i + 1;
        suffix.length = size - i - 1;
    }

    return OK;
}

Status PredicateTable::insertTable(const char *str, ID &id) {
    LengthString prefix, suffix, searchLen;
    getPrefix(str, prefix, suffix);
    char temp[20];
    ID prefixId;

    if (prefix_segment->findIdByString(prefixId, &prefix) == false)
        prefixId = prefix_segment->addStringToSegment(&prefix);
    sprintf(temp, "%d", prefixId);

    string searchStr;
    searchStr.assign(suffix.str, suffix.length);
    for (size_t i = 0; i < strlen(temp); i++) {
#ifdef USE_C_STRING
        //前缀id+后缀=searchStr
        searchStr.insert(searchStr.begin() + i,
                         temp[i] - '0' + 1);        //suffix.insert(suffix.begin() + i, temp[i] - '0');
#else
        searchStr.insert(searchStr.begin() + i, temp[i] - '0');
#endif
    }

    searchLen.str = searchStr.c_str();
    searchLen.length = searchStr.length();
    id = suffix_segment->addStringToSegment(&searchLen);
    return OK;
}

Status PredicateTable::insertTable_sync(const char *str, ID &id) {
    LengthString prefix, suffix, searchLen;
    getPrefix(str, prefix, suffix);
    char temp[20];
    ID prefixId;

    lock.lock();
    if (prefix_segment->findIdByString(prefixId, &prefix) == false)
        prefixId = prefix_segment->addStringToSegment(&prefix);
    sprintf(temp, "%d", prefixId);

    string searchStr;
    searchStr.assign(suffix.str, suffix.length);
    for (size_t i = 0; i < strlen(temp); i++) {
#ifdef USE_C_STRING
        searchStr.insert(searchStr.begin() + i,
                         temp[i] - '0' + 1);        //suffix.insert(suffix.begin() + i, temp[i] - '0');
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

Status PredicateTable::getPredicateByID(string &URI, ID id) {
    URI.clear();
    LengthString prefix_, suffix_;
    if (suffix_segment->findStringById(&suffix_, id) == false)
        return URI_NOT_FOUND;
    char temp[10];
    memset(temp, 0, 10);
    const char *ptr = suffix_.str;

    int i;
#ifdef USE_C_STRING
    for (i = 0; i < 10; i++) {
        if (ptr[i] > 10)
            break;
        temp[i] = (ptr[i] - 1) + '0';
    }
#else
    for(i = 0; i < 10; i++) {
        if(ptr[i] > 9)
        break;
        temp[i] = ptr[i] + '0';
    }
#endif

    ID prefixId = atoi(temp);
    if (prefixId == 1)
        URI.assign(suffix_.str + 1, suffix_.length - 1);
    else {
        if (prefix_segment->findStringById(&prefix_, prefixId) == false)
            return PREDICATE_NOT_FOUND;
        URI.assign(prefix_.str, prefix_.length);
        URI.append("/");
        URI.append(suffix_.str + i, suffix_.length - i);
    }

    return OK;
}

Status PredicateTable::getIDByPredicate(const char *str, ID &id) {
    LengthString prefix, suffix, searchLen;
    getPrefix(str, prefix, suffix);
    string searchStr;
    if (prefix.equals(SINGLE.c_str())) {
        searchStr.clear();
        searchStr.insert(searchStr.begin(), 2);
        searchStr.append(suffix.str, suffix.length);
        searchLen.str = searchStr.c_str();
        searchLen.length = searchStr.length();
        if (suffix_segment->findIdByString(id, &searchLen) == false)
            return PREDICATE_NOT_BE_FINDED;
    } else {
        char temp[10];
        ID prefixId;
        if (prefix_segment->findIdByString(prefixId, &prefix) == false) {
            return PREDICATE_NOT_BE_FINDED;
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
                return PREDICATE_NOT_BE_FINDED;
        }
    }

    return OK;
}

size_t PredicateTable::getPredicateNo() {
    return suffix_segment->idStroffPool->size();
}

PredicateTable *PredicateTable::load(const string dir) {
    PredicateTable *table = new PredicateTable();
    table->prefix_segment = StringIDSegment::load(dir, "predicate_prefix");
    table->suffix_segment = StringIDSegment::load(dir, "predicate_suffix");

    return table;
}

void PredicateTable::dump() {
    prefix_segment->dump();
    suffix_segment->dump();
}
