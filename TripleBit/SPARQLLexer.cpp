#include "SPARQLLexer.h"
#include <math.h>
#include <float.h>

using namespace std;
//---------------------------------------------------------------------------
// RDF-3X
// (c) 2008 Thomas Neumann. Web site: http://www.mpi-inf.mpg.de/~neumann/rdf3x
//
// This work is licensed under the Creative Commons
// Attribution-Noncommercial-Share Alike 3.0 Unported License. To view a copy
// of this license, visit http://creativecommons.org/licenses/by-nc-sa/3.0/
// or send a letter to Creative Commons, 171 Second Street, Suite 300,
// San Francisco, California, 94105, USA.
//---------------------------------------------------------------------------
SPARQLLexer::SPARQLLexer(const std::string& input) :
		input(input), pos(this->input.begin()), tokenStart(pos), tokenEnd(pos), putBack(None), hasTokenEnd(false)
// Constructor
{
}
//---------------------------------------------------------------------------
SPARQLLexer::~SPARQLLexer()
// Destructor
{
}
//---------------------------------------------------------------------------
SPARQLLexer::Token SPARQLLexer::getNext()
// Get the next token
{
	// Do we have a token already?
	if (putBack != None) {
		Token result = putBack;
		putBack = None;
		return result;
	}

	// Reset the token end
	hasTokenEnd = false;
	string value;

	// Read the string
	while (pos != input.end()) {
		tokenStart = pos;
		// Interpret the first character
		switch (*(pos++)) {
		// Whitespace
		case ' ':
		case '\t':
		case '\n':
		case '\r':
		case '\f':
			continue;
			// Single line comment
		case '#':
			while (pos != input.end()) {
				if (((*pos) == '\n') || ((*pos) == '\r'))
					break;
				++pos;
			}
			if (pos != input.end())
				++pos;
			continue;
			// Simple tokens
		case ':':
			return Colon;
		case ';':
			return Semicolon;
		case ',':
			return Comma;
		case '.':
			return Dot;
		case '*':
			return Star;
		case '_':
			return Underscore;
		case '{':
			return LCurly;
		case '}':
			return RCurly;
		case '(':
			return LParen;
		case ')':
			return RParen;
		case '=':
			return Equal;
		case '>':
			if(*pos=='='){
				pos++;
				return Equal;
			}else{
				// pos--;
			}
			return Equal;
			// Not equal
		case '!':
			if ((pos == input.end()) || ((*pos) != '='))
				return Error;
			++pos;
			return NotEqual;
			// Brackets
		case '[':
			// Skip whitespaces
			while (pos != input.end()) {
				switch (*pos) {
				case ' ':
				case '\t':
				case '\n':
				case '\r':
				case '\f':
					++pos;
					continue;
				}
				break;
			}
			// Check for a closing ]
			if ((pos != input.end()) && ((*pos) == ']')) {
				++pos;
				return Anon;
			}
			return LBracket;
		case ']':
			return RBracket;
			// IRI Ref
		case '<':
			if(*pos=='='){
				pos++;
				return Equal;
			}else{
				// pos--;
			}
			tokenStart = pos;
			while (pos != input.end()) {
				if ((*pos) == '>')
					break;
				++pos;
			}
			tokenEnd = pos;
			hasTokenEnd = true;
			if (pos != input.end())
				++pos;
			return IRI;
			// Char
		case '\'':
			tokenStart = pos;
			pos++;
			while (pos != input.end() && (*pos) != '\'') {
				++pos;
			}
			if ((pos - tokenStart) != 1) {
				return Error;
			}
			tokenEnd = pos;
			hasTokenEnd = true;
			if (pos != input.end())
				++pos;
			return Char;
			// String
		case '\"':
			tokenStart = pos;
			while (pos != input.end()) {
				if ((*pos) == '\\') {
					++pos;
					if (pos != input.end())
						++pos;
					continue;
				}
				if ((*pos) == '\"')
					break;
				++pos;
			}
			tokenEnd = pos;
			hasTokenEnd = true;
			if (pos != input.end())
				++pos;
			return String;
			//Numbers
		case '+':
		case '-':
		// case '0':
		// case '1':
		// case '2':
		// case '3':
		// case '4':
		// case '5':
		// case '6':
		// case '7':
		// case '8':
		// case '9':
		// 	tokenStart = pos;
		// 	while (pos != input.end()) {
		// 		if ((*pos) == '\n') {
		// 			while ((*pos) != '.') {
		// 				pos--;
		// 			}
		// 		}
		// 		++pos;
		// 	}
		// 	tokenEnd = pos;
		// 	hasTokenEnd = true;
		// 	value = getTokenValue();
		// 	return getNumberType(value);
			// Variables
		case '?':
		case '$':
			tokenStart = pos;
			while (pos != input.end()) {
				char c = *pos;
				if (((c >= '0') && (c <= '9')) || ((c >= 'A') && (c <= 'Z')) || ((c >= 'a') && (c <= 'z'))) {
					++pos;
				} else
					break;
			}
			tokenEnd = pos;
			hasTokenEnd = true;
			return Variable;
			// Identifier
		default:
			--pos;
			while (pos != input.end()) {
				char c = *pos;
				if (((c >= '0') && (c <= '9')) || ((c >= 'A') && (c <= 'Z')) || ((c >= 'a') && (c <= 'z')) || (c == '_') || (c == '-') || (c == '.')) {
					++pos;
				} else
					break;
			}
			if (pos == tokenStart) {
				return Error;
			} else if (strcasecmp(getTokenValue().c_str(), "true") == 0 || strcasecmp(getTokenValue().c_str(), "false") == 0) {
				return Bool;
			}
			return Identifier;
		}
	}
	return Eof;
}
//---------------------------------------------------------------------------
std::string SPARQLLexer::getTokenValue() const
// Get the value of the current token
{
	if (hasTokenEnd)
		return std::string(tokenStart, tokenEnd);
	else
		return std::string(tokenStart, pos);
}

bool SPARQLLexer::lexDate(string &str, double& date) {
	if (str.empty()) {
		return false;
	}
	SPARQLLexer::strim(str);
	if (str.empty() || str.length() != 19) {
		return false;
	}
	if (str[0] >= '0' && str[0] <= '9' && str[1] >= '0' && str[1] <= '9' && str[2] >= '0' && str[2] <= '9' && str[3] >= '0' && str[3] <= '9' && str[4] == '-' && str[5] >= '0' && str[5] <= '1' && str[6] >= '0' && str[6] <= '9' && str[7] == '-' && str[8] >= '0' && str[8] <= '3' && str[9] >= '0' && str[9] <= '9' && str[10] == ' ' && str[11] >= '0' && str[11] <= '2' && str[12] >= '0' && str[12] <= '9' && str[13] == ':' && str[14] >= '0' && str[14] <= '5' && str[15] >= '0' && str[15] <= '9' && str[16] == ':' && str[17] >= '0' && str[17] <= '5' && str[18] >= '0' && str[18] <= '9') {
		date = (str[0] - '0');
		date = date * 10 + (str[1] - '0');
		date = date * 10 + (str[2] - '0');
		date = date * 10 + (str[3] - '0');
		date = date * 10 + (str[5] - '0');
		date = date * 10 + (str[6] - '0');
		date = date * 10 + (str[8] - '0');
		date = date * 10 + (str[9] - '0');
		date = date * 10 + (str[11] - '0');
		date = date * 10 + (str[12] - '0');
		date = date * 10 + (str[14] - '0');
		date = date * 10 + (str[15] - '0');
		date = date * 10 + (str[17] - '0');
		date = date * 10 + (str[18] - '0');
		return true;
	}
	return false;
}

SPARQLLexer::Token SPARQLLexer::getNumberType(string& s) {
	int len = s.size();
	int left = 0, right = len - 1;
	bool eExisted = false;
	bool dotExisted = false;
	bool digitExisited = false;
	// Delete spaces in the front and end of string
	while (s[left] == ' ')
		++left;
	while (s[right] == ' ')
		--right;
	// If only have one char and not digit, return false
	if (left >= right && (s[left] < '0' || s[left] > '9'))
		return None;
	//Process the first char
	if (s[left] == '.')
		dotExisted = true;
	else if (s[left] >= '0' && s[left] <= '9')
		digitExisited = true;
	else if (s[left] != '+' && s[left] != '-')
		return None;
	// Process the middle chars
	for (int i = left + 1; i <= right - 1; ++i) {
		if (s[i] >= '0' && s[i] <= '9')
			digitExisited = true;
		else if (s[i] == 'e' || s[i] == 'E') { // e/E cannot follow +/-, must follow a digit
			if (!eExisted && s[i - 1] != '+' && s[i - 1] != '-' && digitExisited)
				eExisted = true;
			else
				return None;
		} else if (s[i] == '+' || s[i] == '-') { // +/- can only follow e/E
			if (s[i - 1] != 'e' && s[i - 1] != 'E')
				return None;
		} else if (s[i] == '.') { // dot can only occur once and cannot occur after e/E
			if (!dotExisted && !eExisted)
				dotExisted = true;
			else
				return None;
		} else
			return None;
	}
	// Process the last char, it can only be digit or dot, when is dot, there should be no dot and e/E before and must follow a digit
	if (s[right] >= '0' && s[right] <= '9') {
		if (dotExisted) {
			return Double;
		} else {
			return Integer;
		}
	} else if (s[right] == '.' && !dotExisted && !eExisted && digitExisited)
		return Double;
	else
		return None;
}

double SPARQLLexer::getValueFromToken(const std::string& value, DataType& dataType) {
	double tmp;
	string tmpValue;
	switch (dataType) {
	case BOOL:
	case CHAR:
		return (double) value[0];
	case INT: {
		longlong ll = atoll(value.c_str());
		if (ll >= INT_MIN && ll <= INT_MAX) {
			dataType = INT;
		} else if (ll >= 0 && ll <= UINT_MAX) {
			dataType = UNSIGNED_INT;
		} else if (ll >= LLONG_MIN && ll <= LLONG_MAX) {
			dataType = LONGLONG;
		}
		return (double) ll;
	}
	case DOUBLE:
		tmp = atof(value.c_str());
		if (tmp == HUGE_VAL) {
			MessageEngine::showMessage("data convert to double error", MessageEngine::ERROR);
			dataType = NONE;
			return 0;
		} else if (tmp >= FLT_MIN && tmp <= FLT_MAX) {
			dataType = FLOAT;
		} else if (tmp >= DBL_MIN && tmp <= DBL_MAX) {
			dataType = DOUBLE;
		}
		return tmp;
	case STRING:
		tmpValue = value;
		if (lexDate(tmpValue, tmp)) {
			dataType = DATE;
			return tmp;
		} else {
			dataType = STRING;
			return 0;
		}

	default:
		dataType = NONE;
		return 0;
	}
}
//---------------------------------------------------------------------------
bool SPARQLLexer::isKeyword(const char* keyword) const
// Check if the current token matches a keyword
		{
	std::string::const_iterator iter = tokenStart, limit = hasTokenEnd ? tokenEnd : pos;

	while (iter != limit) {
		char c = *iter;
		if ((c >= 'A') && (c <= 'Z'))
			c += 'a' - 'A';
		if (c != (*keyword))
			return false;
		if (!*keyword)
			return false;
		++iter;
		++keyword;
	}
	return !*keyword;
}
//---------------------------------------------------------------------------
