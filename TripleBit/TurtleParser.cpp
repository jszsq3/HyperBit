#include "TurtleParser.hpp"
#include <iostream>
#include <sstream>
#include <fstream>
#include <string.h>
#include "TripleBit.h"
//#define DEBUG
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
using namespace std;
//---------------------------------------------------------------------------
TurtleParser::Lexer::Lexer(istream& in) :
		in(in), putBack(Eof), line(1), readBufferStart(0), readBufferEnd(0)
// Constructor
{
}
//---------------------------------------------------------------------------
TurtleParser::Lexer::~Lexer()
// Destructor
{
}
//---------------------------------------------------------------------------
bool TurtleParser::Lexer::doRead(char& c)
// Read new characters
		{
	while (in) {
		readBufferStart = readBuffer;
		in.read(readBuffer, readBufferSize);
		if (!in.gcount())
			return false;
		readBufferEnd = readBufferStart + in.gcount();

		if (readBufferStart < readBufferEnd) {
			c = *(readBufferStart++);
			return true;
		}
	}
	return false;
}
//---------------------------------------------------------------------------
static bool issep(char c) {
	return (c == ' ') || (c == '\t') || (c == '\n') || (c == '\r') || (c == '[') || (c == ']') || (c == '(') || (c == ')') || (c == ',') || (c == ';') || (c == ':') || (c == '.');
}

TurtleParser::Lexer::Token TurtleParser::Lexer::lexChar(std::string& token, char c) {
	token.resize(0);
	token += c;
	while (true) {

		if (!read(c))
			break;
		if (c != '\n') {
			token += c;
		} else {
			token = token.substr(0, token.length() - 1);
			unread();
			unread();
			break;
		}
	}
	strim(token);
	if (!token.empty() && token.length() == 3 && token[0] == '\'' && token[2] == '\'') {
		return Char;
	}

	cerr << "lexer error in line " << line << ": invalid char " << token << c << endl;
	throw Exception();
}

//---------------------------------------------------------------------------
TurtleParser::Lexer::Token TurtleParser::Lexer::getNumberType(string& s) {
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

TurtleParser::Lexer::Token TurtleParser::Lexer::lexNumber(std::string& token, char c)
// Lex a number
		{
	token.resize(0);
	token += c;
	while (true) {

		if (!read(c))
			break;
		if (c != '\n') {
			token += c;
		} else {
			token = token.substr(0, token.length() - 1);
			unread();
			unread();
			break;
		}
	}
	Lexer::Token t = getNumberType(token);
	if (t != None) {
		return t;
	} else {
		cerr << "lexer error in line " << line << ": invalid number " << token << c << endl;
	}

	throw Exception();
}

bool TurtleParser::Lexer::lexDate(std::string &str, double& date) {
	if (str.empty()) {
		return false;
	}
	TurtleParser::strim(str);
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
//---------------------------------------------------------------------------
unsigned TurtleParser::Lexer::lexHexCode(unsigned len)
// Parse a hex code
		{
	unsigned result = 0;
	for (unsigned index = 0;; index++) {
		// Done?
		if (index == len)
			return result;

		// Read the next char
		char c;
		if (!read(c))
			break;

		// Interpret it
		if ((c >= '0') && (c <= '9'))
			result = (result << 4) | (c - '0');
		else if ((c >= 'A') && (c <= 'F'))
			result = (result << 4) | (c - 'A' + 10);
		else if ((c >= 'a') && (c <= 'f'))
			result = (result << 4) | (c - 'a' + 10);
		else
			break;
	}
	cerr << "lexer error in line " << line << ": invalid unicode escape" << endl;
	throw Exception();
}
//---------------------------------------------------------------------------
static string encodeUtf8(unsigned code)
// Encode a unicode character as utf8
		{
	string result;
	if (code && (code < 0x80)) {
		result += static_cast<char>(code);
	} else if (code < 0x800) {
		result += static_cast<char>(0xc0 | (0x1f & (code >> 6)));
		result += static_cast<char>(0x80 | (0x3f & code));
	} else {
		result += static_cast<char>(0xe0 | (0x0f & (code >> 12)));
		result += static_cast<char>(0x80 | (0x3f & (code >> 6)));
		result += static_cast<char>(0x80 | (0x3f & code));
	}
	return result;
}
//---------------------------------------------------------------------------
void TurtleParser::Lexer::lexEscape(std::string& token)
// Lex an escape sequence, \ already consumed
		{
	while (true) {
		char c;
		if (!read(c))
			break;
		// Standard escapes?
		if (c == 't') {
			token += '\t';
			return;
		}
		if (c == 'n') {
			token += '\n';
			return;
		}
		if (c == 'r') {
			token += '\r';
			return;
		}
		if (c == '\"') {
			token += '\"';
			return;
		}
		if (c == '>') {
			token += '>';
			return;
		}
		if (c == '\\') {
			token += '\\';
			return;
		}

		// Unicode sequences?
		if (c == 'u') {
			unsigned code = lexHexCode(4);
			token += encodeUtf8(code);
			return;
		}
		if (c == 'U') {
			unsigned code = lexHexCode(8);
			token += encodeUtf8(code);
			return;
		}

		// Invalid escape
		break;
	}
	cerr << "lexer error in line " << line << ": invalid escape sequence" << endl;
	throw Exception();
}
//---------------------------------------------------------------------------
TurtleParser::Lexer::Token TurtleParser::Lexer::lexLongString(std::string& token)
// Lex a long string, first """ already consumed
		{
	char c;
	while (read(c)) {
		if (c == '\"') {
			if (!read(c))
				break;
			if (c != '\"') {
				token += '\"';
				continue;
			}
			if (!read(c))
				break;
			if (c != '\"') {
				token += "\"\"";
				continue;
			}
			return String;
		}
		if (c == '\\') {
			lexEscape(token);
		} else {
			token += c;
			if (c == '\n')
				line++;
		}
	}
	cerr << "lexer error in line " << line << ": invalid string" << endl;
	throw Exception();
}
//---------------------------------------------------------------------------
TurtleParser::Lexer::Token TurtleParser::Lexer::lexString(std::string& token, char c)
// Lex a string
		{
	token.resize(0);

	// Check the next character
	if (!read(c)) {
		cerr << "lexer error in line " << line << ": invalid string" << endl;
		throw Exception();
	}

	// Another quote?
	if (c == '\"') {
		if (!read(c))
			return String;
		if (c == '\"')
			return lexLongString(token);
		unread();
		return String;
	}

	// Process normally
	while (true) {
		if (c == '\"')
			return String;
		if (c == '\\') {
			lexEscape(token);
		} else {
			token += c;
			if (c == '\n')
				line++;
		}
		if (!read(c)) {
			cerr << "lexer error in line " << line << ": invalid string" << endl;
			throw Exception();
		}
	}
}
//---------------------------------------------------------------------------
TurtleParser::Lexer::Token TurtleParser::Lexer::lexURI(std::string& token, char c)
// Lex a URI
		{
	token.resize(0);

	// Check the next character
	if (!read(c)) {
		cerr << "lexer error in line " << line << ": invalid URI" << endl;
		throw Exception();
	}

	// Process normally
	while (true) {
		if (c == '>')
			return URI;
		if (c == '\\') {
			lexEscape(token);
		} else {
			token += c;
			if (c == '\n')
				line++;
		}
		if (!read(c)) {
			cerr << "lexer error in line " << line << ": invalid URI" << endl;
			throw Exception();
		}
	}
}
//---------------------------------------------------------------------------
TurtleParser::Lexer::Token TurtleParser::Lexer::next(std::string& token)
// Get the next token
		{
	// Do we already have one?
	if (putBack != Eof) {
		Token result = putBack;
		token = putBackValue;
		putBack = Eof;
		return result;
	}

	// Read more
	char c;
	while (read(c)) {
		switch (c) {
		case ' ':
		case '\t':
		case '\r':
			continue;
		case '\n':
			line++;
			continue;
		case '\'':
			return lexChar(token, '\'');
		case '#':
			while (read(c))
				if ((c == '\n') || (c == '\r'))
					break;
			if (c == '\n')
				++line;
			continue;
		case '.':
			if (!read(c))
				return Dot;
			unread();
			if ((c >= '0') && (c <= '9'))
				return lexNumber(token, '.');
			return Dot;
		case ':':
			return Colon;
		case ';':
			return Semicolon;
		case ',':
			return Comma;
		case '[':
			return LBracket;
		case ']':
			return RBracket;
		case '(':
			return LParen;
		case ')':
			return RParen;
		case '@':
			return At;
		case '+':
		case '-':
		case '0':
		case '1':
		case '2':
		case '3':
		case '4':
		case '5':
		case '6':
		case '7':
		case '8':
		case '9':
			return lexNumber(token, c);
		case '^':
			if ((!read(c)) || (c != '^')) {
				cerr << "lexer error in line " << line << ": '^' expected" << endl;
				throw Exception();
			}
			return Type;
		case '\"':
			return lexString(token, c);
		case '<':
			return lexURI(token, c);
		default:
			if (((c >= 'A') && (c <= 'Z')) || ((c >= 'a') && (c <= 'z')) || (c == '_')) { // XXX unicode!
				token = c;
				while (read(c)) {
					if (issep(c)) {
						unread();
						break;
					}
					token += c;
				}
				if (token == "a")
					return A;
				if (strcasecmp(token.c_str(), "true") == 0)
					return True;
				if (strcasecmp(token.c_str(), "false") == 0)
					return False;
				return Name;
			} else {
				cerr << "lexer error in line " << line << ": unexpected character " << c << endl;
				throw Exception();
			}
		}
	}

	return Eof;
}
//---------------------------------------------------------------------------
TurtleParser::TurtleParser(istream& in) :
		lexer(in), triplesReader(0), nextBlank(0)
// Constructor
{
}
//---------------------------------------------------------------------------
TurtleParser::~TurtleParser()
// Destructor
{
}
//---------------------------------------------------------------------------
void TurtleParser::parseError(const string& message)
// Report an error
		{
	cerr << "parse error in line " << lexer.getLine() << ": " << message << endl;
	throw Exception();
}
//---------------------------------------------------------------------------
void TurtleParser::newBlankNode(std::string& node)
// Construct a new blank node
		{
	stringstream buffer;
	buffer << "_:_" << (nextBlank++);
	node = buffer.str();
}
//---------------------------------------------------------------------------
void TurtleParser::parseDirective()
// Parse a directive
{
	std::string value;
	if (lexer.next(value) != Lexer::Name)
		parseError("directive name expected after '@'");

	if (value == "base") {
		if (lexer.next(base) != Lexer::URI)
			parseError("URI expected after @base");
		static bool warned = false;
		if (!warned) {
			cerr << "warning: @base directives are currently ignored" << endl;
			warned = true; // XXX
		}
	} else if (value == "prefix") {
		std::string prefixName;
		Lexer::Token token = lexer.next(prefixName);
		// A prefix name?
		if (token == Lexer::Name) {
			token = lexer.next();
		} else
			prefixName.resize(0);
		// Colon
		if (token != Lexer::Colon)
			parseError("':' expected after @prefix");
		// URI
		std::string uri;
		if (lexer.next(uri) != Lexer::URI)
			parseError("URI expected after @prefix");
		prefixes[prefixName] = uri;
	} else {
		parseError("unknown directive @" + value);
	}

	// Final dot
	if (lexer.next() != Lexer::Dot)
		parseError("'.' expected after directive");
}
//---------------------------------------------------------------------------
inline bool TurtleParser::isName(Lexer::Token token)
// Is a (generalized) name token?
		{
	return (token == Lexer::Name) || (token == Lexer::A) || (token == Lexer::True) || (token == Lexer::False);
}
//---------------------------------------------------------------------------
void TurtleParser::parseQualifiedName(const string& prefix, string& name)
// Parse a qualified name
		{
	if (lexer.next() != Lexer::Colon)
		parseError("':' expected in qualified name");
	if (!prefixes.count(prefix))
		parseError("unknown prefix '" + prefix + "'");
	Lexer::Token token = lexer.next(name);
	if (isName(token)) {
		name = prefixes[prefix] + name;
	} else {
		lexer.unget(token, name);
		name = prefixes[prefix];
	}
}
//---------------------------------------------------------------------------
/*void TurtleParser::parseBlank(std::string& entry)
 // Parse a blank entry
 {
 Lexer::Token token = lexer.next(entry);
 switch (token) {
 case Lexer::Name:
 if ((entry != "_") || (lexer.next() != Lexer::Colon)
 || (!isName(lexer.next(entry))))
 parseError("blank nodes must start with '_:'");
 entry = "_:" + entry;
 return;
 case Lexer::LBracket: {
 newBlankNode(entry);
 token = lexer.next();
 if (token != Lexer::RBracket) {
 lexer.ungetIgnored(token);
 std::string predicate, object;
 parsePredicateObjectList(entry, predicate, object);
 triples.push_back(Triple(entry, predicate, object));
 if (lexer.next() != Lexer::RBracket)
 parseError("']' expected");
 }
 return;
 }
 case Lexer::LParen: {
 // Collection
 vector<string> entries;

 while ((token = lexer.next()) != Lexer::RParen) {
 lexer.ungetIgnored(token);
 entries.push_back(string());
 parseObject(entries.back());
 }

 // Empty collection?
 if (entries.empty()) {
 entry = "http://www.w3.org/1999/02/22-rdf-syntax-ns#nil";
 return;
 }

 // Build blank nodes
 vector<string> nodes;
 nodes.resize(entries.size());
 for (unsigned index = 0; index < entries.size(); index++)
 newBlankNode(nodes[index]);
 nodes.push_back("http://www.w3.org/1999/02/22-rdf-syntax-ns#nil");

 // Derive triples
 for (unsigned index = 0; index < entries.size(); index++) {
 triples.push_back(
 Triple(nodes[index],
 "http://www.w3.org/1999/02/22-rdf-syntax-ns#first",
 entries[index]));
 triples.push_back(
 Triple(nodes[index],
 "http://www.w3.org/1999/02/22-rdf-syntax-ns#rest",
 nodes[index + 1]));
 }
 entry = nodes.front();
 }

 default:
 parseError("invalid blank entry");
 }
 }*/
//---------------------------------------------------------------------------
void TurtleParser::parseSubject(Lexer::Token token, std::string& subject)
// Parse a subject
		{
	switch (token) {
	case Lexer::URI:
		// URI
		return;
	case Lexer::A:
		subject = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
		return;
	case Lexer::Colon:
		// Qualified name with empty prefix?
		lexer.unget(token, subject);
		parseQualifiedName("", subject);
		return;
		/*case Lexer::Name:
		 // Qualified name
		 // Blank node?
		 if (subject == "_") {
		 lexer.unget(token, subject);
		 parseBlank(subject);
		 return;
		 }
		 // No
		 parseQualifiedName(subject, subject);
		 return;*/
	case Lexer::LBracket:
		/*case Lexer::LParen:
		 // Opening bracket/parenthesis
		 lexer.unget(token, subject);
		 parseBlank(subject);*/
	default:
		parseError("invalid subject");
	}
}

//---------------------------------------------------------------------------
void TurtleParser::parseObject(std::string& object, char& objType)
// Parse an object
		{
	Lexer::Token token = lexer.next(object);
#ifdef DEBUG
	ofstream out("C:\\Users\\XuQingQing\\Desktop\\temp", ios::app);
	std::string type;
	getType(token, type);
	out << "O: " << type << "\t" << object << endl;
	out.close();
#endif
	switch (token) {
	case Lexer::URI:
		// URI
		objType = STRING;
		return;
	case Lexer::Colon:
		// Qualified name with empty prefix?
		lexer.unget(token, object);
		parseQualifiedName("", object);
		return;
		/*case Lexer::Name:
		 // Qualified name
		 // Blank node?
		 if (object == "_") {
		 lexer.unget(token, object);
		 parseBlank(object);
		 return;
		 }
		 // No
		 parseQualifiedName(object, object);
		 return;*/
	case Lexer::LBracket:
		/*case Lexer::LParen:
		 // Opening bracket/parenthesis
		 lexer.unget(token, object);
		 parseBlank(object);
		 return;*/
	case Lexer::Char:
		objType = CHAR;
		return;
	case Lexer::Integer:
		objType = INT;
		return;
	case Lexer::Decimal:
	case Lexer::Double:
		objType = DOUBLE;
		return;
	case Lexer::A:
		return;
	case Lexer::True:
	case Lexer::False:
		objType = BOOL;
		// Literal
		return;
	case Lexer::String:
		// String literal
	{
		objType = STRING;
		token = lexer.next();
		if (token == Lexer::At) {
			if (lexer.next() != Lexer::Name)
				parseError("language tag expected");
			static bool warned = false;
			if (!warned) {
				cerr << "warning: language tags are currently ignored" << endl;
				warned = true; // XXX
			}
		} else if (token == Lexer::Type) {
			string type;
			token = lexer.next(type);
			if (token == Lexer::URI) {
				// Already parsed
			} else if (token == Lexer::Colon) {
				parseQualifiedName("", type);
			} else if (token == Lexer::Name) {
				parseQualifiedName(type, type);
			}
			static bool warned = false;
			if (!warned) {
				cerr << "warning: literal types are currently ignored" << endl;
				warned = true; // XXX
			}
		} else {
			lexer.ungetIgnored(token);
		}

		return;
	}
	default:
		parseError("invalid object");
	}
}
//---------------------------------------------------------------------------
void TurtleParser::parsePredicateObjectList(const string& subject, string& predicate, string& object, char &objType)
// Parse a predicate object list
		{
	// Parse the first predicate
	Lexer::Token token = lexer.next(predicate);
#ifdef DEBUG
	ofstream out("C:\\Users\\XuQingQing\\Desktop\\temp", ios::app);
	std::string type;
	getType(token, type);
	out << "P: " << type << "\t" << predicate << endl;
	out.close();
#endif
	switch (token) {
	case Lexer::URI:
		break;
	case Lexer::A:
		predicate = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
		break;
	case Lexer::Colon:
		lexer.unget(token, predicate);
		parseQualifiedName("", predicate);
		break;
	case Lexer::Name:
		if (predicate == "_")
			parseError("blank nodes not allowed as predicate");
		parseQualifiedName(predicate, predicate);
		break;
	default:
		parseError("invalid predicate");
	}

	// Parse the object
	parseObject(object, objType);

	/*// Additional objects?
	 token = lexer.next();
	 while (token == Lexer::Comma) {
	 string additionalObject;
	 parseObject(additionalObject);
	 triples.push_back(Triple(subject, predicate, additionalObject));
	 token = lexer.next();
	 }

	 // Additional predicates?
	 while (token == Lexer::Semicolon) {
	 // Parse the predicate
	 string additionalPredicate;
	 switch (token = lexer.next(additionalPredicate)) {
	 case Lexer::URI:
	 break;
	 case Lexer::A:
	 additionalPredicate =
	 "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
	 break;
	 case Lexer::Colon:
	 lexer.unget(token, additionalPredicate);
	 parseQualifiedName("", additionalPredicate);
	 break;
	 case Lexer::Name:
	 if (additionalPredicate == "_")
	 parseError("blank nodes not allowed as predicate");
	 parseQualifiedName(additionalPredicate, additionalPredicate);
	 break;
	 default:
	 lexer.unget(token, additionalPredicate);
	 return;
	 }

	 // Parse the object
	 string additionalObject;
	 parseObject(additionalObject);
	 triples.push_back(
	 Triple(subject, additionalPredicate, additionalObject));

	 // Additional objects?
	 token = lexer.next();
	 while (token == Lexer::Comma) {
	 parseObject(additionalObject);
	 triples.push_back(
	 Triple(subject, additionalPredicate, additionalObject));
	 token = lexer.next();
	 }
	 }
	 lexer.ungetIgnored(token);*/
}

/*void TurtleParser::getType(Lexer::Token token, std::string& type) {
 switch (token) {
 case Lexer::Token::Eof:
 type = "Eof";
 break;
 case Lexer::Token::Dot:
 type = "Dot";
 break;
 case Lexer::Token::Colon:
 type = "Colon";
 break;
 case Lexer::Token::Comma:
 type = "Comma";
 break;
 case Lexer::Token::Semicolon:
 type = "Semicolon";
 break;
 case Lexer::Token::LBracket:
 type = "LBracket";
 break;
 case Lexer::Token::RBracket:
 type = "RBracket";
 break;
 case Lexer::Token::LParen:
 type = "LParen";
 break;
 case Lexer::Token::RParen:
 type = "RParen";
 break;
 case Lexer::Token::At:
 type = "At";
 break;
 case Lexer::Token::Type:
 type = "Type";
 break;
 case Lexer::Token::Integer:
 type = "Integer";
 break;
 case Lexer::Token::Decimal:
 type = "Decimal";
 break;
 case Lexer::Token::Double:
 type = "Double";
 break;
 case Lexer::Token::Char:
 type = "Char";
 break;
 case Lexer::Token::Name:
 type = "Name";
 break;
 case Lexer::Token::A:
 type = "A";
 break;
 case Lexer::Token::True:
 type = "True";
 break;
 case Lexer::Token::False:
 type = "False";
 break;
 case Lexer::Token::String:
 type = "String";
 break;
 case Lexer::Token::URI:
 type = "URI";
 break;
 default:
 type = "";
 break;

 }
 }*/
//---------------------------------------------------------------------------
void TurtleParser::parseTriple(Lexer::Token token, std::string& subject, std::string& predicate, std::string& object, char &objType)
// Parse a triple
		{
	parseSubject(token, subject);
#ifdef DEBUG
	ofstream out("C:\\Users\\XuQingQing\\Desktop\\temp", ios::app);
	std::string type;
	getType(token, type);
	out << "S: " << type << "\t" << subject << endl;
	out.close();
#endif
	parsePredicateObjectList(subject, predicate, object, objType);
	if (lexer.next() != Lexer::Dot)
		parseError("'.' expected after triple");
}
//---------------------------------------------------------------------------
bool TurtleParser::parse(std::string& subject, std::string& predicate, std::string& object, char& objType)
// Read the next triple
		{
	// Some triples left?
	if (triplesReader < triples.size()) {
		subject = triples[triplesReader].subject;
		predicate = triples[triplesReader].predicate;
		object = triples[triplesReader].object;
		if ((++triplesReader) >= triples.size()) {
			triples.clear();
			triplesReader = 0;
		}
		return true;
	}

	// No, check if the input is done
	Lexer::Token token;
	while (true) {
		token = lexer.next(subject);
		if (token == Lexer::Eof)
			return false;

		// A directive?
		if (token == Lexer::At) {
			parseDirective();
			continue;
		} else
			break;
	}

	// No, parse a triple
	parseTriple(token, subject, predicate, object, objType);
	return true;
}
//---------------------------------------------------------------------------
