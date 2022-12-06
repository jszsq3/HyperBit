#ifndef SPARQLLexer_H_
#define SPARQLLexer_H_
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
#include <string.h>
#include <iostream>
#include "TripleBit.h"
//---------------------------------------------------------------------------
/// A lexer for SPARQL input
class SPARQLLexer {
public:
	/// Possible tokens
	enum Token {
		None, Error, Eof, IRI, String, Bool, Char, Integer, Double, Variable, Identifier, Colon, Semicolon, Comma, Dot, Star, Underscore, LCurly, RCurly, LParen, RParen, LBracket, RBracket, Anon, Equal, NotEqual
	};

private:
	/// The input
	std::string input;
	/// The current position
	std::string::const_iterator pos;
	/// The start of the current token
	std::string::const_iterator tokenStart;
	/// The end of the curent token. Only set if delimiters are stripped
	std::string::const_iterator tokenEnd;
	/// The token put back with unget
	Token putBack;
	/// Was the Token end set?
	bool hasTokenEnd;

public:
	/// Constructor
	SPARQLLexer(const std::string& input);
	/// Destructor
	~SPARQLLexer();

	/// Get the next token
	Token getNext();
	/// Get the value of the current token
	std::string getTokenValue() const;
	/// Check if the current token matches a keyword
	bool isKeyword(const char* keyword) const;
	/// Put the last token back
	void unget(Token value) {
		putBack = value;
	}

	double getValueFromToken(const std::string& value, DataType& dataType);
	bool lexDate(std::string &str, double& date);
	Token getNumberType(std::string &s);

	inline static std::string strim(std::string &s) {
		if (s.empty()) {
			return s;
		}
		s.erase(0, s.find_first_not_of(" "));
		s.erase(s.find_last_not_of(" ") + 1);
		return s;
	}
};
//---------------------------------------------------------------------------
#endif
