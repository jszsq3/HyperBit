#include "Sorter.h"
#include "TempFile.h"
#include <vector>
#include <algorithm>
#include <cassert>
#include <cstring>
using namespace std;
//---------------------------------------------------------------------------
/// Maximum amount of usable memory. XXX detect at runtime!
static const unsigned memoryLimit = sizeof(void *) * (1 << 27);
//---------------------------------------------------------------------------
namespace
{
	//---------------------------------------------------------------------------
	/// A memory range
	struct Range
	{
		const uchar *from, *to;

		/// Constructor
		Range(const uchar *from, const uchar *to) : from(from), to(to)
		{
		}

		/// Some content?
		bool equals(const Range &o)
		{
			return ((to - from) == (o.to - o.from)) && (memcmp(from, o.from, to - from) == 0);
		}
	};
	//---------------------------------------------------------------------------
	/// Sort wrapper that colls the comparison function
	struct CompareSorter
	{
		/// Comparison function
		typedef int (*func)(const uchar *, const uchar *);

		/// Comparison function
		const func compare;

		/// Constructor
		CompareSorter(func compare) : compare(compare)
		{
		}

		/// Compare two entries
		bool operator()(const Range &a, const Range &b) const
		{
			return compare(a.from, b.from) < 0;
		}
	};
	//---------------------------------------------------------------------------
	static uchar *spool(uchar *ofs, TempFile &out, const vector<Range> &items, bool eliminateDuplicates)
	// Spool items to disk
	{
		Range last(0, 0);
		for (vector<Range>::const_iterator iter = items.begin(), limit = items.end(); iter != limit; ++iter)
		{
			if ((!eliminateDuplicates) || (!last.equals(*iter)))
			{
				last = *iter;
				out.write(last.to - last.from, last.from);
				ofs += last.to - last.from;
			}
		}
		return ofs;
	}
	//---------------------------------------------------------------------------
}
//---------------------------------------------------------------------------
void Sorter::sort(TempFile &in, TempFile &out, const uchar *(*skip)(const uchar *), int (*compare)(const uchar *, const uchar *), bool eliminateDuplicates)
// Sort a temporary file
{
	// Open the input
	in.close();
	MemoryMappedFile mappedIn;
	assert(mappedIn.open(in.getFile().c_str()));
	const uchar *reader = mappedIn.getBegin(), *limit = mappedIn.getEnd();

	// Produce runs
	vector<Range> runs;
	TempFile intermediate(out.getBaseFile());
	uchar *ofs = 0;
	while (reader < limit)
	{
		// Collect items
		vector<Range> items;
		const uchar *maxReader = reader + memoryLimit;
		while (reader < limit)
		{
			const uchar *start = reader;
			reader = skip(reader);
			items.push_back(Range(start, reader));

			// Memory Overflow?
			if ((reader + (sizeof(Range) * items.size())) > maxReader)
				break;
		}

		// Sort the run
		std::sort(items.begin(), items.end(), CompareSorter(compare));

		// Did everything fit?
		if ((reader == limit) && (runs.empty()))
		{
			spool(0, out, items, eliminateDuplicates);
			break;
		}

		// No, spool to intermediate file
		uchar *newOfs = spool(ofs, intermediate, items, eliminateDuplicates);
		runs.push_back(Range(ofs, newOfs));
		ofs = newOfs;
	}
	intermediate.close();
	mappedIn.close();

	// Do we habe to merge runs?
	if (!runs.empty())
	{
		// Map the ranges
		MemoryMappedFile tempIn;
		assert(tempIn.open(intermediate.getFile().c_str()));
		for (vector<Range>::iterator iter = runs.begin(), limit = runs.end(); iter != limit; ++iter)
		{
			(*iter).from = tempIn.getBegin() + ((*iter).from - static_cast<uchar *>(0));
			(*iter).to = tempIn.getBegin() + ((*iter).to - static_cast<uchar *>(0));
		}

		// Sort the run heads
		std::sort(runs.begin(), runs.end(), CompareSorter(compare));

		// And merge them
		Range last(0, 0);
		while (!runs.empty())
		{
			// Write the first entry if no duplicate
			Range head(runs.front().from, skip(runs.front().from));
			if ((!eliminateDuplicates) || (!last.equals(head)))
				out.write(head.to - head.from, head.from);
			last = head;

			// Update the first entry. First entry done?
			if ((runs.front().from = head.to) == runs.front().to)
			{
				runs[0] = runs[runs.size() - 1];
				runs.pop_back();
			}

			// Check the heap condition
			unsigned pos = 0, size = runs.size();
			while (pos < size)
			{
				unsigned left = 2 * pos + 1, right = left + 1;
				if (left >= size)
					break;
				if (right < size)
				{
					if (compare(runs[pos].from, runs[left].from) > 0)
					{
						if (compare(runs[pos].from, runs[right].from) > 0)
						{
							if (compare(runs[left].from, runs[right].from) < 0)
							{
								std::swap(runs[pos], runs[left]);
								pos = left;
							}
							else
							{
								std::swap(runs[pos], runs[right]);
								pos = right;
							}
						}
						else
						{
							std::swap(runs[pos], runs[left]);
							pos = left;
						}
					}
					else if (compare(runs[pos].from, runs[right].from) > 0)
					{
						std::swap(runs[pos], runs[right]);
						pos = right;
					}
					else
						break;
				}
				else
				{
					if (compare(runs[pos].from, runs[left].from) > 0)
					{
						std::swap(runs[pos], runs[left]);
						pos = left;
					}
					else
						break;
				}
			}
		}
		tempIn.close();
	}

	intermediate.discard();
	out.close();
}
//---------------------------------------------------------------------------
