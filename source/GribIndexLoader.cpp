#include "GribIndexLoader.h"
#include "NFmiNeonsDB.h"
#include "NFmiRadonDB.h"
#include <boost/algorithm/string.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/filesystem.hpp>
#include <boost/lexical_cast.hpp>
#include <iomanip>
#include <sstream>
#include <stdlib.h>

extern Options options;

using namespace std;

GribIndexLoader::~GribIndexLoader() {}
bool GribIndexLoader::Load(const string &theInfile, const std::string &theKeys)
{
	// create a grib index
	itsIndexFileName = CreateIndex(theInfile);

	itsReader.Open(theInfile);

	vector<boost::thread> threadgroup;

	for (short i = 0; i < options.threadcount; i++)
	{
		threadgroup.push_back(boost::thread(&GribIndexLoader::Run, this, i));
	}

	for (unsigned short i = 0; i < threadgroup.size(); i++)
	{
		threadgroup[i].join();
	}

	cout << "Success with " << g_success << " fields, "
	     << "failed with " << g_failed << " fields, "
	     << "skipped " << g_skipped << " fields" << std::endl;

	return true;
}

string GribIndexLoader::GetFileName(BDAPLoader &databaseLoader, const fc_info &g) { return itsIndexFileName; }
/*
 * Create an index file in the same directory as the grib file.
 * If index file already exists grib file is added to the index
 * File name must be given as absolute path.
 */
string GribIndexLoader::CreateIndex(const string &theFileName)
{
	namespace fs = boost::filesystem;

	fs::path pathname(theFileName);
	pathname = fs::system_complete(pathname);

	string idxFileName;
	string dirName = pathname.parent_path().string();

	if (fs::is_directory(pathname.parent_path()))
	{
		idxFileName = dirName + "/" + pathname.stem().string() + ".idx";

		if (options.verbose) cout << "Creating Index file " << idxFileName << endl;

		if (fs::exists(fs::path(idxFileName)))
		{
			itsReader.Open(idxFileName);
			itsReader.AddFileToIndex(theFileName);
		}
		else
		{
			itsReader.BuildIndex(dirName + "/" + theFileName, options.keys);
		}
		if (!options.dry_run)
		{
			itsReader.WriteIndex(idxFileName);
		}
	}
	else
	{
		cout << "Parent path not found." << endl;
		exit(1);
	}

	return idxFileName;
}
