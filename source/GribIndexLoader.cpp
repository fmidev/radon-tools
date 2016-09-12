#include "GribIndexLoader.h"
#include <stdlib.h>
#include <sstream>
#include <iomanip>
#include <boost/filesystem.hpp>
#include <boost/lexical_cast.hpp>
#include "NFmiNeonsDB.h"
#include "NFmiRadonDB.h"
#include <boost/algorithm/string.hpp>
#include <boost/algorithm/string/split.hpp>

extern Options options;

using namespace std;

bool GribIndexLoader::Load(const string &theInfile, const std::string &theKeys)
{
 
  // create a grib index
  itsIndexFilename = CreateIndex(theInfile);

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

  return true;
}

/*
 * Largely taken from GribLoader
 * But only meta data is processed, no message copy. File from index file is passed.
 */

void GribIndexLoader::Process(BDAPLoader& databaseLoader, NFmiGribMessage& message, short threadId)
{
    fc_info g;

    timespec start_ms_ts, stop_ms_ts, start_ts, stop_ts;

    if (options.verbose)
    {
      clock_gettime(CLOCK_REALTIME, &start_ms_ts);
    }

    /*
     * Read metadata from grib msg
     */

    if (!CopyMetaData(databaseLoader, g, message))
    {
      g_skipped++;
      return;
    }

    if (parameters.size() > 0)
    {
      if (std::find(parameters.begin(), parameters.end(), g.parname) == parameters.end())
      {
        g_skipped++;
        return;
      }
    }

    if (levels.size() > 0)
    {
      if (std::find(levels.begin(), levels.end(), g.levname) == levels.end())
      {
        g_skipped++;
        return;
      }
    }

    if (itsIndexFilename.empty())
      exit(1);

    g.filename = itsIndexFilename;

    /*
     * Update new file information to database
     */

    clock_gettime(CLOCK_REALTIME, &start_ts);

    if (options.neons)
    {
      if (!databaseLoader.WriteAS(g))
      {
        g_failed++;
        return;
      }
    }

    if (options.radon)
    {
      if (!databaseLoader.WriteToRadon(g) && !options.neons)
      {
        g_failed++;
        return;
      }
    }

    clock_gettime(CLOCK_REALTIME, &stop_ts);
    size_t start = static_cast<size_t> (start_ts.tv_sec*1000000000 + start_ts.tv_nsec);
    size_t stop =  static_cast<size_t> (stop_ts.tv_sec*1000000000 + stop_ts.tv_nsec);
    size_t databaseTime = (stop - start) / 1000 / 1000;

    g_success++;

    if (options.verbose)
    {
      clock_gettime(CLOCK_REALTIME, &stop_ms_ts);
      start = static_cast<size_t> (start_ms_ts.tv_sec*1000000000 + start_ms_ts.tv_nsec);
      stop = static_cast<size_t> (stop_ms_ts.tv_sec*1000000000 + stop_ms_ts.tv_nsec);
      size_t messageTime = (stop - start) / 1000 / 1000;

      size_t otherTime = messageTime - databaseTime;

      string ftype = "";

      if (g.forecast_type_id > 2)
      {
        ftype = "forecast type " + boost::lexical_cast<string> (g.forecast_type_id) + "/" + boost::lexical_cast<string> (g.forecast_type_value);
      }

      printf("Thread %d: Parameter %s at level %s/%ld %s, database time=%ld, other=%ld, total=%ld ms\n",
                          threadId, g.parname.c_str(), g.levname.c_str(), g.lvl1_lvl2, ftype.c_str(), databaseTime, otherTime, messageTime);
    }

}

/*
 * Create an index file in the same directory as the grib file.
 * If index file already exists grib file is added to the index
 * File name must be given as absolute path.
 */
string GribIndexLoader::CreateIndex(const string& theFileName)
{
    namespace fs = boost::filesystem;

    fs::path pathname(theFileName);
    string idxFileName;

    if (fs::is_directory(pathname.parent_path()))
    {
      idxFileName = pathname.parent_path().string() + "/" + pathname.stem().string() + ".idx";

      if (options.verbose)
        cout << "Creating Index file " << idxFileName << endl;

      if (fs::exists(fs::path(idxFileName)))
      {
        itsReader.Open(idxFileName);
        itsReader.AddFileToIndex(theFileName);
      }
      else
      {
        itsReader.BuildIndex(theFileName,options.keys);
      }
      itsReader.WriteIndex(idxFileName);
    }
    else
    {
        cout << "Parent path not specified. Absolute path needed." << endl;
    }

    return idxFileName;
}

