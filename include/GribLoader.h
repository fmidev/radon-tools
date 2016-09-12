/*
 * class GribLoader
 *
 * Used to load grib data (edition 1 or 2) into NEONS.
 */

#include <string>
#include "BDAPLoader.h"
#include "NFmiGrib.h"
#include <boost/thread.hpp>
#include "options.h"
#include <atomic>
#include <mutex>
#include <string>

#ifndef GRIBLOADER_H
#define GRIBLOADER_H

class GribLoader
{

  public:
    GribLoader();
    ~GribLoader();

    bool Load(const std::string &theInfile);
  protected:
    void Run(short threadId);
    bool DistributeMessages(NFmiGribMessage& newMessage);
    void Process(BDAPLoader& databaseLoader, NFmiGribMessage& message, short threadId);
    void CreateDirectory(const std::string& theFileName);
    bool CopyMetaData(BDAPLoader& databaseLoader, fc_info &g, const NFmiGribMessage &message);

    NFmiGrib itsReader;
    boost::thread_specific_ptr<BDAPLoader> itsThreadedLoader;

    std::vector<std::string> parameters;
    std::vector<std::string> levels;

    std::atomic<int> g_success;
    std::atomic<int> g_skipped;
    std::atomic<int> g_failed;

    std::mutex distMutex, dirCreateMutex;
};
#endif /* GRIBLOADER_H */
