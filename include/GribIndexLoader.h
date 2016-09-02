/*
 * class GribIndexLoader
 *
 * Used to load an index to grib data (edition 1 or 2) into NEONS/RADON.
 */

#include "BDAPLoader.h"
#include "NFmiGrib.h"
#include "NFmiGribMessage.h"
#include <boost/thread.hpp>
#include "options.h"
#include <atomic>
#include <mutex>
#include <string>

class GribIndexLoader
{

  public:
    GribIndexLoader();
    ~GribIndexLoader();

    bool Load(const std::string &theInfile, const std::string &theKeys);
  protected:
    void Run(short threadId);

  private:
    std::string CreateIndex(const std::string& theFileName);
    void Process(BDAPLoader& databaseLoader, NFmiGribMessage& message, short threadId);
    bool CopyMetaData(BDAPLoader& databaseLoader, fc_info &g, const NFmiGribMessage &message);
    bool DistributeMessages(NFmiGribMessage& newMessage);

    NFmiGrib itsReader;
    std::string itsIndexFilename;
    boost::thread_specific_ptr<BDAPLoader> itsThreadedLoader;

    std::vector<std::string> parameters;
    std::vector<std::string> levels;

    std::atomic<int> g_success;
    std::atomic<int> g_skipped;
    std::atomic<int> g_failed;

    std::mutex distMutex;
};
