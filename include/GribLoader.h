/*
 * class GribLoader
 *
 * Used to load grib data (edition 1 or 2) into NEONS.
 */

#include <string>
#include "BDAPLoader.h"
#include "NFmiGrib.h"
#include <memory>
#include <boost/thread.hpp>

class GribLoader
{

  public:
    GribLoader();
    ~GribLoader();

    bool Load(const std::string &theInfile);
protected:
    void Run(short threadId);

  private:
    std::unique_ptr<NFmiGribMessage> DistributeMessages();

    NFmiGrib itsReader;
    boost::thread_specific_ptr<BDAPLoader> itsThreadedLoader;
};
