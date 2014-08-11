/*
 * class GribLoader
 *
 * Used to load grib data (edition 1 or 2) into NEONS.
 */

#include <string>
#include "BDAPLoader.h"
#include "NFmiGrib.h"

class GribLoader
{

  public:
    GribLoader();
    ~GribLoader();

    bool Load(const std::string &theInfile);

  private:

    bool CopyMetaData(fc_info &g, NFmiGrib &reader);
	BDAPLoader itsDatabaseLoader;
	
};
