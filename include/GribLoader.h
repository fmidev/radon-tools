/*
 * class GribLoader
 *
 * Used to load grib data (edition 1 or 2) into NEONS.
 */

#include <string>
#include "BDAPLoader.h"
#include "NFmiGrib.h"
#include <grib_api.h>

class GribLoader : public BDAPLoader 
{

  public:
    GribLoader();
    virtual ~GribLoader();

    bool Load(const std::string &theInfile);

  private:

    std::string outFileName;
    std::string outFileHost;

    bool CopyMetaData(fc_info &g, NFmiGrib &reader);
    //bool MakeWriteREFStorage(grib_handle *h, const fc_info &g);
   // bool WriteAS(const fc_info &g);

};
