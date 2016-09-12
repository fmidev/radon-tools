/*
 * class GribIndexLoader
 *
 * Used to load an index to grib data (edition 1 or 2) into NEONS/RADON.
 */

#include "GribLoader.h"

class GribIndexLoader : public GribLoader
{

  public:

    bool Load(const std::string &theInfile, const std::string &theKeys);
  protected:
    std::string CreateIndex(const std::string& theFileName);
    void Process(BDAPLoader& databaseLoader, NFmiGribMessage& message, short threadId);

    std::string itsIndexFilename;
};
