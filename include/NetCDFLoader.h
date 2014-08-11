/*
 * class NetCDFLoader
 *
 * Used to load NetCDF data into NEONS.
 */

#include <string>
#include "BDAPLoader.h"

class NetCDFLoader
{

  public:
    NetCDFLoader();
    ~NetCDFLoader();

    bool Load(const std::string &theInfile);

  private:
    long Epoch(const std::string &dateTime, const std::string &mask);
	BDAPLoader itsDatabaseLoader;

};
