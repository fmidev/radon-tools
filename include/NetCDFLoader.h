#include "BDAPLoader.h"
#include <string>

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
