#include "BDAPLoader.h"
#include <string>

class GeoTIFFLoader
{
   public:
	GeoTIFFLoader();
	~GeoTIFFLoader() = default;

	bool Load(const std::string& theInfile);

   private:
	BDAPLoader itsDatabaseLoader;
	std::string itsHostName;
};
