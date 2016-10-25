/*
 * class GribIndexLoader
 *
 * Used to load an index to grib data (edition 1 or 2) into NEONS/RADON.
 */

#include "GribLoader.h"

#if defined __GNUC__ && (__GNUC__ < 4 || (__GNUC__ == 4 && __GNUC_MINOR__ <= 6))
#define override // override specifier not support until 4.8
#endif

class GribIndexLoader : public GribLoader
{
   public:
	~GribIndexLoader() override;

	bool Load(const std::string &theInfile, const std::string &theKeys);

   protected:
	std::string CreateIndex(const std::string &theFileName);
	std::string GetFileName(BDAPLoader &databaseLoader, const fc_info &g) override;

	std::string itsIndexFileName;
};
