#include "logger.h"
#include "record.h"
#include <string>

namespace grid_to_radon
{
class NetCDFLoader
{
   public:
	NetCDFLoader();
	~NetCDFLoader() = default;

	std::pair<bool, records> Load(const std::string& theInfile) const;

   private:
	std::string itsHostName;
	himan::logger itsLogger;
};
}
