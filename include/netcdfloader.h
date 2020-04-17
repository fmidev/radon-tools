#include <logger.h>
#include <string>

namespace grid_to_radon
{
class NetCDFLoader
{
   public:
	NetCDFLoader();
	~NetCDFLoader() = default;

	bool Load(const std::string& theInfile);

   private:
	std::string itsHostName;
	himan::logger itsLogger;
	std::set<std::string> ssStateInformation;
};
}
