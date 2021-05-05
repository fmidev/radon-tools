#include "record.h"
#include <string>

namespace grid_to_radon
{
class GeoTIFFLoader
{
   public:
	GeoTIFFLoader() = default;
	~GeoTIFFLoader() = default;

	std::pair<bool, records> Load(const std::string& theInfile) const;
};
}
