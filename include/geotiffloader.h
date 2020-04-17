#include <string>

namespace grid_to_radon
{
class GeoTIFFLoader
{
   public:
	GeoTIFFLoader() = default;
	~GeoTIFFLoader() = default;

	bool Load(const std::string& theInfile);
};
}
