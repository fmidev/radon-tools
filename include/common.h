#include <boost/filesystem.hpp>
#include <file_information.h>
#include <info.h>
#include <plugin_configuration.h>

namespace himan
{
namespace plugin
{
class radon;
}
}

namespace grid_to_radon
{
namespace common
{
bool SaveToDatabase(std::shared_ptr<himan::configuration>& config, std::shared_ptr<himan::info<double>>& info,
                    std::shared_ptr<himan::plugin::radon>& r, const himan::file_information& finfo,
                    std::set<std::string>& ssStateInformation);
void SaveSSStateInformation(std::shared_ptr<himan::configuration>& config, std::shared_ptr<himan::info<double>>& info,
                            std::set<std::string>& ssStateInformation, std::shared_ptr<himan::plugin::radon>& r);
void UpdateSSState(const std::set<std::string>& ssStateInformation);

std::string CanonicalFileName(const std::string& inputFileName);
std::string MakeFileName(std::shared_ptr<himan::configuration>& config, std::shared_ptr<himan::info<double>>& info,
                         const std::string& inputFileName);
bool CheckDirectoryStructure(const boost::filesystem::path& pathname);
void CreateDirectory(const std::string& theFileName);
}
}
