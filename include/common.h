#include "record.h"
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
std::pair<bool, grid_to_radon::record> SaveToDatabase(std::shared_ptr<himan::configuration>& config,
                                                      std::shared_ptr<himan::info<double>>& info,
                                                      std::shared_ptr<himan::plugin::radon>& r,
                                                      const himan::file_information& finfo);
void UpdateSSState(const grid_to_radon::records& records);

std::string CanonicalFileName(const std::string& inputFileName);
std::string MakeFileName(std::shared_ptr<himan::configuration>& config, std::shared_ptr<himan::info<double>>& info,
                         const std::string& inputFileName);
void CreateDirectory(const std::string& theFileName);
bool CheckForFailure(int g_failed, int g_skipped, int g_success);
}
}
