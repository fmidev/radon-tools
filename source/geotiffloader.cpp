#include "geotiffloader.h"
#include "common.h"
#include "options.h"
#include "plugin_factory.h"
#include "timer.h"

#define HIMAN_AUXILIARY_INCLUDE
#include "geotiff.h"
#include "radon.h"
#undef HIMAN_AUXILIARY_INCLUDE

extern grid_to_radon::Options options;

bool grid_to_radon::GeoTIFFLoader::Load(const std::string& theInfile)
{
	auto geotiffpl = GET_PLUGIN(geotiff);

	options.ss_state_update = false;
	himan::file_information finfo;
	finfo.storage_type = himan::kLocalFileSystem;
	finfo.message_no = boost::none;
	finfo.offset = boost::none;
	finfo.length = boost::none;
	finfo.file_location = common::CanonicalFileName(theInfile);
	finfo.file_type = himan::kGeoTIFF;

	auto config = std::make_shared<himan::configuration>();

	config->WriteToDatabase(true);
	config->WriteMode(himan::kSingleGridToAFile);
	config->DatabaseType(himan::kRadon);

	const himan::raw_time rt(options.analysistime, "%Y%m%d%H%M");
	himan::plugin::search_options opts(himan::forecast_time(rt, rt), himan::param(), himan::level(himan::kHeight, 0),
	                                   himan::producer(options.producer),
	                                   std::make_shared<himan::plugin_configuration>(*config));

	himan::timer t(true);
	std::vector<std::shared_ptr<himan::info<double>>> infos = geotiffpl->FromFile(finfo, opts, false, false);
	t.Stop();

	himan::logger logr("geotiffloader");

	if (infos.empty())
	{
		logr.Warning("No valid data read from file");
		return false;
	}

	logr.Info("Read metadata in " + std::to_string(t.GetTime()) + " ms");

	auto radon = GET_PLUGIN(radon);

	int success = 0, failed = 0;

	int bandNo = 1;
	for (auto& info : infos)
	{
		t.Start();
		const std::string theFileName = grid_to_radon::common::MakeFileName(config, info, theInfile);
		std::set<std::string> x;
		finfo.message_no = bandNo;
		if (grid_to_radon::common::SaveToDatabase(config, info, radon, finfo, x))
		{
			success++;
		}
		else
		{
			failed++;
		}

		t.Stop();

		logr.Info("Band #" + std::to_string(bandNo++) + " database time=" + std::to_string(t.GetTime()) + " ms");
	}

	logr.Info("Success with " + std::to_string(success) + " fields, " + "failed with " + std::to_string(failed) +
	          " fields");

	bool retval = true;

	if (options.max_failures >= 0 && failed > options.max_failures)
	{
		retval = false;
	}

	// We need to check for 'total failure' if the user didn't specify a max_failures value.
	if (options.max_failures == -1)
	{
		if (success == 0)
		{
			retval = false;
		}
	}

	return retval;
}
