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

std::pair<bool, grid_to_radon::records> grid_to_radon::GeoTIFFLoader::Load(const std::string& theInfile) const
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
		return std::make_pair(false, grid_to_radon::records{});
	}

	logr.Info("Read metadata in " + std::to_string(t.GetTime()) + " ms");

	auto radon = GET_PLUGIN(radon);

	int success = 0, failed = 0;
	grid_to_radon::records recs;

	int bandNo = 1;
	for (auto& info : infos)
	{
		t.Start();
		const std::string theFileName = grid_to_radon::common::MakeFileName(config, info, theInfile);
		finfo.message_no = bandNo;

		const auto ret = grid_to_radon::common::SaveToDatabase(config, info, radon, finfo);

		if (ret.first)
		{
			success++;
			recs.push_back(ret.second);
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

	const bool retval = common::CheckForFailure(failed, 0, success);

	return std::make_pair(retval, recs);
}
