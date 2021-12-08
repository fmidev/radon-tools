#ifndef _OPTIONS_
#define _OPTIONS_

#include <string>
#include <vector>

namespace grid_to_radon
{
struct Options
{
	Options()
	    : netcdf(false),
	      grib(false),
	      geotiff(false),
	      s3(false),
	      producer(0),
	      analysistime(""),
	      infile(),
	      level(""),
	      use_level_value(false),
	      use_inverse_level_value(false),
	      max_failures(-1),
	      max_skipped(-1),
	      dry_run(false),
	      threadcount(2),
	      ss_state_update(true),
	      in_place_insert(false),
	      directory_structure_check(true),
	      ss_table_name(""),
	      allow_multi_table_gribs(false),
	      metadata_file_name()
	{
	}

	bool netcdf;               // -n
	bool grib;                 // -g
	bool geotiff;              // -G
	bool s3;                   // -s
	unsigned int producer;     // -p
	std::string analysistime;  // -a
	std::vector<std::string> infile;
	std::string level;               // -L
	bool use_level_value;            // --use-level-value
	bool use_inverse_level_value;    // --use-inverse-level-value
	int max_failures;                // --max-failures
	int max_skipped;                 // --max-skipped
	bool dry_run;                    // -d;
	short threadcount;               // -j
	bool ss_state_update;            // -X
	bool in_place_insert;            // -I
	bool directory_structure_check;  // --no-directory-structure-check
	std::string ss_table_name;       // --smartmet-server-table-name
	bool allow_multi_table_gribs;    // --allow-multi-table-gribs
	std::string metadata_file_name;  // --metadata-file-name, -m
};
}  // namespace grid_to_radon

#endif
