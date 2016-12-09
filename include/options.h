#ifndef _OPTIONS_
#define _OPTIONS_

struct Options
{
	Options()
	    : verbose(false),
	      netcdf(false),
	      grib(false),
	      index(false),
	      center(86),
	      process(0),
	      analysistime(""),
	      infile(""),
	      parameters(""),
	      keys(
	          "level:i,indicatorOfParameter:i,centre:i,date:i,generatingProcessIdentifier:i,indicatorOfTypeOfLevel:i,"
	          "step:i,time:i"),
	      level(""),
	      use_level_value(false),
	      use_inverse_level_value(false),
	      max_failures(-1),
	      max_skipped(-1),
	      dry_run(false),
	      leveltypes(""),
	      threadcount(2),
	      neons(true),
	      radon(true)
	{
	}

	bool verbose;  // -v
	bool netcdf;   // -n
	bool grib;     // -g
	bool index;
	unsigned int center;       // -c
	unsigned int process;      // -p
	std::string analysistime;  // -a
	std::string infile;
	std::string parameters;  // -P
	std::string keys;
	std::string level;             // -L
	bool use_level_value;          // --use-level-value
	bool use_inverse_level_value;  // --use-inverse-level-value
	int max_failures;              // --max-failures
	int max_skipped;               // --max-skipped
	bool dry_run;                  // -d;
	std::string leveltypes;        // -l
	short threadcount;             // -j
	bool neons;                    // -N
	bool radon;                    // -R
};

#endif
