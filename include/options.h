#ifndef _OPTIONS_
#define _OPTIONS_

struct Options
{
  Options()
  : verbose(false)
  , netcdf(false)
  , grib(false)
  , center(86)
  , process(0)
  , analysistime("")
  , infile("")
  , parameters("")
  , level("")
  , use_level_value(false)
  , use_inverse_level_value(false)
  , dry_run(false)
  , leveltypes("") {}

  bool verbose;                 // -v
  bool netcdf;                  // -n
  bool grib;                    // -g
  unsigned int center;          // -c
  unsigned int process;         // -p
  std::string analysistime;     // -a
  std::string infile;
  std::string parameters;       // -P
  std::string level;            // -L
  bool use_level_value;         // --use-level-value
  bool use_inverse_level_value; // --use-inverse-level-value
  bool dry_run;                 // -d;
  std::string leveltypes;       // -l
};

#endif
