#include "NetCDFLoader.h"
#include "NFmiNetCDF.h"
#include <boost/lexical_cast.hpp>
#include "boost/date_time/gregorian/gregorian.hpp"
#include "boost/date_time/posix_time/posix_time.hpp"
#include <stdexcept>
#include <sstream>
#include <iomanip>
#include <ctime>
#include <algorithm>
#include "NFmiNeonsDB.h"
#include <boost/algorithm/string.hpp>
#include <boost/algorithm/string/split.hpp>
#include "options.h"
#include "fc_info.h"

extern Options options;

using namespace std;

#define kFloatMissing 32700.f

NetCDFLoader::NetCDFLoader() 
{

  // Epoch() function handles UTC only

  setenv("TZ", "UTC", 1);
}

NetCDFLoader::~NetCDFLoader() {}

bool NetCDFLoader::Load(const string &theInfile) 
{

  NFmiNetCDF reader;

  reader.Read(theInfile);

  if (options.analysistime.empty())
  {
    cerr << "Analysistime not specified" << endl;
    return false;
  }

  //reader.AnalysisTime(options.analysistime);

  if (!reader.IsConvention()) 
  {
    cerr << "File '" << theInfile << "' is not CF conforming NetCDF" << endl;
    return false;
  }

  if (options.verbose)
  {
    cout << "Read " << reader.SizeZ() << " levels," << endl
         << "     " << reader.SizeX() << " x coordinates," << endl
         << "     " << reader.SizeY() << " y coordinates," << endl
         << "     " << reader.SizeT() << " timesteps " << endl
         << "     " << reader.SizeParams() << " parameters from file '" << theInfile << "'" << endl;

  }

  /* Set struct fcinfo accordingly */

  fc_info info;

  info.centre = options.center;

  if (options.process == 0)
  {
    cerr << "process value not found" << endl;
    return false;
  }

  info.process = options.process;

  if (options.analysistime.size() < 10)
  {
    cerr << "Invalid format for analysistime: " << options.analysistime << endl;
    cerr << "Use YYYYMMDDHH24[MI]" << endl;
    return false;
  }

  info.year = boost::lexical_cast<int> (options.analysistime.substr(0, 4));
  info.month = boost::lexical_cast<int> (options.analysistime.substr(4, 2));
  info.day = boost::lexical_cast<int> (options.analysistime.substr(6, 2));
  info.hour = boost::lexical_cast<int> (options.analysistime.substr(8, 2));
  info.minute = 0;

  if (options.analysistime.length() > 10)
  {
    info.minute = boost::lexical_cast<int> (options.analysistime.substr(10, 2));
  }
  
  // ednum == 3 --> netcdf

  info.ednum = 3;
  info.level2 = 0;
  info.locdef = 0;
  info.eps_specifier = "0";
  info.timeRangeIndicator = 0;
  info.timeUnit = 1; // hour

  stringstream ss;

  ss << info.year
     << setw(2)
     << setfill('0')
     << info.month
     << setw(2)
     << setfill('0')
     << info.day
     << setw(2)
     << setfill('0')
     << info.hour;

  info.base_date = ss.str();

  info.ni = reader.SizeX();
  info.nj = reader.SizeY();

  info.lat = static_cast<int> (1000. * reader.Y0());
  info.lon = static_cast<int> (1000. * reader.X0());

  info.di = floor(reader.XResolution() * 1000);
  info.dj = floor(reader.YResolution() * 1000);

  info.filetype = "nc";

  if (reader.Projection() == "latitude_longitude")
    info.grtyp = "ll";

  else if (reader.Projection() == "rotated_latitude_longitude")
    info.grtyp = "rll";

  else
    throw runtime_error("Unsupported projection: " + reader.Projection());

  //float ftime = reader.ValueT(0);

  long atimeEpoch = Epoch(options.analysistime, "%Y%m%d%H%M");

  map<string, short> pskip;

  vector<string> levels;

  string levelString = options.level;

  if (!levelString.empty()) 
  {
    boost::split(levels, levelString, boost::is_any_of(","), boost::token_compress_on);
  }

  vector<string> parameters;

  string paramString = options.parameters;

  if (!paramString.empty()) 
  {
    boost::split(parameters, paramString, boost::is_any_of(","), boost::token_compress_on);
  }

  for (reader.ResetTime(); reader.NextTime(); ) 
  {

    long fctimeEpoch = Epoch(boost::lexical_cast<string> (reader.Time()), reader.TimeUnit());

    float fctime = (fctimeEpoch - atimeEpoch)/3600;

    if (options.verbose)
      cout << "Time " << static_cast<int> (reader.Time()) << " (" << options.analysistime << " +" << fctime << " hours)" << endl;

    info.fcst_per = static_cast<int> (fctime);
    info.step = static_cast<int> (fctime);

    reader.FirstParam();

    do 
    {

      string ncname = reader.Param().Name();

      info.ncname = ncname;

      // If this parameter is known not to be supported, skip it

      if (pskip.count(ncname) > 0)
        continue;

      string grid_parameter_name = itsDatabaseLoader.NeonsDB().GetGribParameterNameFromNetCDF(info.process, ncname);

      if (grid_parameter_name.empty()) 
      {

        if (options.verbose)
          cout << "NetCDF param " << ncname << " not supported" << endl;

        pskip[ncname] = 1;
        continue;

      }

      // If parameter list is specified, check that parameter belongs to it

      if (parameters.size() > 0) 
      {
        if (std::find(parameters.begin(), parameters.end(), grid_parameter_name) == parameters.end()) 
        {

          if (options.verbose)
            cout << "Skipping parameter " << grid_parameter_name << endl;

          pskip[ncname] = 1;
          continue;
        }
      }

      map<string, string> parameter = itsDatabaseLoader.NeonsDB().GetParameterDefinition(info.process, grid_parameter_name);

      if (parameter.empty()) 
      {
        if (options.verbose)
          cout << "Param " << grid_parameter_name << " not supported" << endl;

         pskip[ncname] = 1;
         continue;
      }

      long univ_id = boost::lexical_cast<long> (parameter["univ_id"]);
      string name = parameter["parm_name"];

      info.param = univ_id;
      info.parname = grid_parameter_name;

      if (options.verbose)
        cout << "Parameter " << ncname << " (" << univ_id << " " << name << ")" << endl;

      float level = kFloatMissing;

      info.level2 = 0;

      // Check level type

      if (options.level.empty())
      {
    	// Default

        info.levname = "HEIGHT";
        info.levtype = 105;

      } 
      else 
      {
        info.levname = boost::to_upper_copy(options.level);
        if (info.levname == "MEANSEA")
          info.levtype = 102;
        else if (info.levname == "DEPTH")
          info.levtype = 160;
        else if (info.levname == "HEIGHT")
          info.levtype = 105;
        else if (info.levname == "PRESSURE")
          info.levtype = 100;
        else
          throw std::runtime_error("Invalid level type: " + info.levname);
      }

      if (levels.size() > 0) 
      {
        if (std::find(levels.begin(), levels.end(), info.levname) == levels.end()) {

          if (options.verbose)
            cout << "Skipping level " << info.levname << endl;

          continue;
        }
      }

      if (!reader.HasDimension(reader.Param(), "z")) 
      {

    	// This parameter has no z dimension --> map to level 0

        level = 0;

        info.level1 = static_cast<int> (level);
        info.lvl1_lvl2 = info.level1 + 1000 * info.level2;

        string theFileName = itsDatabaseLoader.REFFileName(info);

        if (theFileName.empty())
          return false;

        info.filename = theFileName;

        if (!options.dry_run)
        {
          if (!reader.WriteSlice(theFileName))
          {
            return false;
          }
        }

        if (options.neons)
        {
          if (!itsDatabaseLoader.WriteAS(info))
          {
            return false;
          }
        }
		
        if (options.radon)
        {
          itsDatabaseLoader.WriteToRadon(info);
        }
		
        if (options.verbose)
        {
          cout << "Wrote z-dimensionless data to file '" << theFileName << "'" << endl;
        }
      }
      else 
      {
        for (reader.ResetLevel(); reader.NextLevel(); ) 
        {

          if (options.use_level_value)
            level = reader.Level();
          else if (options.use_inverse_level_value)
            level = reader.Level() * -1;
          else
            level = reader.LevelIndex(); // ordering number

          info.level1 = static_cast<int> (level);
          info.lvl1_lvl2 = info.level1 + 1000 * info.level2;

          string theFileName = itsDatabaseLoader.REFFileName(info);

          if (theFileName.empty())
            return false;

          info.filename = theFileName;

          if (!options.dry_run)
          {
            if (!reader.WriteSlice(theFileName))
            {
              return false;
            }
          }

          if (options.neons)
          {
            if (!itsDatabaseLoader.WriteAS(info))
            {
              return false;
            }
          }
		  
          if (options.radon)
          {
            itsDatabaseLoader.WriteToRadon(info);
          }
		  
          if (options.verbose)
          {
            cout << "Wrote level " << reader.LevelIndex() << " (" << level << ")" << " to file '" << theFileName << "'" << endl;
          }
        }
      }
    } while (reader.NextParam());
  }

  return true;
}

/*
 * Epoch()
 *
 * Convert a time to epoch. Date mask is mandatory.
 */

long NetCDFLoader::Epoch(const string &dateTime, const string &mask) 
{

  struct tm t = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
  char epoch[40];
  long e;

  if (mask == "%Y-%m-%d %H:%M:%S" || mask == "%Y%m%d%H%M%S" || mask == "%Y%m%d%H%M") {

    strptime(dateTime.c_str(), mask.c_str(), &t);

    strftime(epoch, 40, "%s", &t);

    try 
    {
      e = boost::lexical_cast<long> (epoch);
    } 
    catch(boost::bad_lexical_cast&) 
    {
      cerr << "Date cast failed" << endl;
      exit(1);
    }

  } 
   else if (mask == "hours since 1900-01-01 00:00:00") 
  {

    /*
     * Negative epochs (epoch from times before 1970-01-01) are not guaranteed
     * to work; use offset to adjust.
     */

    long offset = 2208988800; // seconds from 1900-01-01 to 1970-01-01

    try 
    {
      e = (3600 * boost::lexical_cast<long> (dateTime)) - offset;
    } 
    catch(boost::bad_lexical_cast&) 
    {
      cerr << "Date cast failed" << endl;
      exit(1);
    }

  } 
  else if (mask == "hours since 1970-01-01 00:00:00") 
  {

    try 
    {
      e = (3600 * boost::lexical_cast<long> (dateTime));
    } 
    catch(boost::bad_lexical_cast&) 
    {
      cerr << "Date cast failed" << endl;
      exit(1);
    }
  }
  else if (mask == "hours since 2014-01-01 00:00:00")
  {

    try
    {
      e = (3600 * boost::lexical_cast<long> (dateTime));
    }
    catch(boost::bad_lexical_cast&)
    {
      cerr << "Date cast failed" << endl;
      exit(1);
    }
  }
  else 
  {

    cerr << "Invalid time mask: " << mask << endl;
    exit(1);
  }

  return e;

}
