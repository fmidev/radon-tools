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

  if (!AnalysisTime().empty()) 
  {
    reader.AnalysisTime(AnalysisTime());
  }

  if (!reader.IsConvention()) 
  {
    cerr << "File '" << theInfile << "' is not CF conforming NetCDF" << endl;
    return false;
  }

  if (reader.AnalysisTime().empty() && AnalysisTime().empty()) 
  {
    cerr << "Unable to determine analysistime for input file '" << theInfile << "'" << endl;
    return false;
  }

  if (Verbose()) 
  {
    cout << "Read " << reader.SizeZ() << " levels," << endl
         << "     " << reader.SizeX() << " x coordinates," << endl
         << "     " << reader.SizeY() << " y coordinates," << endl
         << "     " << reader.SizeT() << " timesteps " << endl
         << "     " << reader.SizeParams() << " parameters from file '" << theInfile << "'" << endl;

  }

  /* Set struct fcinfo accordingly */

  fc_info info;

  // Set centre (hard-coded to 86)
  // In the future we might have some way of picking this either from the file
  // or from command line

  info.centre = 86;

  // Set process for debugging purposes. In the future it must be picked
  // either from input file or from command line

  if (itsProcess == 0) 
  {
    cerr << "process value not found" << endl;
    return false;
  }

  info.process = itsProcess;

  if (itsAnalysisTime.size() < 10) 
  {
    cerr << "Invalid format for analysistime: " << itsAnalysisTime << endl;
    cerr << "Use YYYYMMDDHH24[MI]" << endl;
    return false;
  }

  info.year = boost::lexical_cast<int> (itsAnalysisTime.substr(0, 4));
  info.month = boost::lexical_cast<int> (itsAnalysisTime.substr(4, 2));
  info.day = boost::lexical_cast<int> (itsAnalysisTime.substr(6, 2));
  info.hour = boost::lexical_cast<int> (itsAnalysisTime.substr(8, 2));
  info.minute = 0;

  if (itsAnalysisTime.length() > 10)
    info.minute = boost::lexical_cast<int> (itsAnalysisTime.substr(10, 2));

  // Pretend this is grib1

  info.ednum = 1;
  info.level2 = 0;
  info.locdef = 0;
  info.eps_specifier = "0";

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

  long atimeEpoch = Epoch(AnalysisTime(), "%Y%m%d%H%M");

  map<string, short> pskip;

  vector<string> levels;

  string levelString = Level();

  if (!levelString.empty()) 
  {
    boost::split(levels, levelString, boost::is_any_of(","), boost::token_compress_on);
  }

  vector<string> parameters;

  string paramString = Parameters();

  if (!paramString.empty()) 
  {
    boost::split(parameters, paramString, boost::is_any_of(","), boost::token_compress_on);
  }

  for (reader.ResetTime(); reader.NextTime(); ) 
  {

    long fctimeEpoch = Epoch(boost::lexical_cast<string> (reader.Time()), reader.TimeUnit());

    float fctime = (fctimeEpoch - atimeEpoch)/3600;

    if (Verbose())
      cout << "Time " << reader.Time() << " (" << AnalysisTime() << " +" << fctime << " hours)" << endl;

    info.fcst_per = static_cast<int> (fctime);
    info.step = static_cast<int> (fctime);

    reader.FirstParam();

    do 
    {

      string ncname = reader.Param().Name();

      // If this parameter is known not to be supported, skip it

      if (pskip.count(ncname) > 0)
        continue;

      string grid_parameter_name = NFmiNeonsDB::Instance().GetGribParameterNameFromNetCDF(itsProcess, ncname);

      if (grid_parameter_name.empty()) 
      {

        if (Verbose())
          cout << "Param " << ncname << " not supported" << endl;

        pskip[ncname] = 1;
        continue;

      }

      // If parameter list is specified, check that parameter belongs to it

      if (parameters.size() > 0) 
      {
        if (std::find(parameters.begin(), parameters.end(), grid_parameter_name) == parameters.end()) 
        {

          if (Verbose())
            cout << "Skipping parameter " << grid_parameter_name << endl;

          pskip[ncname] = 1;
          continue;
        }
      }

      map<string, string> parameter = NFmiNeonsDB::Instance().GetParameterDefinition(itsProcess, grid_parameter_name);

      if (parameter.empty()) 
      {
        if (Verbose())
          cout << "Param " << ncname << " not supported" << endl;

         pskip[ncname] = 1;
         continue;
      }

      long univ_id = boost::lexical_cast<long> (parameter["univ_id"]);
      string name = parameter["parm_name"];

      info.param = univ_id;
      info.parname = grid_parameter_name;

      if (Verbose())
        cout << "Parameter " << ncname << " (" << univ_id << " " << name << ")" << endl;

      float level = kFloatMissing;

      info.level2 = 0;

      // Check level type

      if (itsLevel.empty()) 
      {
    	// Default

        info.levname = "HEIGHT";
        info.levtype = 105;

      } 
      else 
      {
        info.levname = itsLevel;
        if (itsLevel == "MEANSEA")
          info.levtype = 102;
        else if (itsLevel == "DEPTH")
          info.levtype = 160;
        else if (itsLevel == "HEIGHT")
          info.levtype = 105;
        else
          throw std::runtime_error("Invalid level type: " + itsLevel);
      }

      if (levels.size() > 0) 
      {
        if (std::find(levels.begin(), levels.end(), info.levname) == levels.end()) {

          if (Verbose())
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

        string theFileName = REFFileName(info);

        if (theFileName.empty())
          return false;

        info.filename = theFileName;

        if (!DryRun())
          if (!reader.WriteSlice(theFileName))
            return false;

        if (!DryRun())
          if (!WriteAS(info))
            return false;

        if (Verbose())
          cout << "Wrote z-dimensionless data to file '" << theFileName << "'" << endl;
      }
      else 
      {
        for (reader.ResetLevel(); reader.NextLevel(); ) 
        {

          if (itsUseLevelValue)
            level = reader.Level();
          else if (itsUseInverseLevelValue)
            level = reader.Level() * -1;
          else
            level = reader.LevelIndex(); // ordering number

          info.level1 = static_cast<int> (level);
          info.lvl1_lvl2 = info.level1 + 1000 * info.level2;

          string theFileName = REFFileName(info);

          if (theFileName.empty())
            return false;

          info.filename = theFileName;

          if (!DryRun())
            if (!reader.WriteSlice(theFileName))
              return false;

          if (!DryRun())
            if (!WriteAS(info))
              return false;

          if (Verbose())
            cout << "Wrote level " << reader.LevelIndex() << " (" << level << ")" << " to file '" << theFileName << "'" << endl;

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
  else 
  {

    cerr << "Invalid time mask: " << mask << endl;
    exit(1);
  }

  return e;

}
