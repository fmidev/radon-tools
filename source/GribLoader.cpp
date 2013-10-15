#include "GribLoader.h"
#include <stdlib.h>
#include <sstream>
#include <iomanip>
#include <boost/filesystem.hpp>
#include <boost/lexical_cast.hpp>
#include "NFmiNeonsDB.h"
#include <boost/algorithm/string.hpp>
#include <boost/algorithm/string/split.hpp>

using namespace std;

GribLoader::GribLoader() 
{
}

GribLoader::~GribLoader() 
{
}

bool GribLoader::Load(const string &theInfile) 
{

  NFmiGrib reader(theInfile);

  // Read all message from file

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

  map<string,string> pskip;
  map<string,string> lskip;
  int success = 0;
  int failed = 0;

  while (reader.NextMessage()) 
  {

    if (Verbose())
      cout << "Reading message " << reader.CurrentMessageIndex() << "/" << reader.MessageCount() << endl;

    fc_info g;

    /*
     * Read metadata from grib msg
     */

    if (!CopyMetaData(g, reader))
      continue;

    if (pskip.count(g.parname) > 0)
       continue;

    if (lskip.count(g.levname) > 0)
       continue;

    if (parameters.size() > 0) 
    {
      if (std::find(parameters.begin(), parameters.end(), g.parname) == parameters.end()) 
      {

        if (Verbose())
          cout << "Skipping parameter " << g.parname << endl;

        pskip[g.parname] = 1;

        continue;
      }
    }

    if (levels.size() > 0) 
    {
      if (std::find(levels.begin(), levels.end(), g.levname) == levels.end()) 
      {

        if (Verbose())
          cout << "Skipping level " << g.levname << endl;

        lskip[g.levname] = 1;

        continue;
      }
    }

    if (Verbose())
      cout << "Parameter: " << g.parname << " at level " << g.levname << " " << g.lvl1_lvl2 << endl;

    string theFileName = REFFileName(g);

    if (theFileName.empty())
      exit(1);

    g.filename = theFileName;

    namespace fs = boost::filesystem;

    fs::path pathname(theFileName);

    if (!fs::is_directory(pathname.parent_path())) 
    {

      // Create directory

      if (Verbose())
        cout << "Creating directory " << pathname.parent_path().string() << endl;

      if (!DryRun())
        fs::create_directories(pathname.parent_path());
    }

    /*
     * Write grib msg to disk with unique filename.
     */

    if (!DryRun()) 
    {
      if (!reader.WriteMessage(theFileName))
      {
        failed++;
        return false;
      }
    }

    /*
     * Update new file information to database
     */

    if (!WriteAS(g)) 
    {
      failed++;
      return false;
    }

    success++;

  }

  cout << "Loaded " << success << " fields successfully\n";

  if (failed > 0)
  {
    cout << "Error occured with " << failed << " fields" << endl;
  }

  return true;
}



/*
 * ReadMetaData(grib_handle)
 *
 * Read all necessary metadata from a grib message. Structure of function
 * copied from PutGribMsgToNeons_api() (putgribmsgtoneons_api.c:87)
 */

bool GribLoader::CopyMetaData(fc_info &g, NFmiGrib &reader) 
{

  g.centre = reader.Message()->Centre();
  g.ednum = reader.Message()->Edition();

  g.param = reader.Message()->ParameterNumber();
  g.levtype = reader.Message()->LevelType();

  g.process = reader.Message()->Process();

  if (Process() != 0)
    g.process = Process();

  // g.stepType = reader.Message()->TimeRangeIndicator();
  // arome-case. Haetaan nimi, jos stepType erisuuri kuin 0

  if (g.ednum == 1) 
  {
	 g.filetype = "grib";

    g.novers = reader.Message()->Table2Version();

	  g.parname = NFmiNeonsDB::Instance().GetGridParameterName(g.param, g.novers, g.novers);
	  g.levname = NFmiNeonsDB::Instance().GetGridLevelName(g.param, g.levtype, g.novers, g.novers);

  }
  else 
  {

	  g.filetype = "grib2";

	  g.parname = NFmiNeonsDB::Instance().GetGridParameterName(g.param, reader.Message()->ParameterCategory(), reader.Message()->ParameterDiscipline(), g.process);
	  g.levname = NFmiNeonsDB::Instance().GetGridLevelName(g.levtype, g.process);
  }

  if (g.parname.empty()) 
  {
    cerr << "Parameter name not found for number " << g.param << endl;
    return false;
  }
  else if (g.levname.empty()) 
  {
    cerr << "Level name not found for level " << g.levtype << endl;
    return false;
  }

  g.year = reader.Message()->Year();
  g.month = reader.Message()->Month();
  g.day = reader.Message()->Day();
  g.hour = reader.Message()->Hour();
  g.minute = reader.Message()->Minute();

  g.ni = reader.Message()->SizeX();
  g.nj = reader.Message()->SizeY();

  //g.di = reader.Message()->iDirectionIncrement();
  //g.dj = reader.Message()->jDirectionIncrement();

  g.lat = reader.Message()->Y0() * 1000;
  g.lon = reader.Message()->X0() * 1000;

  // This is because we need to find the 
  // correct geometry from GRID_REG_GEOM in neons
  
  // GRIB2 longitudes --> GRIB1 
  if (g.ednum == 2 && (g.lon > 180000)) 
  {  
    g.lon -= 360000;
  }
  else if (g.ednum == 1 && g.lon == -180000 && g.centre != 86) 
  {
    g.lon += 360000;  // Area is whole globe, ECMWF special case
  } 

  switch (reader.Message()->NormalizedGridType()) 
  {
    case 0: // ll
      g.di = reader.Message()->iDirectionIncrement();
      g.dj = reader.Message()->jDirectionIncrement();
      g.di *= 1000;
      g.dj *= 1000;
      g.grtyp = "ll";
      break;

    case 10: // rll
      g.di = reader.Message()->iDirectionIncrement();
      g.dj = reader.Message()->jDirectionIncrement();
      g.di *= 1000;
      g.dj *= 1000;
      g.grtyp = "rll";
      break;

    case 5: // ps, ei tarkoita puoluetta
      g.di = reader.Message()->XLengthInMeters();
      g.dj = reader.Message()->YLengthInMeters();
      g.grtyp = "ps";
      break;

    default:
      cerr << "Invalid geometry for GRIB: only latlon, rotated latlon and polster are supported" << endl;
      return false;
      break;

  }

  stringstream ss;

  ss << g.year
     << setw(2)
     << setfill('0')
     << g.month
     << setw(2)
     << setfill('0')
     << g.day
     << setw(2)
     << setfill('0')
     << g.hour
     << "0000";

  g.base_date = ss.str();

  g.level1 = reader.Message()->LevelValue();
  g.level2 = 0;

  g.lvl1_lvl2 = g.level1 + 1000 * g.level2;

  g.locdef = 0; 
  // if exists, otherwise zero
  if (reader.Message()->KeyExists("localDefinitionNumber"))
  {
    g.locdef = reader.Message()->LocalDefinitionNumber();  
  }

  if (g.ednum == 1) 
  {

     if (g.locdef == 1 || g.locdef == 2 || g.locdef == 5 || g.locdef == 15 || g.locdef == 16 || g.locdef == 30) 
     {
       g.ldeftype = reader.Message()->DataType();
       g.ldefnumber = reader.Message()->PerturbationNumber();
     }
     else if (g.locdef == 19) 
     {
       g.ldefnumber = 0;
     }
   }
   else if (g.ednum == 2) 
   {

     switch (g.locdef) 
     {
       case 1:
       case 11:
         g.ldeftype = reader.Message()->TypeOfEnsembleForecast();
         g.ldefnumber = reader.Message()->PerturbationNumber();
         break;

       case 2:
       case 12:
         g.ldeftype = reader.Message()->DerivedForecast();
         g.ldefnumber = reader.Message()->NumberOfForecastsInTheEnsemble();
         break;

       case 3:
       case 4:
       case 13:
       case 14:
         g.ldeftype = reader.Message()->DerivedForecast();
         g.ldefnumber = reader.Message()->ClusterIdentifier();
         break;

       case 5:
       case 9:
         g.ldeftype = reader.Message()->ForecastProbabilityNumber();
         g.ldefnumber = reader.Message()->ProbabilityType();
         break;

       case 6:
       case 10:
         g.ldeftype = reader.Message()->PercentileValue();
         g.ldefnumber = 0;
         break;

       case 8:
         g.ldeftype = reader.Message()->NumberOfTimeRange();
         g.ldefnumber = reader.Message()->TypeOfTimeIncrement();
         g.step = reader.Message()->EndStep();
         break;
     }
   }

  if (g.locdef > 0)
    g.eps_specifier = boost::lexical_cast<string> (g.locdef) + "_" + boost::lexical_cast<string> (g.ldeftype) + "_" + boost::lexical_cast<string> (g.ldefnumber);
  else
    g.eps_specifier = "0";

  g.stepType = reader.Message()->TimeRangeIndicator();
  g.timeUnit = reader.Message()->StepUnits();
  
  switch (g.stepType) 
  {
    case 0:
    case 1:
    case 10:
      {   // Harmonie case
      if (g.centre == 86 && g.process == 3) 
      {
        long P1 = reader.Message()->P1();
        long P2 = reader.Message()->P2();
        g.step = (P1 << 8 ) | P2;
      }
      else 
      {
        g.step = reader.Message()->StepRange();
      }
      }
    case 2:
    case 3:
    case 4:
    case 5:
      g.startstep = reader.Message()->StartStep();
      g.endstep = reader.Message()->EndStep();

      g.step = g.endstep;
      break;
  }

  if (g.timeUnit == 10) 
  {
    g.step = g.step * 3;
  }
  else if (g.timeUnit == 11) 
  {
    g.step = g.step * 6;
  }
  else if (g.timeUnit == 12) 
  {
    g.step = g.step * 12;
  }

  g.fcst_per = g.step;

  return true;
}
