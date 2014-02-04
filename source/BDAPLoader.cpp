#include "BDAPLoader.h"
#include "NFmiNeonsDB.h"
#include <iostream>
#include <sstream>
#include <iomanip>
#include <boost/lexical_cast.hpp>

using namespace std;

BDAPLoader::BDAPLoader()
  : itsUsername("wetodb") // bdm has server-based authentication
  , itsPassword("3loHRgdio")
  , itsDatabase("neons")
  , itsVerbose(false)
  , itsProcess(0)
  , itsParameters("")
  , itsLevel("")
  , itsLevelTypes("")
  , itsUseLevelValue(false)
  , itsUseInverseLevelValue(false)
  , itsDryRun(false)
{

  char *dbName;

  if ((dbName = getenv("NEONS_DB")) != NULL)
    itsDatabase = static_cast<string> (dbName);

  NFmiNeonsDB::Instance().Connect(itsUsername, itsPassword, itsDatabase);
  Init();
}

BDAPLoader::~BDAPLoader() 
{
}

bool BDAPLoader::Verbose() 
{
  return itsVerbose;
}

void BDAPLoader::Verbose(bool theVerbose) 
{
  itsVerbose = theVerbose;
}

void BDAPLoader::Parameters(std::string theParameters) 
{
  itsParameters = theParameters;
}

string BDAPLoader::Parameters() 
{
  return itsParameters;
}

string BDAPLoader::REFFileName(const fc_info &info) 
{

  // Determine file name

  if (!ReadREFEnvironment())
    return "";

  stringstream ss;

  ss << base
     << "/"
     << info.centre
     << "_"
     << info.process
     << "/"
     << info.year
     << setw(2)
     << setfill('0')
     << info.month
     << setw(2)
     << setfill('0')
     << info.day
     << setw(2)
     << setfill('0')
     << info.hour
     << setw(2)
     << setfill('0')
     << info.minute;

  if (info.ednum == 1) 
  {
    ss << "/"
       << setw(0)
       << info.param
       << "_"
       << info.levtype
       << "_"
       << info.level1
       << "_"
       << info.level2
       << "_"
       << info.grtyp
       << "_"
       << info.ni
       << "_"
       << info.nj
       << "_"
       << info.timeRangeIndicator
       << "_0_"
       << setw(3)
       << setfill('0')
       << info.fcst_per;
   
      if (info.locdef != 0) 
      {
      ss << setw(0)
         << "_"
         << info.locdef
         << "_"
         << info.ldeftype
         << "_"
         << info.ldefnumber;
    }
    ss << "." << info.filetype;
  }
  else if (info.ednum == 2) 
  {
    ss << "/"
       << setw(0)
       << info.parname
       << "_"
       << info.levname
       << "_"
       << info.level1
       << "_"
       << info.level2
       << "_"
       << info.grtyp
       << "_"
       << info.ni
       << "_"
       << info.nj
       << "_"
       << info.timeRangeIndicator
       << "_0_"
       << setw(3)
       << setfill('0')
       << info.fcst_per;

    if (info.locdef != 0) 
    {
      ss << setw(0)
         << "_"
         << info.locdef
         << "_"
         << info.ldeftype
         << "_"
         << info.ldefnumber;
    }

    ss << "." << info.filetype;
  }

  return ss.str();

}

void BDAPLoader::Init() 
{

  itsGeomName = "";
  itsModelName = "";
  itsModelType = "";
  itsTypeSmt = "";
  itsDsetId = "";
  itsTableName = "";
  itsRecCntDsetIni = "";

}

bool BDAPLoader::WriteAS(const fc_info &info) 
{

  if (info.process != itsProcess) 
  {
    // Clear cache if model id changes

    Init();
  }

  stringstream query;

  string outFileHost (host);

  string dset_name = "AF";

  vector<string> row;

  if (itsGeomName.empty()) 
  {
    query << "SELECT geom_name "
          << "FROM grid_reg_geom "
          << "WHERE row_cnt = " << info.nj
          << " AND col_cnt = " << info.ni
          << " AND lat_orig = " << info.lat
          << " AND long_orig = " << info.lon
          << " AND pas_latitude = " << info.dj
          << " AND pas_longitude = " << info.di;

    if (DryRun())
      cout << query.str() << endl;

    NFmiNeonsDB::Instance().Query(query.str());

    row = NFmiNeonsDB::Instance().FetchRow();

    if (row.empty()) 
    {
      cerr << "Geometry not found" << endl;
      return false;
    }

    itsGeomName = row[0];

    query.str("");

  }

  if (itsModelName.empty() || itsModelType.empty() || itsTypeSmt.empty()) 
  {

    query << "SELECT "
          << "m.model_name, "
          << "model_type, "
          << "type_smt "
          << "FROM grid_num_model_grib nu, "
          << "grid_model m, "
          << "grid_model_name na "
          << "WHERE nu.model_id = " << info.process
          << " AND nu.ident_id = " << info.centre
          << " AND m.flag_mod = 0 "
          << " AND nu.model_name = na.model_name "
          << " AND m.model_name = na.model_name";

    if (DryRun())
      cout << query.str() << endl;

    NFmiNeonsDB::Instance().Query(query.str());

    row = NFmiNeonsDB::Instance().FetchRow();

    if (row.empty()) 
    {
      cerr << "Model definition not found" << endl;
      return false;
    }

    itsModelName = row[0];
    itsModelType = row[1];
    itsTypeSmt = row[2];

    query.str("");

  }

  if (itsDsetId.empty() || itsTableName.empty() || itsRecCntDsetIni.empty()) 
  {

    query << "SELECT "
          << "dset_id, "
          << "table_name, "
          << "rec_cnt_dset "
          << "FROM as_grid "
          << "WHERE "
          << "model_type = '" << itsModelType << "'"
          << " AND geom_name = '" << itsGeomName << "'"
          << " AND dset_name = '" << dset_name << "'"
          << " AND base_date = '" << info.base_date << "'";

    NFmiNeonsDB::Instance().Query(query.str());

    if (DryRun())
      cout << query.str() << endl;

    row = NFmiNeonsDB::Instance().FetchRow();

    if (row.empty()) 
    {
      cerr << "Data set definition not found from NEONS table 'as_grid' for geometry '" << itsGeomName << "', base_date " << info.base_date << endl;
      cerr << "The data could be too old" << endl;
      return false;
    }

    itsDsetId = row[0];
    itsTableName = row[1];
    itsRecCntDsetIni = row[2];

    query.str("");

  }

  query << "UPDATE as_grid "
        << "SET rec_cnt_dset = "
        << "rec_cnt_dset + 1, "
        << "date_maj_dset = sysdate "
        << "WHERE dset_id = " << itsDsetId;

  if (DryRun())
    cout << query.str() << endl;

  try 
  {
    if (!DryRun())
      NFmiNeonsDB::Instance().Execute(query.str());
  } 
  catch (int e) 
  {
    cerr << "Error code: " << e << endl;
  }

  query.str("");

  query << "INSERT INTO " << itsTableName
        << " (dset_id, parm_name, lvl_type, lvl1_lvl2, fcst_per, eps_specifier, file_location, file_server) "
        << "VALUES ("
        << itsDsetId << ", "
        << "'" << info.parname << "', "
        << "'" << info.levname << "', "
        << info.lvl1_lvl2 << ", "
        << info.fcst_per << ", "
        << "'" << info.eps_specifier << "', "
        << "'" << info.filename << "', "
        << "'" << outFileHost << "')";

  if (DryRun())
    cout << query.str() << endl;

  try 
  {
    if (!DryRun())
      NFmiNeonsDB::Instance().Execute(query.str());
  } 
  catch (int e) 
  {
    if (e == 1) 
    {
      // ORA-00001: unique constraint violated

      /*
       * Primary key: DSET_ID, PARM_NAME, LVL_TYPE, LVL1_LVL2, FCST_PER, FILE_LOCATION, FILE_SERVER
       */

      query.str("");

      query << "UPDATE " << itsTableName
            << " SET eps_specifier = '" << info.eps_specifier << "'"
            << " WHERE "
            << "dset_id = " << itsDsetId
            << " AND parm_name = '" << info.parname << "'"
            << " AND lvl_type = '" << info.levname << "'"
            << " AND lvl1_lvl2 = " << info.lvl1_lvl2
            << " AND fcst_per = " << info.fcst_per
            << " AND file_location = '" << info.filename << "'"
            << " AND file_server = '" << outFileHost << "'";

      if (DryRun())
        cout << query.str() << endl;

      try 
      {
        if (!DryRun())
          NFmiNeonsDB::Instance().Execute(query.str());
      } 
      catch (int e) 
      {
        // Give up
        NFmiNeonsDB::Instance().Rollback();
        return false;
      }
    }
    else 
    {
      NFmiNeonsDB::Instance().Rollback();
      cerr << "Load failed with: " << itsTableName << "," << info.parname << "," << info.levname
			           << info.lvl1_lvl2 << "," << info.fcst_per << "," << info.filename << endl;
      return false;
    }
  }

  if (DryRun())
    NFmiNeonsDB::Instance().Rollback();
  else
    NFmiNeonsDB::Instance().Commit();

  return true;

}

unsigned long BDAPLoader::Process() 
{
  return itsProcess;
}

void BDAPLoader::Process(unsigned long theProcess) 
{
  itsProcess = theProcess;
}

string BDAPLoader::AnalysisTime()
{
  return itsAnalysisTime;
}

void BDAPLoader::AnalysisTime(string theAnalysisTime) 
{
  itsAnalysisTime = theAnalysisTime;
}

bool BDAPLoader::ReadREFEnvironment() 
{

  if ((base = getenv("NEONS_REF_BASE")) == NULL) 
  {
    cerr << "Environment variable 'NEONS_REF_BASE' not set" << endl;
    return false;
  }

  if ((host = getenv("HOSTNAME")) == NULL)
   {
    cerr << "Environment variable 'HOSTNAME' not set" << endl;
    return false;
  }

  return true;
}

void BDAPLoader::Level(std::string theLevel)
{
  itsLevel = theLevel;
}

string BDAPLoader::Level()
{
  return itsLevel;
}

string BDAPLoader::LevelTypes()
{
  return itsLevelTypes;
}

void BDAPLoader::LevelTypes(std::string theLevelTypes) 
{
  itsLevelTypes = theLevelTypes;
}

void BDAPLoader::UseLevelValue(bool theValue) 
{
  itsUseLevelValue = theValue;
}

void BDAPLoader::UseInverseLevelValue(bool theValue) 
{
  itsUseInverseLevelValue = theValue;
}

void BDAPLoader::DryRun(bool theDryRun) 
{
  itsDryRun = theDryRun;
}

bool BDAPLoader::DryRun()
{
  return itsDryRun;
}
