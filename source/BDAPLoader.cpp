#include "BDAPLoader.h"
#include <iostream>
#include <sstream>
#include <iomanip>
#include <boost/lexical_cast.hpp>
#include "options.h"

using namespace std;

extern Options options;
once_flag oflag;

BDAPLoader::BDAPLoader()
  : itsUsername("wetodb")
  , itsPassword("3loHRgdio")
  , itsDatabase("neons")
  , base(0)
  , itsUseRadon(false)
{

  char *dbName;

  if ((dbName = getenv("NEONS_DB")) != NULL)
    itsDatabase = static_cast<string> (dbName);

  call_once(oflag, &BDAPLoader::InitPool, this, itsUsername, itsPassword, itsDatabase);
  
  if (options.neons)
  {
	  itsNeonsDB = std::unique_ptr<NFmiNeonsDB> (NFmiNeonsDBPool::Instance()->GetConnection());
  }
  
  if (options.radon)
  {
	  itsRadonDB = std::unique_ptr<NFmiRadonDB> (NFmiRadonDBPool::Instance()->GetConnection());
  }   

}

	
void BDAPLoader::InitPool(const string& username, const string& password, const string& database)
{
  NFmiNeonsDBPool::Instance()->ReadWriteTransaction(true);
  NFmiNeonsDBPool::Instance()->Username(username);
  NFmiNeonsDBPool::Instance()->Password(password);
  NFmiNeonsDBPool::Instance()->Database(database);
  NFmiNeonsDBPool::Instance()->MaxWorkers(8);

  try
  {
    NFmiRadonDBPool::Instance()->Username(username);
    NFmiRadonDBPool::Instance()->Password(password);
    NFmiRadonDBPool::Instance()->Database("radon");
    NFmiRadonDBPool::Instance()->MaxWorkers(8);
	
    itsUseRadon = true;
  }
  catch (int e) {
    // nada
  }

  Init();
}

BDAPLoader::~BDAPLoader() 
{
  if (itsNeonsDB)
  {
    NFmiNeonsDBPool::Instance()->Release(itsNeonsDB.get());
	itsNeonsDB.release();
  }
  
  if (itsRadonDB)
  {
    NFmiRadonDBPool::Instance()->Release(itsRadonDB.get());
	itsRadonDB.release();
  }
}

string BDAPLoader::REFFileName(const fc_info &info) 
{

  // Determine file name

  if (!base)
  {
	if (!ReadREFEnvironment())
	{
      return "";
	}
  }

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
     << info.minute
     << "/"
     << info.fcst_per;

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
  }
  else if (info.ednum == 3)
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
       << setw(3)
       << setfill('0')
       << info.fcst_per;
  }

  ss << "." << info.filetype;
  
  return ss.str();

}

void BDAPLoader::Init() 
{

  itsGeomName = "";
  itsModelType = "";
  itsDsetId = "";
  itsTableName = "";

  char* host;
  
  if ((host = getenv("HOSTNAME")) == NULL)
  {
    // env variable hostname not set
    host = new char[100];
    gethostname(host, 100);
    itsHostname = string(host);
    delete [] host;
  }
  else
  {
    itsHostname = string(host);
  }
}

bool BDAPLoader::WriteAS(const fc_info &info) 
{

  // Clear cache
  Init();

  stringstream query;

  vector<string> row;

  if (itsGeomName.empty()) 
  {
    auto geominfo = itsNeonsDB->GetGeometryDefinition(info.ni, info.nj, info.lat, info.lon, info.di, info.dj);
	  
    if (geominfo.empty()) 
    {
      cerr << "Geometry not found" << endl;
      return false;
    }

    itsGeomName = geominfo["geom_name"];

  }

  if (itsModelType.empty()) 
  {

    auto dsetinfo = itsNeonsDB->GetGridDatasetInfo(info.centre, info.process, itsGeomName, info.base_date);
	
    if (dsetinfo.empty()) 
    {
      cerr << "Model definition not found" << endl;
      return false;
    }

    itsDsetId = dsetinfo["dset_id"];
	itsTableName = dsetinfo["table_name"];

  }

  if (itsDsetId.empty() || itsTableName.empty()) 
  {

    query << "SELECT "
          << "dset_id, "
          << "table_name "
          << "FROM as_grid "
          << "WHERE "
          << "model_type = '" << itsModelType << "'"
          << " AND geom_name = '" << itsGeomName << "'"
          << " AND dset_name = 'AF'"
          << " AND base_date = '" << info.base_date << "'";

    itsNeonsDB->Query(query.str());

    if (options.dry_run)
      cout << query.str() << endl;

    row = itsNeonsDB->FetchRow();

    if (row.empty()) 
    {
      cerr << "Data set definition not found from NEONS table 'as_grid' for geometry '" << itsGeomName << "', base_date " << info.base_date << endl;
      cerr << "The data could be too old" << endl;
      return false;
    }

    itsDsetId = row[0];
    itsTableName = row[1];

    query.str("");

  }

  query << "UPDATE as_grid "
        << "SET rec_cnt_dset = "
        << "rec_cnt_dset + 1, "
        << "date_maj_dset = sysdate "
        << "WHERE dset_id = " << itsDsetId;

  if (options.dry_run)
    cout << query.str() << endl;

  try 
  {
    if (!options.dry_run)
      itsNeonsDB->Execute(query.str());
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
        << "'" << itsHostname << "')";

  if (options.dry_run)
    cout << query.str() << endl;

  try 
  {
    if (!options.dry_run)
      itsNeonsDB->Execute(query.str());
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
            << " AND file_server = '" << itsHostname << "'";

      if (options.dry_run)
        cout << query.str() << endl;

      try 
      {
        if (!options.dry_run)
          itsNeonsDB->Execute(query.str());
      } 
      catch (int ee) 
      {
        // Give up
        itsNeonsDB->Rollback();
        return false;
      }
    }
    else 
    {
      itsNeonsDB->Rollback();
      cerr << "Load failed with: " << itsTableName << "," << info.parname << "," << info.levname
                       << info.lvl1_lvl2 << "," << info.fcst_per << "," << info.filename << endl;
      return false;
    }
  }

  if (options.dry_run)
    itsNeonsDB->Rollback();
  else
    itsNeonsDB->Commit();

  return true;

}

bool BDAPLoader::WriteToRadon(const fc_info &info)
{

  // Clear cache

  if (!itsUseRadon)
  {
    return false;
  }

  if (options.verbose)
    cout << "Writing to radon" << endl;

  Init();

  stringstream query;
  vector<string> row;

  map<string,string> r =itsRadonDB->GetProducerFromGrib(info.centre, info.process);

  if (r.size() == 0)
  {
    cerr << "Producer information not found for centre " << info.centre << ", process " << info.process << endl;
    return false;
  }

  long geometry_id = 0;
  string geometry_name = "";

  long producer_id = boost::lexical_cast<long> (r["id"]);

  if (geometry_id == 0)
  {

    query << "SELECT g.id,g.name FROM geom g, projection p "
            << "WHERE g.projection_id = p.id"
            << " AND nj = " << info.nj
            << " AND ni = " << info.ni
            << " AND 1000 * st_x(first_point) = " << info.lon
            << " AND 1000 * st_y(first_point) = " << info.lat
            << " AND 1000 * di = " << info.di
            << " AND 1000 * dj = " << info.dj
            << " AND p." << (info.ednum == 1 ? "grib1_number = " : "grib2_number = ") << info.gridtype;

    if (options.dry_run)
      cout << query.str() << endl;

   itsRadonDB->Query(query.str());

    row = itsRadonDB->FetchRow();

    if (row.empty())
    {
      cerr << "Geometry not found" << endl;
      return false;
    }

    geometry_id = boost::lexical_cast<long> (row[0]);
    geometry_name = row[1];

    query.str("");

  }

  map<string,string> l = itsRadonDB->GetLevelFromGrib(producer_id, info.levtype, info.ednum);

  if (l.empty())
  {
    cerr << "Level " << info.levtype << " not found from radon for producer " << producer_id << "\n";
    return false;
  } 

  long level_id = boost::lexical_cast<long> (l["id"]);

  long param_id = 0;

  if (param_id == 0)
  {
    map<string,string> p;

    if (info.ednum == 1)
    {
      p = itsRadonDB->GetParameterFromGrib1(producer_id, info.novers, info.param, info.timeRangeIndicator, boost::lexical_cast<long> (l["id"]), info.lvl1_lvl2);
	  
      if (p.empty())
      {
        cerr << "Parameter not found from radon\n";
        cerr << "Table version: " << info.novers << " param " << info.param << " tri " << info.timeRangeIndicator << " level " << info.levtype << "/" << info.lvl1_lvl2 << endl;
        return false;
      }
    }
    else if (info.ednum == 2)
    {
      p = itsRadonDB->GetParameterFromGrib2(producer_id, info.discipline, info.category, info.param, boost::lexical_cast<long> (l["id"]), info.lvl1_lvl2);

      if (p.empty())
      {
        cerr << "Parameter not found from radon\n";
        cerr << "Discipline: " << info.discipline << " category " << info.category << " param " << info.param << " level " << info.levtype << "/" << info.lvl1_lvl2 << endl;
        return false;
      }
    }
    else if (info.ednum == 3)
    {
      p = itsRadonDB->GetParameterFromNetCDF(producer_id, info.ncname, boost::lexical_cast<long> (l["id"]), info.lvl1_lvl2);

      if (p.empty())
      {
        cerr << "Parameter not found from radon\n";
        cerr << "NetCDF name: " << info.ncname << endl;
	return false;
      }
    }
    param_id = boost::lexical_cast<long> (p["id"]);
  }

  string tableName = "", schema = "";

  if (tableName.empty())
  {

    query << "SELECT "
          << "schema_name, table_name "
          << "FROM as_grid "
          << "WHERE "
          << "producer_id = " << producer_id
          << " AND geometry_id = " << geometry_id
          << " AND analysis_time = to_timestamp('" << info.base_date << "', 'yyyymmddhh24mi')";

    itsRadonDB->Query(query.str());

    if (options.dry_run)
      cout << query.str() << endl;

    row = itsRadonDB->FetchRow();

    if (row.empty())
    {
      cerr << "Data set definition not found from radon table 'as_grid' for geometry '" << geometry_name << "', base_date " << info.base_date << endl;
      cerr << "The data could be too old" << endl;
      return false;
    }

    schema = row[0];
    tableName = row[1];

    query.str("");

  }

  query.str("");

  string interval = "";

  switch (info.timeUnit)
  {
  case 0:
  case 13:
  case 14:
      interval = " * interval '1 minute'";
      break;
  default:
      interval = " * interval '1 hour'";
      break;
  }
  
  query << "INSERT INTO " << schema << "." << tableName
        << " (producer_id, analysis_time, geometry_id, param_id, level_id, level_value, forecast_period, forecast_type_id, file_location, file_server, forecast_type_value) "
        << "VALUES ("
        << producer_id << ", "
        << "to_timestamp('" << info.base_date << "', 'yyyymmddhh24miss'), "
        << geometry_id << ", "
        << param_id << ", "
        << level_id << ", "
        << info.lvl1_lvl2 << ", "
        << info.fcst_per << interval << ", "
        << info.forecast_type_id << ", "
        << "'" << info.filename << "', "
        << "'" << itsHostname << "', "
        << (info.forecast_type_value == kFloatMissing ? "-1" : boost::lexical_cast<string> (info.forecast_type_value)) << ")";

  if (options.dry_run)
    cout << query.str() << endl;

  try
  {
    if (!options.dry_run && itsUseRadon)
      itsRadonDB->Execute(query.str());
  }
  catch (int e)
  {
    // http://www.postgresql.org/docs/9.3/static/errcodes-appendix.html

    if (e == 23505)
    {
      // 23505     unique_violation

      query.str("");

      // PRIMARY KEY (producer_id, analysis_time, geometry_id, param_id, level_id, level_value, forecast_period, forecast_type_id)

      query << "UPDATE " << schema << "." << tableName
            << " SET file_location = '" << info.filename << "', "
            << " file_server = '" << itsHostname << "', "
            << " forecast_type_value = " << (info.forecast_type_value == kFloatMissing ? "NULL" : boost::lexical_cast<string> (info.forecast_type_value))
            << " WHERE "
            << " producer_id = " << producer_id
            << " AND analysis_time = to_timestamp('" << info.base_date << "', 'yyyymmddhh24miss')"
            << " AND geometry_id = " << geometry_id
            << " AND param_id = " << param_id
            << " AND level_id = " << level_id
            << " AND level_value = " << info.lvl1_lvl2
            << " AND forecast_period = " << info.fcst_per
            << " AND forecast_type_id = " << info.forecast_type_id
        ;

      if (options.dry_run)
        cout << query.str() << endl;

      try
      {
        if (!options.dry_run)
          itsRadonDB->Execute(query.str());
      }
      catch (int ee)
      {
        // Give up
        itsRadonDB->Rollback();
        return false;
      }
    }
    else
    {
      itsRadonDB->Rollback();
      cerr << "Load failed with: " << info.filename << endl;
      return false;
    }
  }

  if (options.dry_run)
    itsRadonDB->Rollback();
  else
    itsRadonDB->Commit();

  return true;

}

bool BDAPLoader::ReadREFEnvironment()
{

  if ((base = getenv("NEONS_REF_BASE")) == NULL)
  {
    cerr << "Environment variable 'NEONS_REF_BASE' not set" << endl;
    return false;
  }

  return true;
}

NFmiNeonsDB& BDAPLoader::NeonsDB() const
{
  return *itsNeonsDB;
}
