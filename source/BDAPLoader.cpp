#include "BDAPLoader.h"
#include "options.h"
#include <boost/algorithm/string.hpp>
#include <boost/lexical_cast.hpp>
#include <iomanip>
#include <iostream>
#include <pqxx/except.hxx>
#include <sstream>

using namespace std;

extern Options options;
once_flag oflag;

BDAPLoader::BDAPLoader()
    : itsUsername("wetodb"), itsDatabase("radon"), itsDatabaseHost("vorlon"), base(0), itsNeedsAnalyze(false)
{
	char myhost[255];
	gethostname(myhost, 100);
	itsHostname = string(myhost);

	const auto pw = getenv("RADON_WETODB_PASSWORD");

	if (pw)
	{
		itsPassword = string(pw);
	}
	else
	{
		throw runtime_error("Password must be specified with environment variable 'RADON_WETODB_PASSWORD'");
	}

	const auto host = getenv("RADON_HOSTNAME");

	if (host)
	{
		itsDatabaseHost = string(host);
	}

	const auto database = getenv("RADON_DATABASENAME");

	if (database)
	{
		itsDatabase = string(database);
	}

	call_once(oflag, [&]() {
		NFmiRadonDBPool::Instance()->Username(itsUsername);
		NFmiRadonDBPool::Instance()->Password(itsPassword);
		NFmiRadonDBPool::Instance()->Database(itsDatabase);
		NFmiRadonDBPool::Instance()->Hostname(itsDatabaseHost);
		NFmiRadonDBPool::Instance()->MaxWorkers(10);

		cout << "Connected to radon (db=" + itsDatabase + ", host=" + itsDatabaseHost + ")" << endl;
	});

	itsRadonDB = std::unique_ptr<NFmiRadonDB>(NFmiRadonDBPool::Instance()->GetConnection());
}

BDAPLoader::~BDAPLoader()
{
	if (itsRadonDB)
	{
		NFmiRadonDBPool::Instance()->Release(itsRadonDB.get());
		itsRadonDB.release();
	}
}

string BDAPLoader::REFFileName(const fc_info& info)
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

	ss << base << "/" << info.centre << "_" << info.process << "/" << info.year << setw(2) << setfill('0') << info.month
	   << setw(2) << setfill('0') << info.day << setw(2) << setfill('0') << info.hour << setw(2) << setfill('0')
	   << info.minute << "/" << info.geom_name << "/" << info.fcst_per << "/" << info.parname << "_"
	   << boost::algorithm::to_lower_copy(info.levname) << "_" << info.level1 << "_" << info.grtyp << "_" << info.ni
	   << "_" << info.nj << "_0_" << setw(3) << setfill('0') << info.fcst_per;

	if (info.forecast_type_id > 2)
	{
		ss << "_" << info.forecast_type_id << "_" << info.forecast_type_value;
	}

	if (info.ednum == 1)
	{
		ss << ".grib";
	}
	else if (info.ednum == 2)
	{
		ss << ".grib2";
	}
	else
	{
		ss << ".nc";
	}

	return ss.str();
}

bool BDAPLoader::GetGeometryInformation(fc_info& info)
{
	double di = info.di_degrees;
	double dj = info.dj_degrees;

	// METNO Analysis in NetCDF has d{i,j}_degrees.
	if ((info.grtyp == "polster" || info.grtyp == "lcc") && info.ednum != 3)
	{
		di = info.di_meters;
		dj = info.dj_meters;
	}

	auto geominfo = itsRadonDB->GetGeometryDefinition(info.ni, info.nj, info.lat_degrees, info.lon_degrees, di, dj,
	                                                  (info.ednum == 3 ? 1 : static_cast<int>(info.ednum)),
	                                                  static_cast<int>(info.gridtype));

	if (geominfo.empty())
	{
		cerr << "Geometry not found from radon: " << info.ni << " " << info.nj << " " << info.lat_degrees << " "
		     << info.lon_degrees << " " << di << " " << dj << " " << info.gridtype << endl;
		return false;
	}

	info.geom_id = stol(geominfo["id"]);
	info.geom_name = geominfo["name"];

	return true;
}

bool BDAPLoader::WriteToRadon(const fc_info& info)
{
	stringstream query;
	vector<string> row;

	long producer_id = info.process;
	long class_id = 1;  // grid data, forecast or observation
	long type_id = 1;   // deterministic forecast

	if (info.forecast_type_id == 2)
	{
		type_id = 2;  // ANALYSIS
	}
	else if (info.forecast_type_id == 3 || info.forecast_type_id == 4)
	{
		type_id = 3;  // ENSEMBLE
	}

	map<string, string> prodInfo;

	if (info.ednum != 3)
	{
		prodInfo = itsRadonDB->GetProducerFromGrib(info.centre, info.process, type_id);

		if (prodInfo.size() == 0)
		{
			cerr << "Producer information not found from radon for centre " << info.centre << ", process "
			     << info.process << " producer type " << type_id << endl;
			return false;
		}

		producer_id = boost::lexical_cast<long>(prodInfo["id"]);
		class_id = boost::lexical_cast<long>(prodInfo["class_id"]);
	}

	if (class_id != 1)
	{
		cerr << "producer class_id is " << class_id << ", grid_to_neons can only handle gridded data (class_id = 1)"
		     << endl;
		return false;
	}

	long geometry_id = info.geom_id;
	string geometry_name = info.geom_name;

	map<string, string> l = itsRadonDB->GetLevelFromGrib(producer_id, info.levtype, info.ednum);

	if (l.empty())
	{
		cerr << "Level " << info.levtype << " not found from radon for producer " << producer_id << "\n";
		return false;
	}

	long level_id = boost::lexical_cast<long>(l["id"]);

	long param_id = 0;

	if (param_id == 0)
	{
		map<string, string> p;

		if (info.ednum == 1)
		{
			p = itsRadonDB->GetParameterFromGrib1(producer_id, info.novers, info.param, info.timeRangeIndicator,
			                                      info.levtype, static_cast<double>(info.lvl1_lvl2));

			if (p.empty())
			{
				cerr << "Parameter not found from radon\n";
				cerr << "Table version: " << info.novers << " param " << info.param << " tri "
				     << info.timeRangeIndicator << " level " << info.levtype << "/" << info.lvl1_lvl2 << endl;
				return false;
			}
		}
		else if (info.ednum == 2)
		{
			p = itsRadonDB->GetParameterFromGrib2(producer_id, info.discipline, info.category, info.param, info.levtype,
			                                      static_cast<double>(info.lvl1_lvl2));

			if (p.empty())
			{
				cerr << "Parameter not found from radon\n";
				cerr << "Discipline: " << info.discipline << " category " << info.category << " param " << info.param
				     << " level " << info.levtype << "/" << info.lvl1_lvl2 << endl;
				return false;
			}
		}
		else if (info.ednum == 3)
		{
			p = itsRadonDB->GetParameterFromNetCDF(producer_id, info.ncname, level_id,
			                                       static_cast<double>(info.lvl1_lvl2));

			if (p.empty())
			{
				cerr << "Parameter not found from radon\n";
				cerr << "NetCDF name: " << info.ncname << endl;
				return false;
			}
		}
		param_id = boost::lexical_cast<long>(p["id"]);
	}

	auto tableinfo = itsRadonDB->GetTableName(producer_id, info.base_date, geometry_name);

	if (tableinfo.empty())
	{
		cerr << "Destination table definition not found from radon table "
		        "'as_grid' for geometry '"
		     << geometry_name << "', base_date " << info.base_date << endl
		     << "The data could be too old" << endl;
		return false;
	}

	if (tableinfo["record_count"] == "0")
	{
		itsNeedsAnalyze = true;
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

	string forecastTypeValue =
	    (info.forecast_type_value == kFloatMissing ? "-1" : boost::lexical_cast<string>(info.forecast_type_value));

	itsLastInsertedTable = tableinfo["schema_name"] + "." + tableinfo["partition_name"];
	itsLastSSStateInformation = to_string(producer_id) + "/" + to_string(geometry_id) + "/" + info.base_date + "/" +
	                            to_string(info.fcst_per) + interval + "/" + to_string(info.forecast_type_id) + "/" +
	                            forecastTypeValue + "/" + tableinfo["schema_name"] + "." + tableinfo["partition_name"];

	query << "INSERT INTO " << tableinfo["schema_name"] << "." << tableinfo["partition_name"]
	      << " (producer_id, analysis_time, geometry_id, param_id, level_id, "
	         "level_value, level_value2, forecast_period, "
	         "forecast_type_id, file_location, file_server, forecast_type_value) "
	      << "VALUES (" << producer_id << ", '" << info.base_date << "', " << geometry_id << ", " << param_id << ", "
	      << level_id << ", " << info.level1 << ", " << info.level2 << ", " << info.fcst_per << interval << ", "
	      << info.forecast_type_id << ", "
	      << "'" << info.filename << "', "
	      << "'" << itsHostname << "', " << forecastTypeValue << ")";

	try
	{
		if (options.dry_run)
		{
			cout << query.str() << endl;
		}
		else
		{
			itsRadonDB->Execute(query.str());
		}
	}
	catch (const pqxx::unique_violation& e)
	{
		query.str("");

		// PRIMARY KEY (producer_id, analysis_time, geometry_id, param_id, level_id,
		// level_value, forecast_period,
		// forecast_type_id)

		query << "UPDATE " << tableinfo["schema_name"] << "." << tableinfo["partition_name"] << " SET file_location = '"
		      << info.filename << "', "
		      << " file_server = '" << itsHostname << "', "
		      << " forecast_type_value = " << forecastTypeValue << " WHERE"
		      << " producer_id = " << producer_id << " AND analysis_time = '" << info.base_date << "'"
		      << " AND geometry_id = " << geometry_id << " AND param_id = " << param_id
		      << " AND level_id = " << level_id << " AND level_value = " << info.level1
		      << " AND level_value2 = " << info.level2 << " AND forecast_period = interval '1 hour' * " << info.fcst_per
		      << " AND forecast_type_id = " << info.forecast_type_id
		      << " AND forecast_type_value = " << info.forecast_type_value;

		try
		{
			itsRadonDB->Execute(query.str());
		}
		catch (const pqxx::pqxx_exception& ee)
		{
			cerr << "Failed: " << ee.base().what() << endl;
			// Give up
			itsRadonDB->Rollback();
			return false;
		}
	}
	catch (const pqxx::pqxx_exception& e)
	{
		itsRadonDB->Rollback();
		cerr << "Load to radon failed with: " << info.filename << ": " << e.base().what() << endl;
		return false;
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
		if ((base = getenv("RADON_REF_BASE")) == NULL)
		{
			cerr << "Environment variable 'NEONS_REF_BASE' or 'RADON_REF_BASE' not set" << endl;
			return false;
		}
	}

	return true;
}

NFmiRadonDB& BDAPLoader::RadonDB() const
{
	return *itsRadonDB;
}
bool BDAPLoader::NeedsAnalyze() const
{
	return itsNeedsAnalyze;
}
string BDAPLoader::LastInsertedTable() const
{
	return itsLastInsertedTable;
}
string BDAPLoader::LastSSStateInformation() const
{
	return itsLastSSStateInformation;
}
