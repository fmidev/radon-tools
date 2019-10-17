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

	ss << base << "/" << info.producer_id << "/" << info.year << setw(2) << setfill('0') << info.month << setw(2)
	   << setfill('0') << info.day << setw(2) << setfill('0') << info.hour << setw(2) << setfill('0') << info.minute
	   << "/" << info.geom_name << "/" << info.fcst_per << "/" << info.parname << "_"
	   << boost::algorithm::to_lower_copy(info.levname) << "_" << info.level1 << "_" << info.projection << "_"
	   << info.ni << "_" << info.nj << "_0_" << setw(3) << setfill('0') << info.fcst_per;

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

bool BDAPLoader::WriteToRadon(const fc_info& info)
{
	stringstream query;
	vector<string> row;

	const auto geometry_id = info.geom_id;
	const auto geometry_name = info.geom_name;

	const long level_id = info.levelid;
	const long param_id = info.paramid;

	query << info.year << "-" << setw(2) << setfill('0') << info.month << "-" << setw(2) << setfill('0') << info.day
	      << " " << setw(2) << setfill('0') << info.hour << ":" << setw(2) << setfill('0') << info.minute << ":00";

	const auto base_date = query.str();

	query.str("");

	auto tableinfo = itsRadonDB->GetTableName(info.producer_id, base_date, geometry_name);

	if (tableinfo.empty())
	{
		cerr << "Destination table definition not found from radon table "
		     << "'as_grid' for producer_id " << info.producer_id << ", geometry '" << geometry_name
		     << "', analysis_time " << base_date << endl
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

	const string forecastTypeValue =
	    (info.forecast_type_value == kFloatMissing ? "-1" : boost::lexical_cast<string>(info.forecast_type_value));

	itsLastInsertedTable = tableinfo["schema_name"] + "." + tableinfo["partition_name"];
	itsLastSSStateInformation = to_string(info.producer_id) + "/" + to_string(geometry_id) + "/" + base_date + "/" +
	                            to_string(info.fcst_per) + interval + "/" + to_string(info.forecast_type_id) + "/" +
	                            forecastTypeValue + "/" + tableinfo["schema_name"] + "." + tableinfo["table_name"];

	// clang-format off

	query << "INSERT INTO " << tableinfo["schema_name"] << "." << tableinfo["partition_name"]
	      << " (producer_id, analysis_time, geometry_id, param_id, level_id, "
	      << "level_value, level_value2, forecast_period, forecast_type_id,"
	      << "file_location, file_server, forecast_type_value, message_no, byte_offset, byte_length)"
	      << " VALUES ("
	      << info.producer_id << ", '"
	      << base_date << "', "
	      << geometry_id << ", "
	      << param_id << ", "
	      << level_id << ", "
	      << info.level1 << ", "
	      << info.level2 << ", "
	      << info.fcst_per
	      << interval << ", "
	      << info.forecast_type_id << ", "
	      << "'" << info.filename << "', "
	      << "'" << itsHostname << "', "
	      << forecastTypeValue;

	// clang-format on

	if (info.messageNo && info.offset && info.length)
	{
		query << ", " << info.messageNo.get() << ", " << info.offset.get() << ", " << info.length.get() << ")";
	}
	else
	{
		query << ", NULL, NULL, NULL)";
	}

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

		// clang-format off

		query << "UPDATE " << tableinfo["schema_name"] << "." << tableinfo["partition_name"]
		      << " SET file_location = '" << info.filename << "', "
		      << " file_server = '" << itsHostname << "', ";

		// clang-format on

		if (info.messageNo && info.offset && info.length)
		{
			query << "message_no = " << info.messageNo.get() << ", byte_offset = " << info.offset.get()
			      << ", byte_length = " << info.length.get();
		}
		else
		{
			query << "message_no = NULL, byte_offset = NULL, byte_length = NULL";
		}

		query << " WHERE"
		      << " producer_id = " << info.producer_id << " AND analysis_time = '" << base_date << "'"
		      << " AND geometry_id = " << geometry_id << " AND param_id = " << param_id
		      << " AND level_id = " << level_id << " AND level_value = " << info.level1
		      << " AND level_value2 = " << info.level2 << " AND forecast_period = interval '1 hour' * " << info.fcst_per
		      << " AND forecast_type_id = " << info.forecast_type_id
		      << " AND forecast_type_value = " << forecastTypeValue;

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
	if (!options.in_place_insert && (base = getenv("NEONS_REF_BASE")) == NULL)
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
