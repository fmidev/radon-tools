#include "GribCreate.h"
#include <stdlib.h>
#include <sstream>
#include <vector>
#include "NFmiNeonsDB.h"

using namespace std;

GribCreate::GribCreate() 
  : itsUsername("neons_client") // bdm has server-based authentication
  , itsPassword("kikka8si")
  , itsDatabase("neons")
  , itsDryRun(false)
{
  char *dbName;

  if ((dbName = getenv("NEONS_DB")) != NULL)
    itsDatabase = static_cast<string> (dbName);

  NFmiNeonsDB::Instance().Connect(itsUsername, itsPassword, itsDatabase);

}

GribCreate::~GribCreate() {
}


bool GribCreate::Create(const string &theDate, const string &hour) {

	stringstream query;
	
  // Get tables to be created
	query << "SELECT table_type_id,"
      << " size_data_per_day, dset_per_day,"
      << " ts_data, ts_index,"
      << " prim_tbl_suffix ||'_'|| " << hour << " ||'_'|| " << theDate << ","
      << " geom_name,"
      << " grid_table_type.model_group,"
      << " retention FROM grid_table_type"
      << " WHERE tache = " << hour
      << " and valid ='Y'"
      << " and eps_type='N'"
      << " and not exists (SELECT null"
      << " FROM grid_table"
      << " WHERE table_name ="
      << " prim_tbl_suffix ||'_'|| " << hour << " ||'_'|| " << theDate << ")"
     	<< " ORDER BY size_data_per_day desc\n";

	if (DryRun())
      cout << query.str() << endl;

	NFmiNeonsDB::Instance().Query(query.str());
	vector<string> table_suffix;
  vector<string> ts_data;
  vector<string> ts_index;
  vector<string> type_id;
  vector<string> geom_name;
  vector<string> model_group;
  vector<string> retention;

	while (true) 
  {
		vector<string> row = NFmiNeonsDB::Instance().FetchRow();

    if (row.empty())
    	break;

    type_id.push_back(row[0]);
    ts_data.push_back(row[3]);
    ts_index.push_back(row[4]);
    table_suffix.push_back(row[5]);
    geom_name.push_back(row[6]);
    model_group.push_back(row[7]);
    retention.push_back(row[8]);
	}

  if (table_suffix.empty())
  {
    cerr << "No tables available for creation.\n";
    return false;
  }

  for(unsigned int i=0; i < table_suffix.size(); i++) 
  { 
      query << "CREATE TABLE " << table_suffix[i]
      	<< " (dset_id number(10) not null,"
      	<< " parm_name varchar2(12) not null,"
      	<< " lvl_type varchar2(12) not null,"
      	<< " lvl1_lvl2 number(8) not null,"
      	<< " fcst_per number(6,2) not null,"
      	<< " eps_specifier varchar2(21),"
      	<< " file_location varchar2(256),"
       	<< " file_server varchar2(40),"
       	<< " CONSTRAINT pk_" << table_suffix[i] << ""
       	<< " PRIMARY KEY (dset_id,parm_name,lvl_type,lvl1_lvl2,fcst_per)"
        << " USING index tablespace " << ts_index[i] << ")"
        << " tablespace " << ts_data[i];
       cout << query.str() << "\n";
       query.str("");

   /* try 
      {
        if (!DryRun())
          NFmiNeonsDB::Instance().Execute(query.str());
      }   
      catch (int e) 
      {
        cerr << "Error code: " << e << endl;
      }	 */

      query << "CREATE public synonym " << table_suffix[i] << " for " << table_suffix[i]
            << " GRANT ALL on " << table_suffix[i] << " to adm_bdap "
            << " GRANT SELECT on " << table_suffix[i] << " to sel_bdap";
    /* try 
      {
        if (!DryRun())
          NFmiNeonsDB::Instance().Execute(query.str());
      }   
      catch (int e) 
      {
        cerr << "Error code: " << e << endl;
      }  */
      
      query.str("");
      query << "INSERT into grid_table"
          << " (table_type_id,table_name)"
          << " VALUES ("
          << type_id[i] << ", " << table_suffix[i] << ")";
      cout << query.str() << "\n";
      query.str("");

      map<string,string> mgroup;
      mgroup = GetModelType(model_group[i]);

      if (mgroup.empty())
      {
        cerr << "No model group found.\n";
        return false;
      }

      query << "INSERT into as_grid"
        << " (dset_id, table_name, model_type, geom_name,dset_name,"
        << " base_date,retention_dset,status_dset, date_maj_dset,"
        << " rec_cnt_dset,remarque,arch_vol_name, arch_id)"
        << " VALUES"
        << " (:dset_id, "<< table_suffix[i] <<"," << mgroup["model_type"]
        << ", "<< geom_name[i] << ",'AF',"
        << " to_date(" << theDate << "||to_char(" << mgroup["reseau"] 
        << ",'09')||'0000')," << retention[i] << ",0,sysdate,"
        << " 0, 'c est une remarque','?',0)";

      cout << query.str() << "\n";
      query.str("");
  }

/*	try 
	{
    	if (!DryRun())
      		NFmiNeonsDB::Instance().Execute(query.str());
  	} 	
  	catch (int e) 
  	{
    	cerr << "Error code: " << e << endl;
	}

	if (DryRun())
		NFmiNeonsDB::Instance().Rollback();
	else
		NFmiNeonsDB::Instance().Commit();

*/
	return true;
}

map<string, string> GribCreate::GetModelType(const string &theGroup) 
{
  stringstream query;
  map<string, string> type;
    
  query << "SELECT model_type,reseau"
    << " FROM grid_model_group"
    << " WHERE model_group = '" << theGroup <<"'";

  NFmiNeonsDB::Instance().Query(query.str());

  vector<string> row = NFmiNeonsDB::Instance().FetchRow();
  
  if (row.empty())
      return type;

  type["model_type"]= row[0];
  type["reseau"]= row[1];

  return type;
}

void GribCreate::DryRun(bool theDryRun) 
{
  itsDryRun = theDryRun;
}

bool GribCreate::DryRun() 
{
  return itsDryRun;
}
