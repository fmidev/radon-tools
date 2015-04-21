// © FMI/KN 12.11.2007 epsfractiles.c Original 12.11.2007
// Ported to masala /partio

/*----------------------------------------------------------------------*/
/*    K   U   V   A   U   S                                             */
/*                                                                      */
/* Ohjelma hakee tedostosta ECMWFn EPS-ennustekentät yhden kerrallaan.  */
/* Kentälle annetaan uusi parametrinumero ja tuottajatunnus, jotta se   */
/* voidaan tallettaa reaaliaikatietokantaan. 				*/
/*                 							*/
/*      								*/
/* Komentorivilla annetaan sen tiedoston nimi, jossa eps-kentät ovat	*/
/*----------------------------------------------------------------------*/

#include <string>
#include <iostream>
#include <vector>
#include <boost/lexical_cast.hpp>

#include "NFmiGrib.h"

const double kFloatMissing = 32700.;
const int membersize = 51;

bool Process(const std::string& inFile, const std::string& outFile);
void Sort(std::vector<std::vector<double>>& fields, int membersize);
void Flip(std::vector<double>& fec, size_t ni, size_t nj);

using namespace std;

int main (int argc, char **argv)
{
	if (argc != 3)
	{
		cerr << "usage: epsfractiles file_in file_out\n";
		exit(1);
	}

	string inFile(argv[1]);
	string outFile(argv[2]);
	
	Process(inFile, outFile);
    	
	exit(0);
}

bool Process(const string& inFile, const string& outFile)
{
	NFmiGrib reader;
	
	vector<double> minim, val10, val25, median, val75, val90, maxim, first, allpoints;
 
	reader.Open(inFile);
	
	bool allocate = true;		
	int messageNum = -1;
		
	size_t sizedecoded = 0, ni = 0, nj = 0;
	
	long paramId = -1;

	vector<vector<double>> fields;
		
	while (reader.NextMessage())	
	{
		if (paramId == -1 )
		{
			paramId = reader.Message().ParameterNumber();
		}
		else if (paramId != reader.Message().ParameterNumber())
		{
			throw runtime_error("Parameter number changed in grib");
		}
		
		messageNum++;
		
		sizedecoded = reader.Message().SizeX() * reader.Message().SizeY();

		double* values = reader.Message().Values();
			
		if (allocate)
		{
			
		 	fields.resize(sizedecoded);
			
			for (size_t i = 0; i < sizedecoded; i++)
			{
				vector<double> field (membersize, kFloatMissing);
				fields[i] = field;
			}
			
			allocate = false;
			
			ni = reader.Message().SizeX();
			nj = reader.Message().SizeY();
		}
		
		for (size_t i = 0; i<sizedecoded; i++) 
		{  
			fields[i][messageNum] = values[i];
		}
	}
	
	cout << "Read " << messageNum+1 << " messages\n";
	
	assert(messageNum+1 == membersize);
	
	if (fields.empty())
	{
		throw runtime_error("No valid data found");
	}

	Sort(fields, membersize);
	
	minim.resize(sizedecoded, kFloatMissing);
	val10.resize(sizedecoded, kFloatMissing);
	val25.resize(sizedecoded, kFloatMissing);
	median.resize(sizedecoded, kFloatMissing);
	val75.resize(sizedecoded, kFloatMissing);
	val90.resize(sizedecoded, kFloatMissing);
	maxim.resize(sizedecoded, kFloatMissing);

	for (size_t i = 0; i < sizedecoded; i++)
	{
		minim[i] = fields[i][0];
		val10[i] = fields[i][4];
		val25[i] = fields[i][12];
		median[i] = fields[i][25];
		val75[i] = fields[i][37];
		val90[i] = fields[i][45];
		maxim[i] = fields[i][50];	
	}
	
	Flip(minim, ni, nj);
	Flip(val10, ni, nj);
	Flip(val25, ni, nj);
	Flip(median, ni, nj);
	Flip(val75, ni, nj);
	Flip(val90, ni, nj);
	Flip(maxim, ni, nj);

	assert(reader.Message().Edition() == 1);
 
	switch (paramId)
	{
		case 167:
			paramId = 173;
			break;
		case 228:
			paramId = 187;
			break;
		case 164:
			paramId = 214;
			break;
		case 49:
			paramId = 221;
			break;
		case 123:
			paramId = 221;
			break;
		case 21:
			paramId = 228;
			break;
		default:
			throw runtime_error("Unknown parameter: " +boost::lexical_cast<string> (paramId));
			break;
	}

	// piggybacking original grib when writing results

	reader.Open(inFile);
	reader.NextMessage();
	
	reader.Message().Table2Version(203);
	reader.Message().Process(240);
	reader.Message().Centre(86);

	reader.Message().JScansPositively(true);

	double Y0 = reader.Message().Y0();
	reader.Message().Y0(reader.Message().Y1());
	reader.Message().Y1(Y0);
	
	reader.Message().ParameterNumber(paramId);
	reader.Message().Values(&minim[0], sizedecoded);
	reader.Message().Write(outFile, true);
	
	reader.Message().ParameterNumber(++paramId);
	reader.Message().Values(&val10[0], sizedecoded);
	reader.Message().Write(outFile, true);

	reader.Message().ParameterNumber(++paramId);
	reader.Message().Values(&val25[0], sizedecoded);
 	reader.Message().Write(outFile, true);

 	reader.Message().ParameterNumber(++paramId);
	reader.Message().Values(&median[0], sizedecoded);
	reader.Message().Write(outFile, true);

	reader.Message().ParameterNumber(++paramId);
	reader.Message().Values(&val75[0], sizedecoded);
	reader.Message().Write(outFile, true);

	reader.Message().ParameterNumber(++paramId);
	reader.Message().Values(&val90[0], sizedecoded);
	reader.Message().Write(outFile, true);
		
 	reader.Message().ParameterNumber(++paramId);
	reader.Message().Values(&maxim[0], sizedecoded);
	reader.Message().Write(outFile, true);
	
	cout << "Wrote file '" << outFile << "'\n";
 
	return true;

}

void Sort(vector<vector<double>>& fields, int perturbationsize)
{
	for (size_t i = 0; i < fields.size(); i++)
	{
		sort(fields[i].begin(), fields[i].end());
	}
}

void Flip(vector<double>& vec, size_t ni, size_t nj)
{
	size_t halfSize = static_cast<size_t> (nj/2);

	for (size_t y = 0; y < halfSize; y++)
	{
		for (size_t x = 0; x < ni; x++)
		{
			size_t upperIndex = y * ni + x, lowerIndex = (nj-1-y) * ni + x;
			double upper = vec[upperIndex];
			double lower = vec[lowerIndex];

			vec[lowerIndex] = upper;
			vec[upperIndex] = lower;
		}
	}
}
