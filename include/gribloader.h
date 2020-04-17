#pragma once

#include "NFmiGrib.h"
#include "options.h"
#include <atomic>
#include <boost/thread.hpp>
#include <mutex>
#include <string>

namespace grid_to_radon
{
class GribLoader
{
   public:
	GribLoader();
	virtual ~GribLoader() = default;

	bool Load(const std::string& theInfile);

   protected:
	void Run(short threadId);
	bool DistributeMessages(NFmiGribMessage& newMessage, unsigned int& messageNo);
	void Process(NFmiGribMessage& message, short threadId, unsigned int messageNo);

	NFmiGrib itsReader;

	std::vector<std::string> parameters;
	std::vector<std::string> levels;

	std::atomic<int> g_success;
	std::atomic<int> g_skipped;
	std::atomic<int> g_failed;

	std::mutex distMutex;

	std::set<std::string> ssStateInformation;
	std::string itsHostName;
	std::string itsInputFileName;
};
}
