#include "CurlEasyPtr.h"
#include <fstream>
#include <sstream>
#include <curl/curl.h>

CurlGlobalSetup::CurlGlobalSetup()
// Constructor
{
   curl_global_init(CURL_GLOBAL_ALL);
}

CurlGlobalSetup::~CurlGlobalSetup()
// Destructor
{
   curl_global_cleanup();
}

CurlEasyPtr CurlEasyPtr::easyInit()
// Start a libcurl easy session
{
   return CurlEasyPtr(curl_easy_init());
}

CurlEasyPtr::~CurlEasyPtr()
// Destructor
{
   if (ptr) curl_easy_cleanup(ptr);
}

void CurlEasyPtr::setUrl(std::string s)
// Set the URL to download
{
   url = std::move(s);
   curl_easy_setopt(ptr, CURLOPT_URL, url.c_str());
}

static void traceUrl(std::string_view url)
// Trace the downloaded URL (if enabled via environment variable)
{
   static const char* tracePath = [] { return ::getenv("TRACE"); }();
   if (!tracePath)
      return;

   auto s = std::ofstream(tracePath, std::ios::app);
   s << url << std::endl;
}

std::stringstream CurlEasyPtr::performToStringStream()
// Download the specified URL to an in-memory stringstream
{
   std::stringstream responseData;
   curl_easy_setopt(ptr, CURLOPT_WRITEDATA, &responseData);
   size_t (*writeToString)(char*, size_t, size_t, void*) = [](char* contents, size_t size, size_t nmemb, void* userdata) -> size_t {
      auto& responseData = *reinterpret_cast<std::stringstream*>(userdata);
      responseData << std::string_view(contents, size * nmemb);
      return size * nmemb;
   };
   curl_easy_setopt(ptr, CURLOPT_WRITEFUNCTION, writeToString);
   traceUrl(url);
   auto res = curl_easy_perform(ptr);
   if (res != CURLE_OK)
      throw std::runtime_error(curl_easy_strerror(res));
   return responseData;
}
