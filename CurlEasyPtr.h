#ifndef CBDP_CURLEASYPTR_H
#define CBDP_CURLEASYPTR_H

#include <string>

using CURL = void;

/// Global libcurl initialization. Keep around as long as using it
class CurlGlobalSetup {
   public:
   /// Constructor
   CurlGlobalSetup();
   /// Destructor
   ~CurlGlobalSetup();

   CurlGlobalSetup(const CurlGlobalSetup&) = delete;
   CurlGlobalSetup& operator=(const CurlGlobalSetup&) = delete;
};

/// A wrapper around the curl_easy interface
class CurlEasyPtr {
   private:
   /// The curl handle
   CURL* ptr = nullptr;
   /// The URL to download
   std::string url;
   /// Constructor. Use easyInit() instead
   explicit CurlEasyPtr(CURL* ptr) : ptr(ptr) {}

   public:
   /// Start a libcurl easy session
   static CurlEasyPtr easyInit();
   /// Destructor
   ~CurlEasyPtr();
   CurlEasyPtr(const CurlEasyPtr&) = delete;
   CurlEasyPtr& operator=(const CurlEasyPtr&) = delete;

   /// Set the URL to download
   void setUrl(std::string s);
   /// Download the specified URL to an in-memory stringstream
   std::stringstream performToStringStream();
};

#endif
