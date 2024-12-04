#include "CurlEasyPtr.h"
#include <algorithm>
#include <array>
#include <cstring>
#include <iostream>
#include <sstream>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <vector>
#include <netdb.h>
#include <sys/socket.h>

using UrlCountResult = std::vector<std::pair<std::string, unsigned>>;

 UrlCountResult processUrl(CurlEasyPtr& curl, std::string_view url) {
   using namespace std::literals;   
   std::unordered_map<std::string, unsigned> urlCounts;

   // Download the file
   curl.setUrl(std::string(url));
   auto csvData = curl.performToStringStream();
   for (std::string row; std::getline(csvData, row, '\n');) {
      auto rowStream = std::stringstream(std::move(row));

      // Check the URL in the second column
      unsigned columnIndex = 0;
      for (std::string column; std::getline(rowStream, column, '\t'); ++columnIndex) {
         // column 0 is id, 1 is URL
         if (columnIndex == 1) {
            // Check if URL is "google.ru"
            auto pos = column.find("://"sv);
            if (pos != std::string::npos) {
               urlCounts[column.substr(pos + 3)]++;
            }
            break;
         }
      }
   }

   UrlCountResult result;
   result.resize(std::min(25ul, urlCounts.size()));
   
   std::partial_sort_copy(urlCounts.begin(), urlCounts.end(), result.begin(), result.end(), 
      [](auto& a, auto& b) { return a.second > b.second; });

   // debug
   for (auto& e : result) std::cout << e.first << ", " << e.second << std::endl;

   return result;
}

/// Worker process that receives a list of URLs and reports the result
/// Example:
///    ./worker localhost 4242
/// The worker then contacts the leader process on "localhost" port "4242" for work
int main(int argc, char* argv[]) {
   if (argc != 3) {
      std::cerr << "Usage: " << argv[0] << " <host> <port>" << std::endl;
      return 1;
   }

   // Set up the connection
   addrinfo hints = {};
   hints.ai_family = AF_UNSPEC;
   hints.ai_socktype = SOCK_STREAM;
   addrinfo* coordinatorAddr = nullptr;
   if (auto status = getaddrinfo(argv[1], argv[2], &hints, &coordinatorAddr); status != 0) {
      std::cerr << "getaddrinfo() failed: " << gai_strerror(status) << std::endl;
      return 1;
   }

   // Try to connect to coordinator
   int connection, status;
   for (unsigned i = 0; i < 10; ++i) {
      for (auto iter = coordinatorAddr; iter; iter = iter->ai_next) {
         connection = socket(iter->ai_family, iter->ai_socktype, iter->ai_protocol);
         if (connection == -1) {
            std::cerr << "socket() failed: " << strerror(connection) << std::endl;
            return 1;
         }
         status = connect(connection, iter->ai_addr, iter->ai_addrlen);
         if (status != -1)
            goto breakConnect;
         close(connection);
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(500));
   }
breakConnect:
   freeaddrinfo(coordinatorAddr);
   if (status == -1) {
      perror("connect() failed");
      return 1;
   }

   // Connected
   auto curlSetup = CurlGlobalSetup();
   auto curl = CurlEasyPtr::easyInit();
   auto buffer = std::array<char, 1024>();
   while (true) {
      auto numBytes = recv(connection, buffer.data(), buffer.size(), 0);
      if (numBytes <= 0) {
         // connection closed / error
         break;
      }
      auto url = std::string_view(buffer.data(), static_cast<size_t>(numBytes));
      auto result = processUrl(curl, url);

      std::stringstream response_stream;
      for (const auto& e : result) {
         response_stream << e.first << " " << e.second << std::endl;
      }

      std::string response = response_stream.str();
      if (send(connection, response.c_str(), response.size(), 0) == -1) {
         perror("send() failed");
         break;
      }
   }

   close(connection);
   return 0;
}
