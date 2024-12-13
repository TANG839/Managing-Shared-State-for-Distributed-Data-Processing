#include "CurlEasyPtr.h"
#include <algorithm>
#include <array>
#include <charconv>
#include <cstddef>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <vector>
#include <netdb.h>
#include <pthread.h>
#include <sys/socket.h>

#if 1
#define LOG(str) std::cerr << str << std::endl;
#endif

using UrlCountResult = std::vector<std::vector<std::pair<std::string, unsigned>>>;

UrlCountResult processUrl(CurlEasyPtr& curl, std::string_view url, unsigned partitionCount) {
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
               auto afterProtocol = column.substr(pos + 3);
               auto endDomain = afterProtocol.find('/');
               std::string url = std::string(afterProtocol.substr(0, endDomain));
               urlCounts[url]++;

            } else {
               urlCounts[column]++;
            }
            break;
         }
      }
   }

   UrlCountResult result;
   result.resize(partitionCount);

   for (auto& entry : urlCounts) {
      size_t hash = std::hash<std::string>{}(entry.first);
      result[hash % partitionCount].push_back(entry);
   }

   // debug
   return result;
}

void sendToBlobStore(std::string_view msg, std::string filename) {
   std::ofstream os("mock_blob_store/" + filename);
   os << msg;
   os.close();
}

std::vector<std::pair<std::string, unsigned>> mergeBlobsWithId(unsigned id) {
   std::string idStr = std::to_string(id);
   std::unordered_map<std::string, unsigned> urlCounts;

   for (const auto& entry : std::filesystem::directory_iterator("mock_blob_store")) {
      std::string filename = entry.path().filename().string();

      if (filename.starts_with("subpartition") && filename.ends_with(idStr)) {
         std::stringstream ss;
         std::ifstream is(entry.path());

         if (is.is_open()) {
            ss << is.rdbuf();
            is.close();

            std::string url;
            unsigned count;
            std::string line;
            while (getline(ss, line)) {
               size_t tab = line.find('\t');
               url = line.substr(0, tab);
               count = static_cast<unsigned>(std::atoi(line.substr(tab + 1, line.size()).c_str()));
               urlCounts[url] += count;
            }

         } else {
            std::cerr << "Failed to open file: " << filename << std::endl;
         }
      }
   }

   std::vector<std::pair<std::string, unsigned>> result(urlCounts.begin(), urlCounts.end());
   std::partial_sort(result.begin(), result.begin() + (long)std::min(result.size(), 25ul), result.end(), 
      [](auto& a, auto& b) { return a.second > b.second; });
   
   // debug
   return result;
}

enum Command {
   INIT = 0,
   DOWNLOAD,
   MERGE
};

/// Worker process that receives a list of URLs and reports the result
/// Example:
///    ./worker localhost 4242
/// The worker then contacts the leader process on "localhost" port "4242" for work
int main(int argc, char* argv[]) {
   if (argc != 3) {
      std::cerr << "Usage: " << argv[0] << " <host> <port>" << std::endl;
      return 1;
   }

   for (const auto& entry : std::filesystem::directory_iterator("./mock_blob_store")) {
      std::filesystem::remove(entry);
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
   unsigned partitionCount = 0;
   std::string workerIdStr;
      std::cerr << "TEST " << std::endl;
      std::cerr << "TEST " << std::endl;
      std::cerr << "TEST " << std::endl;

   while (true) {
      auto numBytes = recv(connection, buffer.data(), buffer.size() - 1, 0);

      if (numBytes <= 0) {
         // connection closed / error
         break;
      }

      buffer[static_cast<size_t>(numBytes)] = 0;
      std::cerr <<  buffer.data() + 1 << std::endl;

      Command cmd = (Command) buffer[0];

      switch (cmd) {
         case INIT: { // request: <partitioncount> <workerID>
            if (numBytes < 0) {
               perror("Could not recv partition count");
               exit(EXIT_FAILURE);
            }

            buffer[buffer.size() - 1] = 0; // terminate string
            std::stringstream ss;
            ss << std::string_view(buffer.data() + 1);

            if (!(ss >> partitionCount >> workerIdStr)) {
               std::cerr << "Invalid init request" << std::endl;
               exit(EXIT_FAILURE);
            }

            break;
         }

         case DOWNLOAD: { // request: <url>
            if (partitionCount == 0) {
               std::cerr << "Invalid partition count" << std::endl;
               exit(EXIT_FAILURE);
            }
            
            auto sep = std::find(buffer.begin() + 1, buffer.end(), ' ');
            unsigned downloadId = static_cast<unsigned>(std::atoi(buffer.data() + 1));
   
            std::string_view url = std::string_view(sep + 1, static_cast<size_t>(numBytes));
            UrlCountResult result = processUrl(curl, url, partitionCount);

            if (result.size() != partitionCount) {
               std::cerr << "Invalid partition of urls\n";
               exit(EXIT_FAILURE);
            }

            for (unsigned i = 0; i < partitionCount; i++) {
               std::stringstream response_stream;
               for (const auto& e : result[i]) {
                  response_stream << e.first << '\t' << e.second << "\n";
               }

               std::string result_string = response_stream.str();
               if (!result_string.empty())
                  sendToBlobStore(result_string, "subpartition_" + std::to_string(downloadId) + "_" + std::to_string(i + 1));
            }

            break;
         }

         case MERGE: { // request: <partition id>
            buffer[12] = 0; // terminate string
            unsigned partitionId = static_cast<unsigned>(std::atoi(buffer.data() + 1));
            if (partitionId == 0) {
               std::cerr << "Invalid partition id" << std::endl;
               exit(EXIT_FAILURE);
            }

            auto result = mergeBlobsWithId(partitionId);
            std::stringstream response_stream;
            for (const auto& e : result) {
               response_stream << e.first << " " << e.second << "\n";
            }

            std::string result_string = response_stream.str();
            if (!result_string.empty())
               sendToBlobStore(result_string, "sorted_partition_" + std::to_string(partitionId));
            LOG("Worke: " << std::to_string(partitionId));
            break;
         }
         default: {
            std::cerr << "Invalid command: " << cmd << std::endl;
            exit(EXIT_FAILURE);
         }
      }

      std::string response = "Success\n";
      if (send(connection, response.c_str(), response.size(), 0) == -1) {
         perror("send() failed");
      }
   }
   
   LOG("Terminate worker");
   close(connection);
   return 0;
}
