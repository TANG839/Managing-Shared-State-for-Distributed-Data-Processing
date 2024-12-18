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
#include <regex>
#include <sstream>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <vector>
#include <netdb.h>
#include <pthread.h>
#include <sys/socket.h>
#include <unistd.h>
#include "AzureBlobClient.h"
#include <iomanip>  // for std::setw


#if 1
#define LOG(str) std::cerr << str << std::endl;
#endif


using UrlCountResult = std::vector<std::vector<std::pair<std::string, unsigned>>>;

UrlCountResult processUrl(std::string_view url, unsigned partitionCount, AzureBlobClient& blobClient, const std::string& container_name) {
    using namespace std::literals;
    std::unordered_map<std::string, unsigned> urlCounts;

    // Helper function for domain extraction
    auto getDomain = [](std::string_view url) -> std::string_view {
        auto pos = url.find("://"sv);
        if (pos != std::string::npos) {
            auto afterProtocol = url.substr(pos + 3);
            auto endDomain = afterProtocol.find('/');
            return afterProtocol.substr(0, endDomain);
        }
        return url;
    };

    // Download and process the file
    std::cerr << "Downloading file: " << url << std::endl;
    auto csvData = blobClient.downloadStringStream(std::string(url), container_name);
    
    for (std::string row; std::getline(csvData, row, '\n');) {
        auto rowStream = std::stringstream(std::move(row));

        // Process second column (URL)
        unsigned columnIndex = 0;
        for (std::string column; std::getline(rowStream, column, '\t'); ++columnIndex) {
            if (columnIndex == 1) {
                // Extract and count domain
                auto domain = getDomain(column);
                urlCounts[std::string(domain)]++;
                break;
            }
        }
    }

    // Partition the results
    UrlCountResult result(partitionCount);
    for (const auto& [domain, count] : urlCounts) {
        size_t hash = std::hash<std::string>{}(domain);
        result[hash % partitionCount].emplace_back(domain, count);
    }

    return result;
}


void sendToBlobStore(std::string_view msg, std::string filename, AzureBlobClient& blobClient, const std::string& container_name) {
    std::stringstream ss;
    ss << msg;
    blobClient.uploadStringStream(filename, ss, container_name);
}
std::vector<std::pair<std::string, unsigned>> mergeBlobsWithId(unsigned id, AzureBlobClient& blobClient, const std::string& container_name) {
    std::unordered_map<std::string, unsigned> urlCounts;
    
    // List blobs with the pattern matching subpartition_{downloadId}_{id}
    auto blobList = blobClient.listBlobs(container_name);
    std::string pattern = "subpartition_" + std::to_string(id);
    
    for (const auto& blobName : blobList) {
        if (blobName.starts_with(pattern)) {
            auto content = blobClient.downloadStringStream(blobName, container_name);
            std::string line;
            while (getline(content, line)) {
                size_t tab = line.find('\t');
                if (tab != std::string::npos) {
                    std::string url = line.substr(0, tab);
                    unsigned count = static_cast<unsigned int>(std::stoul(line.substr(tab + 1)));
                    urlCounts[url] += count;
                }
            }
        }
    }
    
    std::vector<std::pair<std::string, unsigned>> result(urlCounts.begin(), urlCounts.end());
    std::partial_sort(result.begin(),  result.begin() + static_cast<long>(std::min(result.size(), 25ul)), result.end(),
        [](auto& a, auto& b) { return a.second > b.second; });
    
    return result;
}
enum Command {
   INIT = 0,
   DOWNLOAD,
   MERGE
};

int main(int argc, char* argv[]) {
   if (argc != 3) {
      std::cerr << "Usage: " << argv[0] << " <host> <port>" << std::endl;
      return 1;
   }

   static const std::string accountName = "csb10032003e7e1ee2c";
   //the token expires in an hour, it needs to be regnerated
   static const std::string accountToken = "eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsIng1dCI6InoxcnNZSEhKOS04bWdndDRIc1p1OEJLa0JQdyIsImtpZCI6InoxcnNZSEhKOS04bWdndDRIc1p1OEJLa0JQdyJ9.eyJhdWQiOiJodHRwczovL3N0b3JhZ2UuYXp1cmUuY29tLyIsImlzcyI6Imh0dHBzOi8vc3RzLndpbmRvd3MubmV0LzFjNmJhMjc0LTNjMzktNGE2MC1hMzZiLTM2N2IxNDc3MGM5My8iLCJpYXQiOjE3MzQ1MzQyNjMsIm5iZiI6MTczNDUzNDI2MywiZXhwIjoxNzM0NTM4MzM3LCJhY3IiOiIxIiwiYWlvIjoiQVlRQWUvOFlBQUFBRTRzUC9QTUx5dDM1aDkvNC92QkIzV081YnRqQ25hNFBjSWF5ZEZZSjRtajVkZFNCZ000MXA5S3d4ZFhkamx1MnJSc3pLeUw4enQ4M1JVMmpLVkdGSlNWbkhKSW5uQ3NVNmJ1cUxWVDlseVlxVERWbnVLaVduSFJIVXNJcEYrbFh3ODFhZm1jeU1PT29NZkI4czBWZjMyUzVWd2ZPLzNkdTNXMUNidmxLRFpJPSIsImFsdHNlY2lkIjoiMTpsaXZlLmNvbTowMDAzNDAwMjgwNzE1OEI2IiwiYW1yIjpbInB3ZCIsIm1mYSJdLCJhcHBpZCI6IjA0YjA3Nzk1LThkZGItNDYxYS1iYmVlLTAyZjllMWJmN2I0NiIsImFwcGlkYWNyIjoiMCIsImVtYWlsIjoibW9hYWQubWFhcm91ZmlAaWNsb3VkLmNvbSIsImZhbWlseV9uYW1lIjoiTWFyb3VmaSIsImdpdmVuX25hbWUiOiJNb2FkIiwiZ3JvdXBzIjpbIjY0MGYzNGQ3LThlNzctNDg2My04OWE2LTI1ZjY4YjEyNTMxOCJdLCJpZHAiOiJsaXZlLmNvbSIsImlkdHlwIjoidXNlciIsImlwYWRkciI6IjJhMDk6ODBjMDoxOTI6MDo1OGVlOjU5ZmM6OTM0YTozYTA5IiwibmFtZSI6Ik1vYWQgTWFyb3VmaSIsIm9pZCI6IjZiMDhiYWE0LWJlYzctNDhhOC05OGQ0LTIyYTIyNmNkYWEzZSIsInB1aWQiOiIxMDAzMjAwM0U3RTFFRTJDIiwicmgiOiIxLkFSTUJkS0pySERrOFlFcWphelo3RkhjTWs0R21CdVRVODZoQ2tMYkNzQ2xKZXZFVUFUSVRBUS4iLCJzY3AiOiJ1c2VyX2ltcGVyc29uYXRpb24iLCJzdWIiOiJKc3lwVWo3UkwxdnJmcndsYS1zYlVBNGxGOW9FSXkwUGdFUG5hUUZqWndJIiwidGlkIjoiMWM2YmEyNzQtM2MzOS00YTYwLWEzNmItMzY3YjE0NzcwYzkzIiwidW5pcXVlX25hbWUiOiJsaXZlLmNvbSNtb2FhZC5tYWFyb3VmaUBpY2xvdWQuY29tIiwidXRpIjoiTmNfbGh3OURZRXloYnNDWnFUWE5BQSIsInZlciI6IjEuMCIsInhtc19pZHJlbCI6IjEgMTYiLCJ4bXNfdGRiciI6IkVVIn0.D_mJVbFFIGJO8_BUXQfieUq12ilQlYUDmNJnE0nzVz_F4-zo3eIF3EhcjBWjOMvXO0lEzM3kW5uZ5q_dsVMfZJkS-e1c0kaDQd5CHdkrT3CDkb8Y9rGeszMfGsDDkttCzo0cvrYxi7bD4W9JDTqecdx6vmCBXDuVqS5bk0SMpdVv1GMXTPYhfXza2HTneOM0lMMN_v3gAwYIq2fZ0NFzxPsUlxNkKym9a8Ygd2bg8f2PPphmkdYU6mYdoHuPSMybnOB27pPo3WaVuXLgSjgIZ14coVceTgpK8ngVq-c3OPz2JLtqr7CfptiawSQ2QWmljm1WYNDNDbrzkNrX3qs9yQ";
   static const std::string container_name ="cbdp-files";
   //after each rerun manually remove the old cbdp-files-intermediate and create a new one
   // you would have to wait a little a bit to create one with the same name
   static const std::string container_name_intermediate ="cbdp-files-intermediate";
   auto blobClient = AzureBlobClient(accountName, accountToken,container_name);

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

   while (true) {
      auto numBytes = recv(connection, buffer.data(), buffer.size() - 1, 0);

      if (numBytes <= 0) {
         // connection closed / error
         break;
      }

      buffer[static_cast<size_t>(numBytes)] = 0;

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
        exit(EXIT_FAILURE);
    }
    
    // Debug buffer content
    for (size_t i = 0; i < std::min<size_t>(50, static_cast<size_t>(numBytes)); i++) {
        if (buffer[i] == '\n') std::cerr << "\\n";
        else if (buffer[i] == '\r') std::cerr << "\\r";
        else std::cerr << buffer[i];
    }
    
    auto sep = std::find(buffer.begin() + 1, buffer.end(), ' ');
    unsigned downloadId = static_cast<unsigned>(std::atoi(buffer.data() + 1));

    // Create string_view and check its content
    std::string_view url = std::string_view(sep + 1, static_cast<size_t>(numBytes - (sep - buffer.begin()) - 1));
    
    UrlCountResult result = processUrl(url, partitionCount, blobClient, container_name);
    
    if (result.size() != partitionCount) {
        exit(EXIT_FAILURE);
    }
    for (unsigned i = 0; i < partitionCount; i++) {
        std::stringstream response_stream;
        for (const auto& e : result[i]) {
            response_stream << e.first << '\t' << e.second << "\n";
        }

        std::string result_string = response_stream.str();
        if (!result_string.empty()) {
            sendToBlobStore(result_string, 
                          "subpartition_" + std::to_string(downloadId) + "_" + std::to_string(i + 1),
                          blobClient, 
                          container_name_intermediate);
        }
    }
    break;
}

         case MERGE: { // request: <partition id>
            // std::cerr << "\n=== woker is Merging===" << std::endl;
            buffer[12] = 0; // terminate string
            unsigned partitionId = static_cast<unsigned>(std::atoi(buffer.data() + 1));
            if (partitionId == 0) {
               std::cerr << "Invalid partition id" << std::endl;
               exit(EXIT_FAILURE);
            }

            auto result = mergeBlobsWithId(partitionId, blobClient, container_name_intermediate);
            std::stringstream response_stream;
            for (const auto& e : result) {
               response_stream << e.first << " " << e.second << "\n";
            }

            std::string result_string = response_stream.str();
            if (!result_string.empty()){
               // std::cerr << "\n=== sending to blob store the sorted_partition ===" << std::to_string(partitionId) << std::endl;
               sendToBlobStore(result_string, "sorted_partition_" + std::to_string(partitionId),blobClient, container_name_intermediate);
            }
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
   
   // LOG("Terminate worker");
   close(connection);
   return 0;
}
