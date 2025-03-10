#include "AzureBlobClient.h"
#include <cassert>
#include <cstddef>
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <numeric>
#include <stack>
#include <string>

#include "CurlEasyPtr.h"
#include <array>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <curl/curl.h>
#include <netdb.h>
#include <sys/poll.h>
#include <unistd.h>

#if 1
#define LOG(str) std::cerr << str << std::endl;
#endif

enum Status {
   NEW = 0,
   INITIALIZED,
};

enum Command {
   INIT = 0,
   DOWNLOAD,
   MERGE
};

static bool sendCommand(int fd, const char* data, size_t size, Command cmd) {
   // send partition count
   std::vector<char> buf;
   buf.reserve(size + 1);
   std::memcpy(buf.data() + 1, data, size);
   buf[0] = cmd;

   if (auto status = send(fd, buf.data(), size + 1, 0); status == -1) {
      perror(("Coordinator, send() command failed " + std::to_string(cmd)).c_str());
      return true;
   }
   return false;
}

// Return a listening socket
int getListenerSocket(char* port) {
   addrinfo hints = {};
   hints.ai_flags = AI_PASSIVE;
   hints.ai_family = AF_INET6;
   hints.ai_socktype = SOCK_STREAM;

   // Get us a socket and bind it
   addrinfo* aInfo = nullptr;
   if (auto status = getaddrinfo(nullptr, port, &hints, &aInfo); status != 0) {
      std::cerr << "getaddrinfo() failed: " << gai_strerror(status) << std::endl;
      exit(1);
   }

   int listener;
   addrinfo* iter;
   for (iter = aInfo; iter; iter = iter->ai_next) {
      listener = socket(iter->ai_family, iter->ai_socktype, iter->ai_protocol);
      if (listener < 0)
         continue;

      // Lose the pesky "address already in use" error message
      int optval = 1;
      setsockopt(listener, SOL_SOCKET, SO_REUSEADDR, &optval, sizeof(int));

      if (bind(listener, iter->ai_addr, iter->ai_addrlen) < 0) {
         perror("failed to bind listener");
         close(listener);
         continue;
      }

      break;
   }

   freeaddrinfo(aInfo); // All done with this

   // If we got here, it means we didn't get bound
   if (!iter) {
      std::cerr << "could not bind() to: " << port << std::endl;
      exit(1);
   }

   // Listen
   if (listen(listener, 128) == -1) {
      perror("listen() failed");
      exit(1);
   }

   return listener;
}

bool receiveResponse(pollfd fd, auto handleClientFailure) {
   std::stringstream ss;
   std::array<char, 1024> buffer;

   ssize_t numBytes = recv(fd.fd, buffer.data(), buffer.size() - 1, 0);

   if (numBytes > 0) {
      buffer[static_cast<size_t>(numBytes)] = '\0';
      std::string_view sv(buffer.begin(), buffer.begin() + numBytes);
      if (sv != "Success\n") {
         handleClientFailure();
         return true;
      }

   } else {
      if (numBytes < 0) {
         perror("recv() failed");
      }

      handleClientFailure();
      return true;
   }

   return false;  
}

std::unordered_map<int, Status> workerStatus; // fd -> status

const int PARTITION_COUNT = 32;

int main(int argc, char* argv[]) {
   if (argc != 3) {
      std::cerr << "Usage: " << argv[0] << " <URL to csv list> <listen port>" << std::endl;
      return 1;
   }

   std::string partitionCountStr = std::to_string(PARTITION_COUNT);

   auto curlSetup = CurlGlobalSetup();

   auto listUrl = std::string(argv[1]);

   static const std::string accountName = "csb10032003e7e1ee2c";
   static const std::string accountToken = "eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsIng1dCI6InoxcnNZSEhKOS04bWdndDRIc1p1OEJLa0JQdyIsImtpZCI6InoxcnNZSEhKOS04bWdndDRIc1p1OEJLa0JQdyJ9.eyJhdWQiOiJodHRwczovL3N0b3JhZ2UuYXp1cmUuY29tLyIsImlzcyI6Imh0dHBzOi8vc3RzLndpbmRvd3MubmV0LzFjNmJhMjc0LTNjMzktNGE2MC1hMzZiLTM2N2IxNDc3MGM5My8iLCJpYXQiOjE3MzQ1MzQyNjMsIm5iZiI6MTczNDUzNDI2MywiZXhwIjoxNzM0NTM4MzM3LCJhY3IiOiIxIiwiYWlvIjoiQVlRQWUvOFlBQUFBRTRzUC9QTUx5dDM1aDkvNC92QkIzV081YnRqQ25hNFBjSWF5ZEZZSjRtajVkZFNCZ000MXA5S3d4ZFhkamx1MnJSc3pLeUw4enQ4M1JVMmpLVkdGSlNWbkhKSW5uQ3NVNmJ1cUxWVDlseVlxVERWbnVLaVduSFJIVXNJcEYrbFh3ODFhZm1jeU1PT29NZkI4czBWZjMyUzVWd2ZPLzNkdTNXMUNidmxLRFpJPSIsImFsdHNlY2lkIjoiMTpsaXZlLmNvbTowMDAzNDAwMjgwNzE1OEI2IiwiYW1yIjpbInB3ZCIsIm1mYSJdLCJhcHBpZCI6IjA0YjA3Nzk1LThkZGItNDYxYS1iYmVlLTAyZjllMWJmN2I0NiIsImFwcGlkYWNyIjoiMCIsImVtYWlsIjoibW9hYWQubWFhcm91ZmlAaWNsb3VkLmNvbSIsImZhbWlseV9uYW1lIjoiTWFyb3VmaSIsImdpdmVuX25hbWUiOiJNb2FkIiwiZ3JvdXBzIjpbIjY0MGYzNGQ3LThlNzctNDg2My04OWE2LTI1ZjY4YjEyNTMxOCJdLCJpZHAiOiJsaXZlLmNvbSIsImlkdHlwIjoidXNlciIsImlwYWRkciI6IjJhMDk6ODBjMDoxOTI6MDo1OGVlOjU5ZmM6OTM0YTozYTA5IiwibmFtZSI6Ik1vYWQgTWFyb3VmaSIsIm9pZCI6IjZiMDhiYWE0LWJlYzctNDhhOC05OGQ0LTIyYTIyNmNkYWEzZSIsInB1aWQiOiIxMDAzMjAwM0U3RTFFRTJDIiwicmgiOiIxLkFSTUJkS0pySERrOFlFcWphelo3RkhjTWs0R21CdVRVODZoQ2tMYkNzQ2xKZXZFVUFUSVRBUS4iLCJzY3AiOiJ1c2VyX2ltcGVyc29uYXRpb24iLCJzdWIiOiJKc3lwVWo3UkwxdnJmcndsYS1zYlVBNGxGOW9FSXkwUGdFUG5hUUZqWndJIiwidGlkIjoiMWM2YmEyNzQtM2MzOS00YTYwLWEzNmItMzY3YjE0NzcwYzkzIiwidW5pcXVlX25hbWUiOiJsaXZlLmNvbSNtb2FhZC5tYWFyb3VmaUBpY2xvdWQuY29tIiwidXRpIjoiTmNfbGh3OURZRXloYnNDWnFUWE5BQSIsInZlciI6IjEuMCIsInhtc19pZHJlbCI6IjEgMTYiLCJ4bXNfdGRiciI6IkVVIn0.D_mJVbFFIGJO8_BUXQfieUq12ilQlYUDmNJnE0nzVz_F4-zo3eIF3EhcjBWjOMvXO0lEzM3kW5uZ5q_dsVMfZJkS-e1c0kaDQd5CHdkrT3CDkb8Y9rGeszMfGsDDkttCzo0cvrYxi7bD4W9JDTqecdx6vmCBXDuVqS5bk0SMpdVv1GMXTPYhfXza2HTneOM0lMMN_v3gAwYIq2fZ0NFzxPsUlxNkKym9a8Ygd2bg8f2PPphmkdYU6mYdoHuPSMybnOB27pPo3WaVuXLgSjgIZ14coVceTgpK8ngVq-c3OPz2JLtqr7CfptiawSQ2QWmljm1WYNDNDbrzkNrX3qs9yQ";
   static const std::string container_name ="cbdp-files";
   static const std::string container_name_intermediate ="cbdp-files-intermediate";

   auto blobClient = AzureBlobClient(accountName, accountToken,container_name);
   auto fileList = blobClient.downloadStringStream("filelist.txt",container_name);

   static unsigned fileId = 0;
   std::vector<std::pair<std::string, unsigned>> filesTodo;
   filesTodo.reserve(100);
   for (std::string url; std::getline(fileList, url, '\n');)
      filesTodo.emplace_back(std::move(url), fileId++);

   // Listen
   auto listener = getListenerSocket(argv[2]);
   unsigned workerID = 0;

   // Setup polling for new connections and worker responses
   std::vector<pollfd> pollFds;
   pollFds.push_back(pollfd{
      .fd = listener,
      .events = POLLIN,
      .revents = {},
   });

   // Distribute the work
   auto distributedWork = std::unordered_map<int, std::pair<std::string, unsigned>>();

   auto assignWork = [&](int fd) {
      if (filesTodo.empty())
         return;
      
      distributedWork[fd] = std::move(filesTodo.back());
      filesTodo.pop_back();

      const auto& file = distributedWork[fd];
      std::string msg = std::to_string(file.second) + ' ' + file.first;

      if (sendCommand(fd, msg.c_str(), msg.size(), DOWNLOAD)) {
         filesTodo.push_back(std::move(distributedWork[fd]));
         distributedWork.erase(fd);
      }
   };

   // Init and distribute download tasks
   while (!filesTodo.empty() || !distributedWork.empty()) {
      poll(pollFds.data(), static_cast<nfds_t>(pollFds.size()), -1);
      for (size_t index = 0, limit = pollFds.size(); index != limit; ++index) {
         const auto& pollFd = pollFds[index];
         // Look for ready connections
         if (!(pollFd.revents & POLLIN)) continue; // this is a bit whack but ok

         if (pollFd.fd == listener) {
            // Incoming connection -> accept
            auto addr = sockaddr();
            socklen_t addrLen = sizeof(sockaddr);
            auto newConnection = accept(listener, &addr, &addrLen);
            if (newConnection == -1) {
               perror("accept() failed");
               continue;
            }
            pollFds.push_back(pollfd{
               .fd = newConnection,
               .events = POLLIN,
               .revents = {},
            });

            workerStatus[newConnection] = NEW;
            std::string request = partitionCountStr + ' ' + std::to_string(workerID++);

            if (sendCommand(newConnection, request.data(), request.size(), INIT)) {
               exit(EXIT_FAILURE);
            }

            continue;
         }

         if (workerStatus[pollFd.fd] == NEW) {
            bool failed = receiveResponse(pollFd, [&] {
               LOG("Failed during init: " << pollFd.fd);

               // Drop the connection
               close(pollFd.fd); // Bye!
               std::swap(pollFds[index], pollFds.back());
               pollFds.pop_back();
               --index;
               --limit;
            });


            if (!failed) {
               workerStatus[pollFd.fd] = INITIALIZED;
               assignWork(pollFd.fd);
            }


            continue;
         }

         if (workerStatus[pollFd.fd] == INITIALIZED) {
            receiveResponse(pollFd, [&] {
               LOG("Failed during download: " << pollFd.fd);

               // Worker failed. Make sure the work gets done by someone else.
               if (distributedWork.contains(pollFd.fd)) {
                  filesTodo.push_back(std::move(distributedWork[pollFd.fd]));
                  distributedWork.erase(pollFd.fd);
               }
               // Drop the connection
               close(pollFd.fd); // Bye!
               std::swap(pollFds[index], pollFds.back());
               pollFds.pop_back();
               --index;
               --limit;
            });

            distributedWork.erase(pollFd.fd);

            // Assign more work
            assignWork(pollFd.fd);
            continue;
         }
      }
   }

   std::stack<unsigned> partitionsToMerge;
   for (unsigned i = 1; i <= PARTITION_COUNT; i++)
      partitionsToMerge.push(i);

   std::unordered_map<int, unsigned> mergingPartitions;

   // distribute merging tasks
   auto sendMerge = [&](size_t& index, size_t& limit, int fd) {
      assert(workerStatus[fd] == INITIALIZED && "Workers must be initialized");

      if (partitionsToMerge.empty()) {
         mergingPartitions.erase(fd);
         return;
      }

      unsigned partitionId = partitionsToMerge.top();
      partitionsToMerge.pop();
      mergingPartitions[fd] = partitionId;

      std::string partitionIdStr = std::to_string(partitionId);

      if (sendCommand(fd, partitionIdStr.data(), partitionIdStr.size(), MERGE)) {
         // handle failure
         LOG("Failed during merge: " << fd);
         partitionsToMerge.push(partitionId);
         mergingPartitions.erase(fd);
         std::swap(pollFds[index], pollFds.back());
         pollFds.pop_back();
         --index;
         --limit;
      }
   };

   size_t limit = std::min(partitionsToMerge.size(), pollFds.size());
   for (size_t index = 0; index < limit; ++index) {
      const auto& pollFd = pollFds[index];
      // Look for ready connections
      if (pollFd.fd == listener) continue;

      sendMerge(index, limit, pollFd.fd);
   }
      
   while (!partitionsToMerge.empty() || !mergingPartitions.empty()) {
      poll(pollFds.data(), static_cast<nfds_t>(pollFds.size()), -1);

      for (size_t index = 0, limit = pollFds.size(); index != limit; ++index) {
         const auto& pollFd = pollFds[index];
         // Look for ready connections
         if (!(pollFd.revents & POLLIN) || pollFd.fd == listener) continue;

         receiveResponse(pollFd, [&] {
            LOG("Failed during merge: " << pollFd.fd);

            // Worker failed. Make sure the work gets done by someone else.
            if (mergingPartitions.contains(pollFd.fd)) {
               partitionsToMerge.push(std::move(mergingPartitions[pollFd.fd]));
               mergingPartitions.erase(pollFd.fd);
            }

            // Drop the connection
            close(pollFd.fd); // Bye!
            std::swap(pollFds[index], pollFds.back());
            pollFds.pop_back();
            --index;
            --limit;
         });

       sendMerge(index, limit, pollFd.fd);
      }
   }


// std::cerr << "\n=== Starting Final URL Processing ===" << std::endl;
std::unordered_map<std::string, unsigned> combinedCounts;
auto blobList = blobClient.listBlobs(container_name_intermediate);
// std::cerr << "Found " << blobList.size() << " blobs in container '" << container_name_intermediate << "'" << std::endl;

// First pass: combine all counts for identical URLs
for (const auto& blobName : blobList) {
    if (blobName.starts_with("sorted_partition_")) {
      //   std::cerr << "Processing blob: " << blobName << std::endl;
        auto blobContent = blobClient.downloadStringStream(blobName, container_name_intermediate);
      // to find the count we look for the last whote space in the line
      std::string line;
      while (std::getline(blobContent, line)) {
         size_t lastSpace = line.rfind(' ');
         if (lastSpace != std::string::npos) {
            std::string url = line.substr(0, lastSpace);
            // Cast explicitly to handle the precision warning
            auto countLong = std::stoul(line.substr(lastSpace + 1));
            unsigned int count = static_cast<unsigned int>(countLong);
            combinedCounts[url] += count;
         }
      }
      //   std::cerr << "Processed " << blobName << std::endl;
    }
}

// Convert to vector for sorting
std::vector<std::pair<std::string, unsigned>> finalResults;
finalResults.reserve(combinedCounts.size());
for (const auto& [url, count] : combinedCounts) {
    finalResults.emplace_back(url, count);
}

// Sort by count and keep top 25
std::partial_sort(
    finalResults.begin(),
    finalResults.begin() + static_cast<long>(std::min(finalResults.size(), 25ul)),
    finalResults.end(),
    [](const auto& a, const auto& b) { return a.second > b.second; }
);

if (finalResults.size() > 25) {
    finalResults.resize(25);
}

// std::cerr << "\n=== Final Results ===" << std::endl;
// std::cerr << "Total unique URLs found: " << combinedCounts.size() << std::endl;
// std::cerr << "Showing top " << finalResults.size() << " results:" << std::endl;

for (const auto& [url, count] : finalResults) {
    std::cout << url << " " << count << std::endl;
}

// std::cerr << "=== Processing Complete ===" << std::endl;

// Cleanup
for (auto& pollFd : pollFds) {
    close(pollFd.fd);
}

return 0;
}