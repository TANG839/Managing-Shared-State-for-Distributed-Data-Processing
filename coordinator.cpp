#include "AzureBlobClient.h"
#include <cassert>
#include <cstddef>
#include <cstdlib>
#include <iostream>
#include <numeric>
#include <stack>
#include <string>

#include "CurlEasyPtr.h"
#include <array>
#include <charconv>
#include <iostream>
#include <sstream>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <netdb.h>
#include <sys/poll.h>
#include <unistd.h>

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

auto receiveResponse(pollfd fd, auto handleClientFailure) {
   std::stringstream ss;
   std::array<char, 1024> buffer;

   while (true) {
      ssize_t numBytes = recv(fd.fd, buffer.data(), buffer.size() - 1, 0);

      if (numBytes > 0) {
         buffer[static_cast<size_t>(numBytes)] = '\0';
         std::string_view sv(buffer.begin(), buffer.begin() + numBytes);
         ss << sv;
         if (sv.back() == '\n')
            break;

      } else {
            if (numBytes < 0) {
                perror("recv() failed");
            }

            handleClientFailure();
            break;
        }
   }

   std::string line;
   std::vector<std::pair<std::string, unsigned>> parsedData;

   std::string url;
   unsigned number;
   while (ss >> url >> number) {
      parsedData.emplace_back(url, number);
   }

   return parsedData;
}

std::unordered_map<int, Status> workerStatus; // fd -> status

int main(int argc, char* argv[]) {
   if (argc != 3) {
      std::cerr << "Usage: " << argv[0] << " <URL to csv list> <listen port>" << std::endl;
      return 1;
   }

   const int partitionCount = 3;
   std::string partitionCountStr = std::to_string(partitionCount);

   auto curlSetup = CurlGlobalSetup();

   auto listUrl = std::string(argv[1]);

   // Download the file list
   auto curl = CurlEasyPtr::easyInit();
   curl.setUrl(listUrl);
   auto fileList = curl.performToStringStream();

   std::vector<std::string> filesTodo;
   filesTodo.reserve(100);
   for (std::string url; std::getline(fileList, url, '\n');)
      filesTodo.push_back(std::move(url));

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
   auto distributedWork = std::unordered_map<int, std::string>();

   auto assignWork = [&](int fd) {
      if (filesTodo.empty()) 
         return;

      distributedWork[fd] = std::move(filesTodo.back());
      filesTodo.pop_back();

      const auto& file = distributedWork[fd];

      if(sendCommand(fd, file.c_str(), file.size(), DOWNLOAD)) {
         filesTodo.push_back(std::move(distributedWork[fd]));
         distributedWork.erase(fd);
      }

   };

   // Init and distribute download tasks
   while (!filesTodo.empty() || !distributedWork.empty()) {
      poll(pollFds.data(), pollFds.size(), -1);
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
            std::string request = partitionCountStr + " " + std::to_string(workerID++);
            if (sendCommand(newConnection, request.data(), request.size(), INIT)) {
               exit(EXIT_FAILURE);
            }

            continue;
         }

         auto handleClientFailure = [&] {
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
         };

         if (workerStatus[pollFd.fd] == NEW) {
               std::cout << "assign work " << pollFd.fd << std::endl;

            workerStatus[pollFd.fd] = INITIALIZED;
            assignWork(pollFd.fd);
            continue;
         }

         if (workerStatus[pollFd.fd] == INITIALIZED) {
            auto result = receiveResponse(pollFd, handleClientFailure);
            distributedWork.erase(pollFd.fd);

            // Assign more work
            assignWork(pollFd.fd);
            continue;
         }
      }
   }

   std::stack<unsigned> partitionsToMerge;
   for (unsigned i = 1; i <= partitionCount; i++)
      partitionsToMerge.push(i);

   std::unordered_map<int, unsigned> mergingPartitions;

   // distribute merging tasks
   while (!partitionsToMerge.empty() || !mergingPartitions.empty()) {
      poll(pollFds.data(), pollFds.size(), -1);
      for (size_t index = 0, limit = pollFds.size(); index != limit; ++index) {
         const auto& pollFd = pollFds[index];
         // Look for ready connections
         if (!(pollFd.revents & POLLIN)) continue; // this is a bit whack but ok

         assert(workerStatus[pollFd.fd] == INITIALIZED && "Workers must be initialized");

         unsigned partitionId = partitionsToMerge.top();
         partitionsToMerge.pop();
         mergingPartitions[pollFd.fd] = partitionId;

         std::string partitionIdStr = std::to_string(partitionId);
          std::cout << partitionId << std::endl;

         if(sendCommand(pollFd.fd, partitionIdStr.data(), partitionIdStr.size(), MERGE)) {
            // handle failure
            partitionsToMerge.push(partitionId);
            mergingPartitions.erase(pollFd.fd);
            std::swap(pollFds[index], pollFds.back());
            pollFds.pop_back();
            --index;
            --limit;
         }            
      }
   }

   // TODO: read merged files from blob store and compute top 25. entries are sorted -> use std::merge to merge

   // Cleanup
   for (auto& pollFd : pollFds)
      close(pollFd.fd);

   return 0;
}

/*
   static const std::string accountName = "cbdp1";
   static const std::string accountToken = "eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsIng1dCI6IlQxU3QtZExUdnlXUmd4Ql82NzZ1OGtyWFMtSSIsImtpZCI6IlQxU3QtZExUdnlXUmd4Ql82NzZ1OGtyWFMtSSJ9.eyJhdWQiOiJodHRwczovL3N0b3JhZ2UuYXp1cmUuY29tLyIsImlzcyI6Imh0dHBzOi8vc3RzLndpbmRvd3MubmV0LzdlODJmZDZkLTNlZGUtNDg3Mi1hZGMzLTgxNTExMTg1YjFhYy8iLCJpYXQiOjE3MDEyODQ4NDQsIm5iZiI6MTcwMTI4NDg0NCwiZXhwIjoxNzAxMjg5MDk5LCJhY3IiOiIxIiwiYWlvIjoiQVlRQWUvOFZBQUFBWENEUHFhQmxrUTBUUStIRUhyU2xveHNkMTdyZTR4MjR3ejhtSEpyS2pBNGlJL2hpOGlNZTUvTzQ2R0tlSGlMQytpem5kN2hyUWVBV2hNV2NqVWpOYmRQL2dGRHFTb05XRlVNSjUyaUJiRDNpVHdCT2xhSHJCQ2RSUDFZREFwdk92NDVOZERBSU5hTWFad1JvMUMwNDRzU1RVb0tZYktJbXE4SVo1S1BqSHI0PSIsImFsdHNlY2lkIjoiMTpsaXZlLmNvbTowMDAzMDAwMDY5NTAyNThGIiwiYW1yIjpbInB3ZCIsIm1mYSJdLCJhcHBpZCI6IjA0YjA3Nzk1LThkZGItNDYxYS1iYmVlLTAyZjllMWJmN2I0NiIsImFwcGlkYWNyIjoiMCIsImVtYWlsIjoiZ29ldHp0QGluLnR1bS5kZSIsImZhbWlseV9uYW1lIjoiR29ldHoiLCJnaXZlbl9uYW1lIjoiVG9iaWFzIiwiZ3JvdXBzIjpbIjkyZTA5YmMwLWNjYjAtNDI5NS1hNWQwLWY3YWM5NTQ0MjAxYiJdLCJpZHAiOiJsaXZlLmNvbSIsImlwYWRkciI6IjJhMDE6YzIzOmJkOTc6M2YwMDpkOGVjOjVkM2E6MThhOjNlZWIiLCJuYW1lIjoiVG9iaWFzIEdvZXR6Iiwib2lkIjoiMTJiZGY5MGEtYTYzZi00YWQzLWE5ZmItZWI1N2RlODI3YTU2IiwicHVpZCI6IjEwMDMyMDAzMTU3QTdCQTQiLCJyaCI6IjAuQWE4QWJmMkNmdDQtY2tpdHc0RlJFWVd4cklHbUJ1VFU4NmhDa0xiQ3NDbEpldkdzQUU4LiIsInNjcCI6InVzZXJfaW1wZXJzb25hdGlvbiIsInN1YiI6ImVfT0dxVUt1ZUdwekpfOHg2WTNueG5QS2pDcG1qRk1DeWhIcDRwMTNhTXMiLCJ0aWQiOiI3ZTgyZmQ2ZC0zZWRlLTQ4NzItYWRjMy04MTUxMTE4NWIxYWMiLCJ1bmlxdWVfbmFtZSI6ImxpdmUuY29tI2dvZXR6dEBpbi50dW0uZGUiLCJ1dGkiOiJGNm1DRkZNMlQwYUxqMkdJTExjckFRIiwidmVyIjoiMS4wIiwieG1zX3RkYnIiOiJFVSJ9.JrdlunDBTsyzm7qIO7eNG50G2FajoEAZpU0ckm11tcgkeLB87GXh0GCdmHxvlz2KuChywz7QfvyzL7xkJI5Lg2gXsclrYx-3Y0VmdJSf2gq5HfPMiZ8yJTrv3wkYWqPbGH22Af0stkqC_-P5EidR_NDNAI8hETWoosyK80KHbsS9dn8xScTb2GZMaVC_fFvahgdmcOzyLuDayd9OalnN6yacP1HEAfxs6gpgUXz8OYJ6N7VSeoRAaPodHAiSTiJG1qouxNC5YyqSmXwNj5w8m6JHYVn2kwmqc2kAzVDdbEB03JYMFajq4MYwbLTY2O_qkyir3_MbUq6NB17_tRDZZA";
   auto blobClient = AzureBlobClient(accountName, accountToken);

   std::cerr << "Creating Azure blob container" << std::endl;
   blobClient.createContainer("cbdp-assignment-5");

   std::cerr << "Uploading a blob" << std::endl;
   {
      std::stringstream upload;
      upload << "Hello World!" << std::endl;
      blobClient.uploadStringStream("hello", upload);
   }

   std::cerr << "Downloading the blob again" << std::endl;
   auto downloaded = blobClient.downloadStringStream("hello");

   std::cerr << "Received: " << downloaded.view() << std::endl;

   std::cerr << "Deleting the container" << std::endl;
   blobClient.deleteContainer();
*/

// helper function for extracting domains
// static std::string_view getDomain(std::string_view url)
// {
//    using namespace std::literals;
//    auto pos = url.find("://"sv);
//    if (pos != std::string::npos) {
//       auto afterProtocol = std::string_view(url).substr(pos + 3);
//       auto endDomain = afterProtocol.find('/');
//       return afterProtocol.substr(0, endDomain);
//    }
//    return url;
// }


