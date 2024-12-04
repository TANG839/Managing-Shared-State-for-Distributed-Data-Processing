#include "AzureBlobClient.h"
#include <cstddef>
#include <iostream>
#include <string>

#include "CurlEasyPtr.h"
#include <array>
#include <charconv>
#include <iostream>
#include <sstream>
#include <string>
#include <unordered_map>
#include <vector>
#include <netdb.h>
#include <sys/poll.h>
#include <unistd.h>

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
         ss << std::string_view(buffer.begin(), buffer.begin() + numBytes);

      } else {
            if (numBytes < 0) {
                perror("recv() failed");
            }

            handleClientFailure();
            continue;
        }
   }

   std::string line;
   std::vector<std::pair<std::string, unsigned>> parsedData;

   while (std::getline(ss, line)) {
      std::istringstream lineStream(line);
      std::string url;
      unsigned number;

      if (lineStream >> url >> number) {
         parsedData.emplace_back(url, number);
      } else {
         std::cerr << "Invalid line format: " << line << std::endl;
      }
   }

   // Process the parsed data
   return parsedData;
}

int main(int argc, char* argv[]) {
   if (argc != 3) {
      std::cerr << "Usage: " << argv[0] << " <URL to csv list> <listen port>" << std::endl;
      return 1;
   }

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

   // Setup polling for new connections and worker responses
   std::vector<pollfd> pollFds;
   pollFds.push_back(pollfd{
      .fd = listener,
      .events = POLLIN,
      .revents = {},
   });

   // Distribute the work
   size_t result = 0;
   auto distributedWork = std::unordered_map<int, std::string>();

   auto assignWork = [&](int fd) {
      if (filesTodo.empty()) 
         return;

      distributedWork[fd] = std::move(filesTodo.back());
      filesTodo.pop_back();

      const auto& file = distributedWork[fd];
      if (auto status = send(fd, file.c_str(), file.size(), 0); status == -1) {
         perror("send() failed");
         filesTodo.push_back(std::move(distributedWork[fd]));
         distributedWork.erase(fd);
      }
   };

   while (!filesTodo.empty() || !distributedWork.empty()) {
      poll(pollFds.data(), pollFds.size(), -1);
      for (size_t index = 0, limit = pollFds.size(); index != limit; ++index) {
         const auto& pollFd = pollFds[index];
         // Look for ready connections
         if (!(pollFd.revents & POLLIN)) continue;

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

            // And directly assign some work
            assignWork(newConnection);
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

         // Client is ready -> recv result and send more work
         auto buffer = std::array<char, 32>();
         auto numBytes = recv(pollFd.fd, buffer.data(), buffer.size(), 0);
         if (numBytes <= 0) {
            if (numBytes < 0)
               perror("recv() failed: ");
            handleClientFailure();
            continue;
         }

         // Result ok
         result += clientResult;
         distributedWork.erase(pollFd.fd);

         // Assign more work
         assignWork(pollFd.fd);
      }
   }

   std::cout << result << std::endl;

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


