#include "AzureBlobClient.h"
#include <iostream>
#include <string>



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

/// Leader process that coordinates workers. Workers connect on the specified port
/// and the coordinator distributes the work of the CSV file list.
/// Example:
///    ./coordinator http://example.org/filelist.csv 4242
int main(int argc, char* argv[]) {
   if (argc != 3) {
      std::cerr << "Usage: " << argv[0] << " <URL to csv list> <listen port>" << std::endl;
      return 1;
   }

   // TODO: add your azure credentials, get them via:
   // az storage account list
   // az account get-access-token --resource https://storage.azure.com/ -o tsv --query accessToken
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

   return 0;
}

