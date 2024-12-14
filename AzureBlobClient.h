#ifndef CBDP_AZUREBLOBCLIENT_H
#define CBDP_AZUREBLOBCLIENT_H

#include "blob/blob_client.h"
#include <sstream>
#include <string>
#include <vector>

/// A simplified wrapper around the Azure storage API
class AzureBlobClient {
   /// The blob client
   azure::storage_lite::blob_client client;
   /// The name of the current container. We create a single global container for this assignment
   std::string containerName;

   /// Create the blob_client with the given credentials
   static azure::storage_lite::blob_client createClient(const std::string& accountName, const std::string& accessToken);

   public:
   /// Constructor
   /// @accountName: The account name. You should be able to see the account details via:
   ///               az storage account list
   /// @accessToken: An access token for azure. Get an access token via:
   ///               az account get-access-token --resource https://storage.azure.com/ -o tsv --query accessToken
   AzureBlobClient(const std::string& accountName, const std::string& accessToken, const std::string& container_name );
   AzureBlobClient(const AzureBlobClient&) = delete;
   AzureBlobClient& operator=(const AzureBlobClient&) = delete;

   /// Create a container that stores all blobs
   void createContainer(std::string containerName);

   /// Set the container name for operations
   void setContainer(std::string containerName);
   
   /// Delete the container that stored all blobs
   void deleteContainer();

   /// Write a string stream to a blob
   void uploadStringStream(const std::string& blobName, std::stringstream& stream,const std::string& container_name_intermediate);

   /// Read a string stream from a blob
   std::stringstream downloadStringStream(const std::string& blobName, const std::string& container_name_intermediate);

   /// List all blobs in the container
   std::vector<std::string> listBlobs(const std::string& container_name_intermediate);
};

#endif
