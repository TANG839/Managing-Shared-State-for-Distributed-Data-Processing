#include "AzureBlobClient.h"

static std::string formatError(const azure::storage_lite::storage_error& error)
// Format an Azure storage error
{
   return "Error " + error.code + ": " + error.code_name + (error.message.empty() ? "" : ": ") + error.message;
}

azure::storage_lite::blob_client AzureBlobClient::createClient(const std::string& accountName, const std::string& accessToken)
// Create a container that stores all blobs
{
   using namespace azure::storage_lite;
   std::shared_ptr<storage_credential> cred = std::make_shared<token_credential>(accessToken);
   std::shared_ptr<storage_account> account = std::make_shared<storage_account>(accountName, std::move(cred), /* use_https */ true);

   return {std::move(account), 16};
}

AzureBlobClient::AzureBlobClient(const std::string& accountName, const std::string& accessToken)
   : client(createClient(accountName, accessToken))
// Constructor
{
}

void AzureBlobClient::createContainer(std::string containerName)
// Create a container that stores all blobs
{
   auto containerRequest = client.create_container(containerName).get();
   if (!containerRequest.success())
      throw std::runtime_error("Azure create container failed: " + formatError(containerRequest.error()));
   this->containerName = std::move(containerName);
}

void AzureBlobClient::deleteContainer()
// Delete the container that stored all blobs
{
   auto deleteRequest = client.delete_container(containerName).get();
   if (!deleteRequest.success())
      throw std::runtime_error("Azure delete container failed: " + formatError(deleteRequest.error()));
   this->containerName = {};
}

void AzureBlobClient::uploadStringStream(const std::string& blobName, std::stringstream& stream)
// Write a string stream to a blob
{
   auto uploadRequest = client.upload_block_blob_from_stream(containerName, blobName, stream, {}).get();
   if (!uploadRequest.success())
      throw std::runtime_error("Azure upload blob failed: " + formatError(uploadRequest.error()));
}

std::stringstream AzureBlobClient::downloadStringStream(const std::string& blobName)
// Read a string stream from a blob
{
   std::stringstream result;
   auto downloadRequest = client.download_blob_to_stream(containerName, blobName, 0, 0, result).get();
   if (!downloadRequest.success())
      throw std::runtime_error("Azure download blob failed: " + formatError(downloadRequest.error()));
   return result;
}

std::vector<std::string> AzureBlobClient::listBlobs()
// List all blobs in the container
{
   std::vector<std::string> results;
   std::string continuationToken;

   do {
      auto blobs = client.list_blobs_segmented(containerName, "/", continuationToken, "").get();
      if (!blobs.success())
         throw std::runtime_error("Azure list blobs: " + formatError(blobs.error()));
      for (auto& blob : blobs.response().blobs)
         results.push_back(std::move(blob.name));
      continuationToken = std::move(blobs.response().next_marker);
   } while (!continuationToken.empty());

   return results;
}
