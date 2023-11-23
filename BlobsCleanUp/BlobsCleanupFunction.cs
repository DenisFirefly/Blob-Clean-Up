using System;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Logging;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage;

namespace BlobsCleanUp
{
    public class BlobsCleanupFunction
    {
        [FunctionName("BlobsCleanupFunction")]
        public static void Run([TimerTrigger("0 0 0 1 6 3", RunOnStartup = true)]TimerInfo myTimer, ILogger log)
        {
            string storageAccountName = "jsonproblemstorage";
            string storageAccountKey = "zv+tanERNbrDAu/9PjKlq5JMHGJHn27V5sx64VHvqLQM41OxPJd1DX72emedlwY35Gv6W9WKkXJw+AStiNSUzg==";
            string containerName = "json-test-try";
            string searchText = "updated";

            try
            {
                // Create a storage credentials object using the storage account name and key
                StorageCredentials storageCredentials = new(storageAccountName, storageAccountKey);

                // Create a CloudStorageAccount object using the storage credentials
                CloudStorageAccount storageAccount = new(storageCredentials, true);

                // Create a CloudBlobClient object using the storage account
                CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient();

                // Get a reference to the container
                CloudBlobContainer container = blobClient.GetContainerReference(containerName);

                // Set the BlobContinuationToken to null to start from the beginning
                BlobContinuationToken continuationToken = null;

                do
                {
                    // Get a segment of blobs from the container
                    BlobResultSegment resultSegment = container.ListBlobsSegmentedAsync("", true, BlobListingDetails.None, null, continuationToken, null, null).Result;

                    // Process each blob in the segment
                    foreach (IListBlobItem blobItem in resultSegment.Results)
                    {
                        log.LogWarning($"blobItem is CloudBlockBlob: {blobItem is CloudBlockBlob}");
                        log.LogWarning($"blobItem is CloudBlockBlob: {blobItem.GetType}");
                        /*if (blobItem is CloudBlockBlob blob && blob.Name.Contains(searchText))*/
                        if (blobItem is CloudBlockBlob blob && !blob.Name.Contains(searchText))
                        {
                            log.LogWarning($"!blob.Name.Contains(searchTextVersionTwo): {!blob.Name.Contains(searchText)}");
                            // Delete the blob
                            blob.DeleteAsync();
                            log.LogInformation($"Deleted blob: {blob.Name}");
                        } else if (blobItem is CloudAppendBlob appendBlob && !appendBlob.Name.Contains(searchText))
                        {
                            log.LogWarning($"!blob.Name.Contains(searchTextVersionTwo): {!appendBlob.Name.Contains(searchText)}");
                            // Delete the blob
                            appendBlob.DeleteAsync();
                            log.LogInformation($"Deleted blob: {appendBlob.Name}");
                        }
                    }

                    // Set the continuation token for the next segment, if available
                    continuationToken = resultSegment.ContinuationToken;
                }
                while (continuationToken != null);
            }
            catch (Exception ex)
            {
                log.LogError($"Error occurred: {ex.Message}");
            }
        }
    }
}
