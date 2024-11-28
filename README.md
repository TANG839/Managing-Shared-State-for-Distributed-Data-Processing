# Managing Shared State for Distributed Query Execution
The first task of this assignment is to deploy your assignment 4 solution (or the base one we provide) in Azure, and answer the relevant questions from the [assignment sheet](assignment_sheet_5.pdf).

The second and main programming task of the assignment is to implement distributed query execution with shared state. 

We start again with the same partitioned data on external storage and similar elasticity goals.
However, we now process a query that needs to share state between workers.

We want to calculate how often each domain appeared (not only a specific domain as in the last assignment), and report the result for the top 25 domains.

For each input partition, we build multiple (partial) aggregates, one for each domain, which we then need to merge. Merging on the coordinator, as we did in the last assignment, does not scale well for this query. To scale the merge phase, we partition the aggregates and store them in shared state. After the initial aggregation and partitioning, we distribute the work of merging the partial aggregate partitions, and send the merged results to the coordinator.

As an example, consider the initial `filelist.csv` with three partitions:

```
test.csv.1
test.csv.2
test.csv.3
```

Each worker takes a partition, partitions and aggregates it, and stores the aggregate in shared state files.
E.g., we partition the aggregates each into three partitions:

```
aggregated.1.test.csv.1
aggregated.2.test.csv.1
aggregated.3.test.csv.1
aggregated.1.test.csv.2
aggregated.2.test.csv.2
aggregated.3.test.csv.2
aggregated.1.test.csv.3
aggregated.2.test.csv.3
aggregated.3.test.csv.3
```

Now each of the workers takes one aggregate partition, e.g.:

```
aggregated.1.test.csv.1
aggregated.1.test.csv.2
aggregated.1.test.csv.3
```

For this aggregate partition, we can now calculate a total result and get the top 25 `top25.1.csv`, which we again store
in shared state. Afterwards, the coordinator can collect the small results, calculate, and print the overall top 25.

You can see a detailed diagram of the query execution stages [here](screenshots/diagram.png)

## Shared State

To test data stored in shared state locally, you can use files on your file system
([`std::fstream`](https://en.cppreference.com/w/cpp/io/basic_fstream)).
To share state in Azure, you can use the [Azure Storage Library](https://github.com/Azure/azure-storage-cpplite).
Our scaffold includes a small example how to use the Azure blob storage (similar to S3).

For Azure storage, you also need
to [create a storage account](https://learn.microsoft.com/en-us/azure/storage/common/storage-account-create?tabs=azure-cli):

```
az storage account create --name "cbdp$RANDOM" --resource-group cbdp-resourcegroup --location westeurope
```

You will also need to add the `Storage Blob Data Contributor` role assignment to that storage account through the Azure
web interface.

## Execution

Install dependencies. Slightly more due than last time due to the Azure library.

```
sudo apt install cmake g++ libcurl4-openssl-dev libssl-dev uuid-dev
```

In order to run the example we give you in the coordinator, add your storage account name and token in lines 33-34. 

To create an access token, use: 
```
az account get-access-token --resource https://storage.azure.com/ -o tsv --query accessToken
```
 
## Submission:
You can submit everything via GitLab.
First fork this repository, and add all members of your group to a single repository.
Then, in this group repository, add:
* Names of all members of your group in groupMembers.txt
* Code that implements the assignment
* Test scripts that demonstrate the capabilities of your solution (correctness, elasticity, resilience)
* A written report giving a brief description of your implementation, and answering the questions that you can find on the assignment sheet.


## Deploy to Azure:

In this assignment, please run your experiments on Microsoft Azure.
We have a [tutorial](AZURE_TUTORIAL.md) with detailed instructions on how to deploy and run your solution in Azure.