# Managing Shared State for Distributed Query Execution Documentation
We will present 2 ways our solution can be deployed: either locally or on Azure Cloud. For both solutions, Blob storage is needed.
We use 2 containers for Blob storage: one for reading the files and the 100 partitions, and one for intermediate writing of sub-partitions/merging etc.

Both containers need to be created manually on the cloud and the required names and credentials must be filled in the code before compiling.
The intermediate container needs to be empty. When you rerun the experiment, you need to delete it and create another one with the same name manually.

We first upload the files from https://db.in.tum.de/teaching/ws2425/clouddataprocessing/data to the Azure Blob storage container. We also upload a version of filelist that only contains the file names as they are in Azure to the same container:
```bash
clickbench.00.csv
clickbench.01.csv
clickbench.02.csv
...
```
The Python script `send_data_to_azure.py` automates the uploading process.
Note that we use an Azure Storage connection string to upload the data to Azure Blob storage in the script which you need to modify. The connection string follows this format:
```bash
DefaultEndpointsProtocol=https;AccountName=<your-account-name>;AccountKey=<your-account-key>;EndpointSuffix=core.windows.net
```



### Local execution:
The token in the code that enables us to interact with Blob storage `expires every hour`, so it needs to be replaced with your current token before deploying containers to Azure in both the coordinator and the worker. As mentioned in the Azure tutorial, you can obtain a new token using this command:
```bash
az account get-access-token --resource https://storage.azure.com/ -o tsv --query accessToken
```
Our unit test `runtest.sh` will start the coordinator and the workers, providing results along with time statistics. You can modify the number of workers in the script. It takes approximately 3 minutes to execute on a MacBook Pro with the following specifications:
- 1,4 GHz Quad-Core Intel Core i5
- 8GB RAM
- 8 cores
- MacOS: 14.6.1 (23G93)
```bash
mkdir build & cd build
cmake ..
make
cd ..
chmod +x runTest.sh
./runTest.sh
```

### Docker conatiner deployement
Now we need to cross-compile the code for the Docker container. We encountered a problem: if the OS version or Linux distribution dependencies don't match those in the container, the code won't work. Additionally, compiling at runtime in Azure would increase runtime and make debugging more difficult.

Therefore, our solution is to cross-compile in another Docker container beforehand and then use these executables to create the coordinator and worker containers on Azure.

Cross compile:
```bash
mdkir cmake-build-debug
./cross_compile_push_to_azure.sh 
```
and then deploy either locally:

```bash
./deploy [number of workers]
```
or on azure:
```bash
./deploy_conatainers_azure.sh [number of workers]
```







