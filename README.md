# AzFuse

AzFuse is a lightweight [blobfuse](https://github.com/Azure/azure-storage-fuse)-like
python tool with the data transfer
implemented through [AzCopy](https://github.com/Azure/azure-storage-azcopy).
With this tool, reading a file in azure storage is similar to reading a local
file, which is the same principle of blobfuse. However, the underlying data
transfer is to leverage azcopy, which provides a much faster speed. With this
python tool, large amount of files can be cached with one or a few azcopy calls
rather than to download each file sequentially. This would significantly reduce
the downloading time. Concurrent file reading is properly handled.
The tool also provides an asynchronize way to upload the
cache file to the storage for writing, in which case it does not block the main
process. 

## Installation
1. install azcopy to `~/code/azcopy/azcopy` or make the call of `azcopy` point
  to the correct azcopy path. azcopy can be downloaded from [here](https://docs.microsoft.com/en-us/azure/storage/common/storage-use-azcopy-v10).
  Make sure it is version 10 or higher.
2. install it by the following
```bash
pip install https://github.com/microsoft/azfuse.git
```

## Preliminary
Azfuse contains 3 different kinds of file paths.
1. `local` or `logical` path, which is populated by the user script. For example, the user
   script may want to access the file, named `data/abc.txt`, which is named as
   `local` path.
2. `remote` path, which is the path in azure
   storage blob. For example, if the azure storage path is 
   `https://accountname.blob.core.windows.net/containername/path/data/abc.txt`, the
   `remote` path will be `path/data/abc.txt`. Note that, the remote path does not
   include the `containername` in the url.
3. `cache` path, which is the destination file of the azcopy, e.g. `/tmp/data/abc.txt`.

The pipeline is
1. the user script tries to access `data/abc.txt`
2. if it is in read mode, the tool will check if the `cache` path exists.
    - if it exists, it returns the handle of the `cache` file
    - if it does not exist, it will download the file from `remote` path to
      `cache` path and return the handle of the `cache` file.
3. if it is in write mode, the tool will open the `cache` path, and return the
   handle of the `cache` path. After it finishes, the tool will upload the
   `cache` file to `remote` file.

## Setup
1. Set the environment variable of `AZFUSE_CLOUD_FUSE_CONFIG_FILE` as the
   configuration file path, e.g. `AZFUSE_CLOUD_FUSE_CONFIG_FILE=./aux_data/configs/azfuse.yaml`
2. The configuration file is in yaml format, and is a list of dictionary. Each
   dictionary contains `local`, `remote`, `cache`, and `storage_account`.
   ```yaml
   - cache: /tmp/azfuse/data
     local: data
     remote: azfuse_data
     storage_account: storage_config_name
   - cache: /tmp/azfuse/models
     local: models
     remote: models
     storage_account: storage_config_name
   ```
   The path in the yaml file is the prefix of the corresponding path. For example, if the
   local path is `data/abc.txt`, the `cache` path will be
   `/tmp/azfuse/data/abc.txt`, and the `remote` path will be
   `azfuse_data/abc.txt`. The tool will match each prefix from the first to the
   last, and the one which is matched first will be the one used.

   The storage account here is the base file name. Here, the path will be
   `./aux_data/storage_account/storage_config_name.yaml`. The folder can be
   changed by setting `AZFUSE_STORAGE_ACCOUNT_CONFIG_FOLDER`. The storage
   account yaml file's format should be like this
   ```yaml
   account_name: accountname
   account_key: accountkey
   sas_token: sastoken
   container_name: containername
   ```
   `account_key` or `sas_token` can be `null`. The `sas_token` should start from
   `?`.
3. Enable the feature by setting `AZFUSE_USE_FUSE=1`. By default, it is
   disabled, and the file access is the same as to access the local folder.

## Examples
- Open a file to read
  ```python
  from azfuse import File
  with File.open('data/abc.txt', 'r') as fp:
      content = fp.read()
  ```
  It will match the prefix of `local` path in the configuration file. If the
  cache file exists, it just returns the handle of the cache file. Otherwise,
  it will download the file from the `remote` path of the Azure Blob to the
  `cache` file, and then return the handle.

- Open a file to write
  ```python
  from azfuse import File
  with File.open('data/abc.txt', 'w') as fp:
       fp.write('abc')
  ```
  No matter whether there exists a cache file with the same name, it will open the
  cache file. Before it leaves `with`, it will upload the `cache` file to the
  `remote` file in the Azure Blob Storage.

- Pre-cache a bunch of files for processing
  ```python
  from azfuse import File
  File.prepare(['data/{}.txt'.format(i)] for i in range(1000))
  for i in range(1000):
      with File.open('data/{}.txt'.format(i), 'r') as fp:
         content = fp.read()
  ```
  The function of `prepare` will download all files in one azcopy call, which is much faster than download each file sequentially.
  As `prepare()` has already downloaded all the files to the cache folder, there
  will be no azcopy download when calling `File.open()`. 

- Upload the file in an asynchronous way.
  ```python
  from azfuse import File
  with File.async_upload(enabled=True):
     for i in range(1000):
         with File.open('data/{}.txt'.format(i), 'w') as fp:
              fp.write(str(i))
  ```
  A separate subprocess will be launched to upload the cache files. It will
  also upload multiple cache files at the same time in one azcopy call if there are.
  The cache file can also be re-directed to `/dev/shm` such that the file
  writing into cache files will be faster. It is enabled by `File.async_upload(enabled=True, shm_as_tmp=True)`
  In this case, the upload process
  will delete the cache file once it is uploaded.



## Contributing

This project welcomes contributions and suggestions.  Most contributions require you to agree to a
Contributor License Agreement (CLA) declaring that you have the right to, and actually do, grant us
the rights to use your contribution. For details, visit https://cla.opensource.microsoft.com.

When you submit a pull request, a CLA bot will automatically determine whether you need to provide
a CLA and decorate the PR appropriately (e.g., status check, comment). Simply follow the instructions
provided by the bot. You will only need to do this once across all repos using our CLA.

This project has adopted the [Microsoft Open Source Code of Conduct](https://opensource.microsoft.com/codeofconduct/).
For more information see the [Code of Conduct FAQ](https://opensource.microsoft.com/codeofconduct/faq/) or
contact [opencode@microsoft.com](mailto:opencode@microsoft.com) with any additional questions or comments.

## Trademarks

This project may contain trademarks or logos for projects, products, or services. Authorized use of Microsoft 
trademarks or logos is subject to and must follow 
[Microsoft's Trademark & Brand Guidelines](https://www.microsoft.com/en-us/legal/intellectualproperty/trademarks/usage/general).
Use of Microsoft trademarks or logos in modified versions of this project must not cause confusion or imply Microsoft sponsorship.
Any use of third-party trademarks or logos are subject to those third-party's policies.
