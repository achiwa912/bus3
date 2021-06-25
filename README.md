
# Table of Contents

1.  [bus3 - buckup to S3](#org14bb7c0)
    1.  [Overview](#org10c7d44)
    2.  [Getting started](#orgab43aa1)
        1.  [Prerequisites](#orgac83ede)
        2.  [Installation](#org8dc4069)
        3.  [Configuration file](#org01e5a06)
        4.  [Usage](#orgcbaf2b7)
2.  [License](#org5266f9b)
3.  [Contact](#orgb055843)
4.  [Acknowledgements](#orga4bdfec)



<a id="org14bb7c0"></a>

# bus3 - buckup to S3

`bus3.py` is a backup tool to S3 storage.  It fully utilizes `asyncio` to maximize concurrency.  It relies on `aiofiles`, `aiosqlite` and `aioboto3` libraries.

**Important notice - bus3 is under development and may or may not work for now.  Especially, restore is not implemented yet!**


<a id="org10c7d44"></a>

## Overview

bus3 is designed so that it is supposed to be able to:

-   backup files, directories and symbolic links
-   preserve extended attributes
-   keep backup history
-   perform file or chunk (default 64MB) level dedupe
-   backup very large files w/o using up memory
-   handle a large number of files w/o using up memory
-   maximize cuncurrency with asyncio (coroutines)
    -   spawn an async task for each file or directory to back up
    -   spawn an async task for each object write to S3

bus3 splits large files into chunks and stores them as separate objects in S3 storage.  It stores file metadata in database.  The database is also backed up to S3 storage.


<a id="orgab43aa1"></a>

## Getting started


<a id="orgac83ede"></a>

### Prerequisites

-   S3 storage
    -   **Not tested with Amazon AWS S3 (yet)**
-   Linux
    -   Developed on Fedora 33
-   Python 3.8 or later
-   bus3.py - the backup tool
-   bus3.yaml - config file


<a id="org8dc4069"></a>

### Installation

1.  Prepare S3 storage and a dedicated bucket for bus3.py
2.  Setup python 3.8 or later
3.  Install aiofiles
4.  Install aioboto3
5.  Install aiosqlite
6.  Edit bus3.yaml for S3 storage endpoint, bucket name and directory to backup
7.  Setup `~/.aws/credentials` (eg, aws cli)
8.  Run `python bus3.py -b` to backup


<a id="org01e5a06"></a>

### Configuration file

bus3.yaml is the configuration file.

    root_dir: /<path-to-backup-directory>
    s3_config:
      s3_bucket: <bucket name>
      s3_endpoint: https://<S3-storage-URL>:<port>


<a id="orgcbaf2b7"></a>

### Usage

To back up:

To see backup history/list:

To restore directory/file:

`<file/directory-to-restore>` can either be specified as a full path (ie, starts with `/`) or a relative path to the backup root directory sepcified in the `bus3.yaml`.  If `all` is specified, bus3 will restore all backup files and directories.

If `-n <backup-history-number>` is not specified, bus3 will restore the latest version.

Note:
If bus3.py doesn't find the database file (bus3.db) in the current directory when it performs a backup (`-b`), it will create a new one.  If bus3.py can't find the database file when it restores (`-r`) or loads backup history (`-l` or no option), it will try to restore the database file from S3 storage.


<a id="org5266f9b"></a>

# License

bus3.py is under [MIT license](https://en.wikipedia.org/wiki/MIT_License).


<a id="orgb055843"></a>

# Contact

Kyosuke Achiwa - @kyos\_achwan - achiwa912+gmail.com (please replace `+` with `@`)

Project Link: <https://github.com/achiwa912/bus3>


<a id="orga4bdfec"></a>

# Acknowledgements

TBD

