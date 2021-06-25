
# Table of Contents

1.  [bus3 - buckup to S3](#orgacf878d)
    1.  [Overview](#org054e0c8)
    2.  [Getting started](#org146452d)
        1.  [Prerequisites](#orgd38c9ab)
        2.  [Installation](#orgda9a5df)
        3.  [Configuration file](#orgd77a3b0)
        4.  [Usage](#orge34617d)
2.  [License](#orgb1d9f4e)
3.  [Contact](#orgd35389d)
4.  [Acknowledgements](#orge864668)



<a id="orgacf878d"></a>

# bus3 - buckup to S3

`bus3.py` is a backup tool to S3 storage.  It fully utilizes `asyncio` to maximize concurrency.  It relies on `aiofiles`, `aiosqlite` and `aioboto3` libraries.

**Important notice - bus3 is still under development (experimental) and may or may not work for now.  Especially, restore is not implemented yet!**


<a id="org054e0c8"></a>

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


<a id="org146452d"></a>

## Getting started


<a id="orgd38c9ab"></a>

### Prerequisites

-   S3 storage
    -   **Not tested with Amazon AWS S3 (yet)**
-   Linux
    -   Developed on Fedora 33
-   Python 3.8 or later
-   bus3.py - the backup tool
-   bus3.yaml - config file


<a id="orgda9a5df"></a>

### Installation

1.  Prepare S3 storage and a dedicated bucket for bus3.py
2.  Setup python 3.8 or later
3.  Install aiofiles
4.  Install aioboto3
5.  Install aiosqlite
6.  Edit bus3.yaml for S3 storage endpoint, bucket name and directory to backup
7.  Setup `~/.aws/credentials` (eg, aws cli)
8.  Run `python bus3.py -b` to backup


<a id="orgd77a3b0"></a>

### Configuration file

bus3.yaml is the configuration file.

    root_dir: /<path-to-backup-directory>
    s3_config:
      s3_bucket: <bucket name>
      s3_endpoint: https://<S3-storage-URL>:<port>


<a id="orge34617d"></a>

### Usage

To back up:

    python bus3.py -b

To see backup history/list:

    python bus3.py [-l]

To restore directory/file:

    python bus3.py -r all|<file/dierctory-to-restore> <directory-to-be-restored> [-n <backup-history-number>]

`<file/directory-to-restore>` can either be specified as a full path (ie, starts with `/`) or a relative path to the backup root directory sepcified in the `bus3.yaml`.  If `all` is specified, bus3 will restore all backup files and directories.

If `-n <backup-history-number>` is not specified, bus3 will restore the latest version.

Note:
If bus3.py doesn't find the database file (bus3.db) in the current directory when it performs a backup (`-b`), it will create a new one.  If bus3.py can't find the database file when it restores (`-r`) or loads backup history (`-l` or no option), it will try to restore the database file from S3 storage.


<a id="orgb1d9f4e"></a>

# License

bus3.py is under [MIT license](https://en.wikipedia.org/wiki/MIT_License).


<a id="orgd35389d"></a>

# Contact

Kyosuke Achiwa - @kyos\_achwan - achiwa912+gmail.com (please replace `+` with `@`)

Project Link: <https://github.com/achiwa912/bus3>


<a id="orge864668"></a>

# Acknowledgements

TBD

