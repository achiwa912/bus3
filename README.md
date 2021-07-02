
# Table of Contents

1.  [bus3 - buckup to S3](#orgc8d5ed7)
    1.  [Overview](#org3c91971)
    2.  [Getting started](#org064c15d)
        1.  [Prerequisites](#org2cd3ef7)
        2.  [Installation](#orgb92cee0)
        3.  [Configuration file](#org72945a1)
        4.  [Usage](#org8d33daf)
2.  [License](#org9fd4f33)
3.  [Contact](#org7ac65b1)
4.  [Acknowledgements](#orgd8ed78a)



<a id="orgc8d5ed7"></a>

# bus3 - buckup to S3

`bus3.py` is a backup tool to S3 storage.  It fully utilizes `asyncio` to maximize concurrency.  It relies on `aiofiles`, `aiosqlite` and `aioboto3` libraries.

**Important notice - bus3 is still under development (experimental) and may or may not work for now.  Especially, restore is not implemented yet!**


<a id="org3c91971"></a>

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


<a id="org064c15d"></a>

## Getting started


<a id="org2cd3ef7"></a>

### Prerequisites

-   S3 storage
    -   **Not tested with Amazon AWS S3 (yet)**
-   Linux
    -   Developed on Fedora 33
-   Python 3.8 or later
-   bus3.py - the backup tool
-   bus3.yaml - config file


<a id="orgb92cee0"></a>

### Installation

1.  Prepare S3 storage and a dedicated bucket for bus3.py
2.  Setup python 3.8 or later
3.  Install aiofiles
4.  Install aioboto3
5.  Install aiosqlite
6.  Edit bus3.yaml for S3 storage endpoint, bucket name and directory to backup
7.  Setup `~/.aws/credentials` (eg, aws cli)
8.  Run `python bus3.py -b` to backup


<a id="org72945a1"></a>

### Configuration file

bus3.yaml is the configuration file.

    root_dir: /<path-to-backup-directory>
    s3_config:
      s3_bucket: <bucket name>
      s3_endpoint: https://<S3-storage-URL>:<port>


<a id="org8d33daf"></a>

### Usage

To back up:

    python bus3.py -b

To see backup history/list:

    python bus3.py [-l]

Example output:

    (bus3) [test@localhost bus3]$ python bus3.py -l
      #: date & time         backup root directory
      0: 2021-06-24 15:31:01 /home/test/py/bus3/test
      1: 2021-06-24 15:57:25 /home/test/py/bus3/test
      2: 2021-06-24 16:26:53 /home/test/py/bus3/test
      3: 2021-06-24 22:34:11 /home/test/py/bus3/test
      4: 2021-06-25 07:26:45 /home/test/py/bus3/test
      5: 2021-06-25 07:31:05 /home/test/py/bus3/test
      6: 2021-06-25 07:41:52 /home/test/py/bus3/test
    07:46:42,292 INFO: Completed or gracefully terminated

`#` is the backup history number (or scan counter)

To restore directory/file:

    python bus3.py -r all|<file/dierctory-to-restore> <directory-to-be-restored> [<backup-history-number>]

`<file/directory-to-restore>` can either be specified as a full path (ie, starts with `/`) or a relative path to the backup root directory sepcified in the `bus3.yaml`.  If `all` is specified, bus3 will restore all backup files and directories.

If `<backup-history-number>` is not specified, bus3 will restore the latest version.

To restore database file:

    python bus3.py --restore_database [<negative number>]

`<negative number>` is -1, -2, etc. and indicates relative version number from the latest.  For example, '-1' is the 2nd latest and '-2' is the 3rd latest.

Note:
If bus3.py doesn't find the database file (bus3.db) in the current directory when it performs a backup (`-b`), it will create a new one.


<a id="org9fd4f33"></a>

# License

bus3.py is under [MIT license](https://en.wikipedia.org/wiki/MIT_License).


<a id="org7ac65b1"></a>

# Contact

Kyosuke Achiwa - @kyos\_achwan - achiwa912+gmail.com (please replace `+` with `@`)

Project Link: <https://github.com/achiwa912/bus3>


<a id="orgd8ed78a"></a>

# Acknowledgements

TBD

