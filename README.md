
# Table of Contents

1.  [bus3 - buckup to S3](#org5b4d2b3)
    1.  [Overview](#org3e5e1b1)
    2.  [Getting started](#orge0a9a39)
        1.  [Prerequisites](#org0230581)
        2.  [Installation](#org065d0c9)
        3.  [FYI; Postgres config for Fedora/CentOS](#org8616237)
        4.  [Configuration file](#org8a6e601)
        5.  [Usage](#org2ad2a5c)
2.  [License](#orgb4a82ce)
3.  [Contact](#org6b1bfe1)
4.  [Acknowledgements](#org03e6acb)



<a id="org5b4d2b3"></a>

# bus3 - buckup to S3

`bus3.py` is a backup tool to S3 storage.  It fully utilizes `asyncio` to maximize concurrency.  It relies on `aiofiles`, `asyncpg` and `aioboto3` libraries.

**Important notice** - bus3 is still under development (experimental) and may or may not work for now.  


<a id="org3e5e1b1"></a>

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


<a id="orge0a9a39"></a>

## Getting started


<a id="org0230581"></a>

### Prerequisites

-   S3 storage
    -   **Not tested with Amazon AWS S3 (yet)**
-   Linux
    -   Developed on Fedora 33 and CentOS 8
-   Python 3.8 or later
-   bus3.py - the backup tool
-   bus3.yaml - config file
-   Maybe need root priviledge to execute


<a id="org065d0c9"></a>

### Installation

1.  Prepare S3 storage and a dedicated bucket for bus3.py
2.  Setup python 3.8 or later
3.  Setup Postgres and create a database named `bus3`
4.  Install aiofiles
5.  Install aioboto3=8.3.0 (latest 9.0 doesn't work???)
6.  Install asyncpg
7.  Install pyyaml
8.  Edit bus3.yaml for S3 storage endpoint, bucket name and directory to backup
9.  Setup `~/.aws/credentials` (eg, aws cli)
10. Run `python bus3.py -b` to backup


<a id="org8616237"></a>

### FYI; Postgres config for Fedora/CentOS

<https://fedoraproject.org/wiki/PostgreSQL#Installation>

1.  sudo dnf install postgresql-server
2.  sudo vi /var/lib/pgsql/data/pg\_hba.conf

    host    all             all             127.0.0.1/32            md5

1.  sudo postgresql-setup &#x2013;initdb
2.  sudo systemctl start postgresql
3.  sudo su - postgres
4.  createdb bus3
5.  psql

    ALTER USER postgres PASSWORD '<db-password>';


<a id="org8a6e601"></a>

### Configuration file

bus3.yaml is the configuration file.

    root_dir: /<path-to-backup-directory>
    s3_config:
      s3_bucket: <bucket name>
      s3_endpoint: https://<S3-storage-URL>:<port>


<a id="org2ad2a5c"></a>

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

Important: Please make sure to backup database after backup files/directories with bus3.py.


<a id="orgb4a82ce"></a>

# License

bus3.py is under [MIT license](https://en.wikipedia.org/wiki/MIT_License).


<a id="org6b1bfe1"></a>

# Contact

Kyosuke Achiwa - @kyos\_achwan - achiwa912+gmail.com (please replace `+` with `@`)

Project Link: <https://github.com/achiwa912/bus3>


<a id="org03e6acb"></a>

# Acknowledgements

TBD

