
# Table of Contents

1.  [bus3 - buckup to S3](#org788b70b)
    1.  [Overview](#org23aad41)
    2.  [Getting started](#orgb29f1c8)
        1.  [Prerequisites](#orge06d251)
        2.  [Installation](#orga09f549)
        3.  [FYI; Postgres config for Fedora/CentOS](#org4f9ba88)
        4.  [Configuration file](#org3704590)
        5.  [Usage](#org052f2a0)
2.  [License](#orgfd831cc)
3.  [Contact](#orgdecdab7)
4.  [Acknowledgements](#orgaa911ae)
5.  [Appendix; Performance testing](#org2a75d86)
    1.  [Small random files (4KB)](#org789f56f)
    2.  [large random files (1 or 4GB)](#org65efd41)



<a id="org788b70b"></a>

# bus3 - buckup to S3

`bus3.py` is an experimental backup tool to S3 storage.  It fully utilizes `asyncio` to maximize concurrency with small footprint.  It relies on `aiofiles`, `asyncpg` and `aioboto3` libraries.

**Important notice** - bus3 is still under development (experimental) and may or may not work for now.  


<a id="org23aad41"></a>

## Overview

bus3 is designed to be able to:

-   backup files, directories and symbolic/hard links
-   preserve extended attributes
-   track backup history and file versions
-   perform file or chunk (default 64MB) level dedupe
-   backup very large files without using up all the memory
-   handle a large number of files without using up memory
-   maximize cuncurrency with asyncio (coroutines)
    -   spawn an async task for each file or directory to back up
    -   spawn an async task for each object write to S3
-   support PostgreSQL as opposed to sqlite3 to avoid the global write lock

bus3 splits large files into chunks and stores them as separate objects in S3 storage.  It stores file metadata in the database.  The database needs to be backed up separately after each backup.


<a id="orgb29f1c8"></a>

## Getting started


<a id="orge06d251"></a>

### Prerequisites

-   S3 storage
    -   **Not tested with Amazon AWS S3 (yet)**
-   Linux
    -   Developed on Fedora 33 and CentOS 8
-   Python 3.8 or later
-   bus3.py - the backup tool
-   bus3.yaml - config file
-   May need root priviledge to execute


<a id="orga09f549"></a>

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


<a id="org4f9ba88"></a>

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


<a id="org3704590"></a>

### Configuration file

bus3.yaml is the configuration file.

    root_dir: /<path-to-backup-directory>
    s3_config:
      s3_bucket: <bucket name>
      s3_endpoint: https://<S3-storage-URL>:<port>


<a id="org052f2a0"></a>

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

`<file/directory-to-restore>` can either be specified as a full path (ie, starts with `/`) or a relative path to the backup root directory sepcified in the `bus3.yaml`.  If `all` is specified, bus3 will restore all backup files and directories.  (Most tests specify `all` so far.)

If `<backup-history-number>` is not specified, bus3 will restore the latest version.

Important: Please make sure to backup database after each backup files/directories with bus3.py.


<a id="orgfd831cc"></a>

# License

bus3.py is under [MIT license](https://en.wikipedia.org/wiki/MIT_License).


<a id="orgdecdab7"></a>

# Contact

Kyosuke Achiwa - @kyos\_achwan - achiwa912+gmail.com (please replace `+` with `@`)

Project Link: <https://github.com/achiwa912/bus3>


<a id="orgaa911ae"></a>

# Acknowledgements

TBD


<a id="org2a75d86"></a>

# Appendix; Performance testing

Conducted performance test in a local environment with a locally connected S3 storage (ie, **NOT** Amazon AWS).


<a id="org789f56f"></a>

## Small random files (4KB)

Backed up and restored 1000 4KB random files in a directory.

<table border="2" cellspacing="0" cellpadding="6" rules="groups" frame="hsides">


<colgroup>
<col  class="org-right" />

<col  class="org-right" />

<col  class="org-right" />

<col  class="org-right" />

<col  class="org-right" />
</colgroup>
<tbody>
<tr>
<td class="org-right">S3 pool size</td>
<td class="org-right">max S3 tasks</td>
<td class="org-right">max DB tasks</td>
<td class="org-right">backup (files/sec)</td>
<td class="org-right">restore (files/sec)</td>
</tr>


<tr>
<td class="org-right">150</td>
<td class="org-right">150</td>
<td class="org-right">96</td>
<td class="org-right">45.2</td>
<td class="org-right">59.9</td>
</tr>


<tr>
<td class="org-right">150</td>
<td class="org-right">150</td>
<td class="org-right">150</td>
<td class="org-right">61.1</td>
<td class="org-right">59.1</td>
</tr>


<tr>
<td class="org-right">150</td>
<td class="org-right">150</td>
<td class="org-right">256</td>
<td class="org-right">60.9</td>
<td class="org-right">62.7</td>
</tr>


<tr>
<td class="org-right">256</td>
<td class="org-right">256</td>
<td class="org-right">256</td>
<td class="org-right">61.8</td>
<td class="org-right">59.5</td>
</tr>


<tr>
<td class="org-right">96</td>
<td class="org-right">256</td>
<td class="org-right">256</td>
<td class="org-right">65.8</td>
<td class="org-right">58.3</td>
</tr>


<tr>
<td class="org-right">64</td>
<td class="org-right">256</td>
<td class="org-right">256</td>
<td class="org-right">66.9</td>
<td class="org-right">63.0</td>
</tr>


<tr>
<td class="org-right">32</td>
<td class="org-right">256</td>
<td class="org-right">256</td>
<td class="org-right">63.9</td>
<td class="org-right">60.0</td>
</tr>


<tr>
<td class="org-right">16</td>
<td class="org-right">256</td>
<td class="org-right">256</td>
<td class="org-right">46.5</td>
<td class="org-right">59.4</td>
</tr>


<tr>
<td class="org-right">8</td>
<td class="org-right">256</td>
<td class="org-right">256</td>
<td class="org-right">37.9</td>
<td class="org-right">62.7</td>
</tr>
</tbody>
</table>


<a id="org65efd41"></a>

## large random files (1 or 4GB)

<table border="2" cellspacing="0" cellpadding="6" rules="groups" frame="hsides">


<colgroup>
<col  class="org-right" />

<col  class="org-right" />

<col  class="org-right" />

<col  class="org-right" />

<col  class="org-right" />
</colgroup>
<tbody>
<tr>
<td class="org-right">file size (GB)</td>
<td class="org-right">files</td>
<td class="org-right">max large buffers</td>
<td class="org-right">backup (MB/s)</td>
<td class="org-right">restore (MB/s)</td>
</tr>


<tr>
<td class="org-right">4</td>
<td class="org-right">2</td>
<td class="org-right">16</td>
<td class="org-right">57.57</td>
<td class="org-right">88.68</td>
</tr>


<tr>
<td class="org-right">1</td>
<td class="org-right">1</td>
<td class="org-right">16</td>
<td class="org-right">57.5</td>
<td class="org-right">92.53</td>
</tr>


<tr>
<td class="org-right">1</td>
<td class="org-right">2</td>
<td class="org-right">16</td>
<td class="org-right">55.15</td>
<td class="org-right">78.18</td>
</tr>


<tr>
<td class="org-right">1</td>
<td class="org-right">4</td>
<td class="org-right">16</td>
<td class="org-right">56.29</td>
<td class="org-right">88.63</td>
</tr>


<tr>
<td class="org-right">1</td>
<td class="org-right">8</td>
<td class="org-right">16</td>
<td class="org-right">56.8</td>
<td class="org-right">93.79</td>
</tr>


<tr>
<td class="org-right">1</td>
<td class="org-right">8</td>
<td class="org-right">32</td>
<td class="org-right">56.48</td>
<td class="org-right">90.69</td>
</tr>


<tr>
<td class="org-right">1</td>
<td class="org-right">16</td>
<td class="org-right">32</td>
<td class="org-right">54.73</td>
<td class="org-right">91.09</td>
</tr>
</tbody>
</table>

