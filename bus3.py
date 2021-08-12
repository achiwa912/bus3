import os
import errno
import sys
import io
import asyncio
import logging
import signal
import hashlib
import datetime
import argparse
from enum import Enum
from pathlib import Path
import contextlib

import yaml
import aiofiles
import aiofiles.os
import aioboto3
import asyncpg

config = {
    'db_endpoint': 'postgresql://postgres@127.0.0.1/bus3',
    'chunksize': 64*1024*1024,  # max object chunk size in S3 (64MB)
    # 'chunksize': 4*1024*1024,  # max object chunk size in S3 (4MB; for testing)
    'buffersize': 256*1024,  # buffer size for hash calculation (256KB)
    's3_max': 256,  # max number of S3 tasks
    'db_max': 256,  # max number of db tasks
    'lb_max': 16,  # max number of tasks using large buffers
    's3_pool_size': 128,  # S3 client pool size
    'restore_max': 256,  # max concurrent restore tasks
    'db_timeout': 180,  # timeout value
    'db_password': 'bus3pass',
    # global temp variables from here:
    'scan_counter': 1,  # initial value
    'root_dir': None,  # backup root directory (will be overwritten)
    'large_buffers': 0,  # Number of large buffers (up to chunksize) being used
    'runmode': 0,  # 0: list history, 1: backup, 2: restore, 3: restore database
    'dbrestore_rel': 0,  # relative number from latest backed-up database file
    'restore_target': None,  # file or folder to restore
    'restore_to': None,  # restore to directory
    'restore_version': 0,  # optional restore version
    'num_tasks': 0,  # number of tasks
    'processed_files': 0,  # number of processed files
    'processed_size': 0,  # total size of processed files
    'start_time': 0,
    'end_time': 0,
    'db_pool': None,  # database connection pool
    's3_pool': [],  # S3 client pool
}
processing_db = []  # list of paths to files/dirs
processing_s3 = []  # list of paths to files
task_list = []  # task list
hardlink_dict = {}  # dict of hard links (fsid, inode): <path> or None

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s,%(msecs)d %(levelname)s: %(message)s",
    datefmt="%H:%M:%S",
)


class Kind(Enum):
    FILE, DIRECTORY, SYMLINK = range(3)


class RunMode(Enum):
    LIST_HISTORY, BACKUP, RESTORE, RESTORE_DB = range(4)


async def set_dirent_version(path, parent, fsid, stat, kind):
    """
    Set dirent and version tables
    Return:
        dirent_row_id: dirent id
        version_row_id: version id if created.  -1 if not
        contents_changed: True if file contents changed
        is_hardlink: True if it's a hard link
    """
    async with config['db_pool'].acquire() as db:

        is_hardlink = False  # hard link flag
        # dirent table
        dirent_row = await db.fetchrow(
            "SELECT * FROM dirent WHERE fsid=$1 AND inode=$2",
            fsid, stat.st_ino)
        if not dirent_row:
            dirent_row_id = await db.fetchval(
                "INSERT INTO dirent (is_deleted, type, fsid, inode, scan_counter) VALUES (0, $1, $2, $3, $4) RETURNING id",
                kind.name, fsid, stat.st_ino, config['scan_counter'])
        else:
            dirent_row_id = dirent_row[0]
            if dirent_row[5] == config['scan_counter']:
                is_hardlink = True
            else:
                await db.execute(
                    "UPDATE dirent SET is_deleted = 0, scan_counter = $1 WHERE id = $2",
                    config['scan_counter'], dirent_row_id)

        # version table
        version_row_id = -1
        contents_changed = False
        link_path = ""
        if kind == Kind.SYMLINK:
            link_path = os.readlink(path)
        version_row = await db.fetchrow(
            "SELECT * FROM version WHERE dirent_id=$1 ORDER BY id DESC",
            dirent_row_id)
        if not version_row or is_hardlink:
            xattrdic = {}
            names = os.listxattr(path, follow_symlinks=False)
            for name in names:
                xattrdic[name] = os.getxattr(path, name, follow_symlinks=False)
            version_row_id = await db.fetchval(
                "INSERT INTO version (is_delmarker, name, size, ctime, mtime, atime, permission, uid, gid, link_path, xattr, dirent_id, scan_counter, parent_id, is_hardlink) VALUES (0, $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14) RETURNING id",
                os.path.basename(path), stat.st_size,
                datetime.datetime.fromtimestamp(stat.st_ctime),
                datetime.datetime.fromtimestamp(stat.st_mtime),
                datetime.datetime.fromtimestamp(stat.st_atime),
                stat.st_mode, stat.st_uid, stat.st_gid, link_path,
                str(xattrdic), dirent_row_id,
                config['scan_counter'], parent, is_hardlink)
            contents_changed = True
        elif version_row[4] != datetime.datetime.fromtimestamp(stat.st_ctime) \
                or version_row[5] != datetime.datetime.fromtimestamp(stat.st_mtime):
            xattrdic = {}
            names = os.listxattr(path, follow_symlinks=False)
            for name in names:
                xattrdic[name] = os.getxattr(path, name, follow_symlinks=False)
            version_row_id = await db.fetchval(
                "INSERT INTO version (is_delmarker, name, size, ctime, mtime, atime, permission, uid, gid, link_path, xattr, dirent_id, scan_counter, parent_id, is_hardlink) VALUES (0, $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14) RETURNING id",
                os.path.basename(path), stat.st_size,
                datetime.datetime.fromtimestamp(stat.st_ctime),
                datetime.datetime.fromtimestamp(stat.st_mtime),
                datetime.datetime.fromtimestamp(stat.st_atime),
                stat.st_mode, stat.st_uid, stat.st_gid, link_path,
                str(xattrdic), dirent_row_id,
                config['scan_counter'], parent, is_hardlink)
            if version_row[5] != stat.st_mtime:  # contents changed?
                contents_changed = True
        if is_hardlink:
            await db.execute("UPDATE version SET is_hardlink=True WHERE dirent_id=$1", dirent_row_id)
    return dirent_row_id, version_row_id, contents_changed, is_hardlink


async def write_to_s3(chunk_index, file_path, object_hash, size, contents):
    """Create an S3 object

    Args:
        chunk_index: (if file size > chunk size)
        file_path
        object_hash: will be object key name
        size: object size
        contents: full contents if size <= buffer size 
                  (ie, don't need to read file again)
    """
    processing_s3.append(file_path)
    while not config['s3_pool']:
        await asyncio.sleep(0.5)
    s3 = config['s3_pool'].pop()

    if size <= config['buffersize']:
        view = memoryview(contents)
        fo = io.BytesIO(view)
        await s3.upload_fileobj(fo, config['s3_bucket'], object_hash)
        config['s3_pool'].append(s3)  # put S3 client back to pool
        processing_s3.remove(file_path)
        del view
        del contents
        config['num_tasks'] -= 1
        return

    while config['large_buffers'] >= config['lb_max']:
        await asyncio.sleep(1)
    config['large_buffers'] += 1
    logging.info(f"Grab a large buffer: {size} for {file_path}:{chunk_index}")
    large_buffer = bytearray(size)
    view = memoryview(large_buffer)
    async with aiofiles.open(file_path, mode='rb') as f:
        await f.seek(chunk_index * config['chunksize'])
        await f.readinto(view)
        fo = io.BytesIO(view)
        await s3.upload_fileobj(fo, config['s3_bucket'], object_hash)
    del view
    del large_buffer
    config['large_buffers'] -= 1
    config['s3_pool'].append(s3)  # put S3 client back to pool
    processing_s3.remove(file_path)
    logging.info(
        f"Done chunk s3 write: {file_path}:{chunk_index} (db:{len(processing_db)},s3:{len(processing_s3)})")
    config['num_tasks'] -= 1


async def process_file(path, parent, fsid, islink):
    """Process a file.

    Args:
        path: path to file
        parent: parent directory version id
        fsid: filesystem id
        islink: True if symbolic link
    """
    processing_db.append(path)
    logging.info(
        f"Processing file started: (db:{len(processing_db)},s3:{len(processing_s3)})")
    stat = await aiofiles.os.stat(path, follow_symlinks=False)
    if islink:  # symbolic link
        await set_dirent_version(path, parent, fsid, stat, Kind.SYMLINK)
        processing_db.remove(path)
        logging.info(f"Processed symlink: {path}")
        cofnfig['num_tasks'] -= 1
        return

    version_row_id, contents_changed = 1, True
    _, version_row_id, contents_changed, is_hardlink = \
        await set_dirent_version(path, parent, fsid, stat, Kind.FILE)
    if not contents_changed or is_hardlink:  # no update to file contents?
        if is_hardlink:
            logging.info(f"hard link for file: {path}")
        processing_db.remove(path)
        config['num_tasks'] -= 1
        return

    chunksize = config['chunksize']
    bufsize = config['buffersize']
    async with aiofiles.open(path, mode='rb') as f:
        chunk_index = 0
        eof = False  # end of file
        while True:  # create chunks
            hash_val = hashlib.sha256()
            size = 0
            contents = prev_contents = b''
            logging.info(f"Calc hash started: {path}:{chunk_index}")
            while size < chunksize:  # calculate hash for the file or up to chunk size
                contents = await f.read(bufsize)
                size += len(contents)
                if not contents:
                    eof = True
                    break
                hash_val.update(contents)
                prev_contents = contents
            logging.info(f"Calc hash end: {path}:{chunk_index}")
            object_hash = hash_val.hexdigest()
            async with config['db_pool'].acquire() as db:
                ver_object_row = await db.fetchrow("SELECT * FROM ver_object WHERE object_hash=$1", object_hash)
                # find an ver_object_row -> same content object is in S3
                if not ver_object_row and size != 0:
                    await db.execute(
                        "INSERT INTO ver_object (ver_id, object_hash) VALUES ($1, $2)",
                        version_row_id, object_hash)
            if not ver_object_row and size != 0:
                config['num_tasks'] += 1
                logging.info(f"Invoke S3 write - {path}:{chunk_index}")
                task = asyncio.create_task(
                    write_to_s3(chunk_index, path, object_hash, size,
                                prev_contents))
                task_list.append(task)
            if eof:
                break
            chunk_index += 1
    processing_db.remove(path)
    logging.info(
        f"Processed file: (db:{len(processing_db)},s3:{len(processing_s3)})")
    config['processed_files'] += 1
    config['processed_size'] += stat.st_size
    config['num_tasks'] -= 1


async def process_dir(path, parent):
    """Process a directory.

    Args:
        path: directory path name
        parent: parent version row id (-1 if top)
    """
    # Check if it's in the DB and if updated
    processing_db.append(path)
    fsid = str(os.statvfs(path).f_fsid)  # not async
    stat = await aiofiles.os.stat(path)
    _, version_row_id, _, is_hardlink = \
        await set_dirent_version(path, parent, fsid, stat, Kind.DIRECTORY)
    processing_db.remove(path)
    if is_hardlink:
        logging.info(f"hard link for dir: {path}")
        config['num_tasks'] -= 1
        return

    # create tasks for dirs and files in the directory
    for dent in os.scandir(path):
        while len(processing_db) > config['db_max'] \
                or len(processing_s3) > config['s3_max']:
            await asyncio.sleep(1)

        if dent.is_file(follow_symlinks=False):
            config['num_tasks'] += 1
            while config['num_tasks'] > config['db_max']:
                await asyncio.sleep(2)
            task = asyncio.create_task(
                process_file(dent.path, version_row_id, fsid, False))
            task_list.append(task)
        elif dent.is_dir(follow_symlinks=False):
            task = asyncio.create_task(
                process_dir(dent.path, version_row_id))
            task_list.append(task)
        elif dent.is_symlink():
            task = asyncio.create_task(
                process_file(dent.path, version_row_id, fsid, True))
            task_list.append(task)
        else:
            logging.info(f"Not file or dir: {dent.path}  Skipped")
    logging.info(f"Processed dir: {path}")
    config['num_tasks'] -= 1


async def shutdown(signal, loop):
    """Cleanup tasks."""
    logging.error(f"Received exit signal {signal.name}...")
    tasks = [t for t in asyncio.all_tasks() if t is not
             asyncio.current_task()]
    [task.cancel() for task in tasks]
    logging.info(f"Cancelling {len(tasks)} outstanding tasks")
    await asyncio.gather(*tasks, return_exceptions=True)
    loop.stop()


async def check_s3():
    """
    Check if can S3 bucket
    Return True/False
    """
    try:
        async with aioboto3.resource(
                's3', endpoint_url=config['s3_endpoint'], verify=False) as s3:
            await s3.meta.client.head_bucket(Bucket='bus3')
    except:
        logging.error(f"Can't connect to S3 or bucket bus3 doesn't exist.")
        return False
    return True


async def check_db():
    """
    Check if database file exists
    Return True/False
    """
    db = None
    try:
        db = await asyncpg.connect(
            config['db_endpoint'], password=config['db_password'],
            command_timeout=config['db_timeout'])
        val = await db.fetchval(
            "SELECT datname FROM pg_catalog.pg_database WHERE datname = 'bus3'")
        if val:
            db.close()
            return True
    except:
        pass
    if db:
        db.close()
    return False


async def async_backup():
    """
    asynchronous backup main task
    Create database tables, kick the scan, wait for all tasks
    and mark deleted files
    """
    healthy = await check_s3()
    if not healthy:
        return

    # create database connection pool
    config['db_pool'] = await asyncpg.create_pool(
        config['db_endpoint'], password=config['db_password'],
        command_timeout=config['db_timeout'])

    # create S3 client pool
    context_stack = contextlib.AsyncExitStack()
    for _ in range(config['s3_pool_size']):
        s3 = await context_stack.enter_async_context(
            aioboto3.client(
                's3', endpoint_url=config['s3_endpoint'],
                verify=False))
        config['s3_pool'].append(s3)

    async with config['db_pool'].acquire() as db:
        async with db.transaction():
            await db.execute("""CREATE TABLE IF NOT EXISTS dirent (
            id SERIAL PRIMARY KEY,
            is_deleted integer NOT NULL,
            type text NOT NULL,
            fsid text NOT NULL,
            inode integer NOT NULL,
            scan_counter bigint NOT NULL
            );""")
            await db.execute("CREATE INDEX IF NOT EXISTS Dentidx1 ON dirent(fsid, inode);")
            await db.execute("""CREATE TABLE IF NOT EXISTS version (
            id SERIAL PRIMARY KEY,
            is_delmarker integer NOT NULL,
            name text NOT NULL,
            size bigint NOT NULL,
            ctime timestamp NOT NULL,
            mtime timestamp NOT NULL,
            atime timestamp NOT NULL,
            permission integer NOT NULL,
            uid integer NOT NULL,
            gid integer NOT NULL,
            link_path text,
            xattr text,
            dirent_id integer NOT NULL,
            scan_counter bigint NOT NULL,
            parent_id integer NOT NULL,
            is_hardlink bool NOT NULL,
            FOREIGN KEY (dirent_id) REFERENCES dirent (id)
            );""")
            await db.execute("CREATE INDEX IF NOT EXISTS Veridx1 ON version(dirent_id);")
            await db.execute("""CREATE TABLE IF NOT EXISTS ver_object (
            id SERIAL PRIMARY KEY,
            ver_id integer NOT NULL,
            object_hash text NOT NULL,
            FOREIGN KEY (ver_id) REFERENCES version (id)
            );""")
            await db.execute("CREATE INDEX IF NOT EXISTS Voidx1 ON ver_object(id, ver_id);")
            await db.execute("CREATE INDEX IF NOT EXISTS Voidx2 ON ver_object(object_hash);")
            await db.execute("""CREATE TABLE IF NOT EXISTS scan (
            scan_counter bigint PRIMARY KEY,
            start_time timestamp NOT NULL,
            root_dir text NOT NULL
            );""")
            maxsc = await db.fetchval("SELECT MAX(scan_counter) FROM dirent;")
            if maxsc or maxsc == 0:
                config['scan_counter'] = maxsc + 1
            logging.info(f"scan_counter: {config['scan_counter']}")
            await db.execute("INSERT INTO scan (scan_counter, start_time, root_dir) VALUES ($1, $2, $3)", config['scan_counter'], datetime.datetime.now(), config['root_dir'])
    task = asyncio.create_task(process_dir(config['root_dir'], -1))
    task_list.append(task)
    await asyncio.sleep(1)  # wait a while for task_list to be populated
    while config['num_tasks'] > 0:
        logging.info(f"Waiting tasks: {config['num_tasks']}")
        await asyncio.sleep(1)
    # await asyncio.sleep(1)  # wait a while for task_list to be populated
    await asyncio.gather(*task_list)

    # Take care of deleted files and directories
    async with config['db_pool'].acquire() as db:

        # mark dirent as deleted
        dent_rows = await db.fetch("SELECT id FROM dirent WHERE is_deleted = 0 AND scan_counter < $1", config['scan_counter'])
        for dent_row in dent_rows:
            logging.info(f"{dent_row}")
            await db.execute("UPDATE dirent SET is_deleted = 1 WHERE id = $1", dent_row[0])

            # Insert delete marker version
            r = await db.fetchrow("SELECT * FROM version WHERE dirent_id=$1 ORDER BY id DESC", dent_row[0])
            if r[1] != 1:  # is_delmarker
                logging.info(f"delete row - {r}")
                await db.execute("INSERT INTO version (is_delmarker, name, size, ctime, mtime, atime, permission, uid, gid, dirent_id, scan_counter, parent_id, is_hardlink) VALUES (1, $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)", r[2], r[3], r[4], r[5], r[6], r[7], r[8], r[9], r[12], config['scan_counter'], r[14], r[15])

    """
    # backup db file to S3
    async with aioboto3.client(
            's3', endpoint_url=config['s3_endpoint'], verify=False) as s3:
        obj_name = '_'.join(
            [config['db_endpoint'], str(config['scan_counter'])])
        await s3.upload_file(
            config['db_endpoint'], config['s3_bucket'], obj_name)
    """
    await asyncio.sleep(1)
    for s3 in config['s3_pool']:
        await s3.close()


async def async_list():
    """
    asynchronous task to list backup history
    """
    healthy = await check_db()
    if not healthy:
        return

    # create database connection pool
    config['db_pool'] = await asyncpg.create_pool(
        config['db_endpoint'], password=config['db_password'],
        command_timeout=config['db_timeout'])

    async with config['db_pool'].acquire() as db:
        rows = await db.fetch("SELECT * FROM scan")
        print(f"  #: {'date & time'.ljust(19)} backup root directory")
        for row in rows:
            print(f"{row[0]:3d}: {str(row[1])[:19]} {row[2]}")


async def async_restoredb():
    """
    async task to restore database file from S3
    if an optional negative number is specified, bus3 will restore
    older versions
    """
    healthy = await check_s3()
    if not healthy:
        return

    async with aioboto3.resource(
            's3', endpoint_url=config['s3_endpoint'], verify=False) as s3:
        bucket = await s3.Bucket(config['s3_bucket'])
        dbbkup_objs = bucket.objects.filter(
            Prefix=config['db_endpoint'])
        sorted_dbbkups = []
        async for name in dbbkup_objs:
            logging.info(f"name: {str(name)}")
            name = name.key
            sorted_dbbkups.append(name[len(config['db_endpoint'])+1:])
    sorted_dbbkups.sort()
    logging.info(f"sorted_dbbkups - {sorted_dbbkups}")
    try:
        file_name = '_'.join([config['db_endpoint'],
                              sorted_dbbkups[config['dbrestore_rel']-1]])
    except:
        logging.error(f"No such database backup file version.")
        return
    async with aioboto3.client(
            's3', endpoint_url=config['s3_endpoint'], verify=False) as s3:
        try:
            await s3.download_file(
                config['s3_bucket'], file_name, config['db_endpoint'])
            logging.info(f"restored databae: {file_name}")
        except:
            logging.error(f"Can't download {file_name}")


async def restore_obj(restore_to, dent_id, ver_id, parent_id, kind):
    """
    async task to restore a file/directory/symlink version
    """
    # get database info
    processing_db.append(ver_id)
    async with config['db_pool'].acquire() as db:
        dent_row = await db.fetchrow(
            "SELECT * FROM dirent WHERE id=$1", dent_id)
        ver_row = await db.fetchrow(
            "SELECT * FROM version WHERE id=$1", ver_id)
        is_hardlink = ver_row[15]
        process_hardlink = False
        fsid_inode = (dent_row[3], dent_row[4])
        if is_hardlink:
            logging.info(f"hlink proc: {ver_row[2]} {hardlink_dict}")
            if fsid_inode in hardlink_dict.keys():
                process_hardlink = True  # skip restore as it's a hard link
            else:
                hardlink_dict[fsid_inode] = None
        verobjs = await db.fetch(
            "SELECT * FROM ver_object WHERE ver_id=$1 ORDER BY id", ver_id)
        if kind == Kind.DIRECTORY and not is_hardlink:
            # Get children to dispatch
            children_rows = await db.fetch("SELECT d.id, v.id, v.name, v.parent_id, d.type, v.is_delmarker, MAX(v.scan_counter) FROM dirent d JOIN version v ON d.id=v.dirent_id WHERE (SELECT dirent_id FROM version WHERE id = v.parent_id) = $1 AND v.scan_counter <= $2 GROUP BY v.name, d.id, v.id ORDER BY v.scan_counter DESC", dent_id, config['restore_version'])
    processing_db.remove(ver_id)

    # download file contents
    fpath = os.path.join(restore_to, ver_row[2])
    if kind == Kind.FILE and not process_hardlink:
        # logging.info(f"fpath: {fpath}")
        remaining_size = ver_row[3]  # file size
        async with aiofiles.open(fpath, mode='wb') as f:
            # download file contents
            processing_s3.append(ver_id)
            async with aioboto3.client(
                    's3', endpoint_url=config['s3_endpoint'],
                    verify=False) as s3:
                # logging.info(
                #    f"S3 loading bucket {config['s3_bucket']} to {fpath}")
                bufsize = remaining_size
                if remaining_size > config['chunksize']:
                    bufsize = config['chunksize']
                large_flag = False
                if bufsize >= config['chunksize']//16:
                    large_flag = True
                    while config['large_buffers'] >= config['lb_max']:
                        await asyncio.sleep(1)
                    config['large_buffers'] += 1
                large_buffer = bytearray(bufsize)
                view = memoryview(large_buffer)
                for verobj in verobjs:
                    #logging.info(f"verobj: {verobj[2]}")
                    fi = io.BytesIO(view)
                    await s3.download_fileobj(
                        config['s3_bucket'], verobj[2], fi)
                    fi.seek(0)
                    if remaining_size <= bufsize:
                        await f.write(fi.read(remaining_size))
                    else:
                        await f.write(fi.read(bufsize))
                    remaining_size -= bufsize
                del view
                del large_buffer
                if large_flag:
                    config['large_buffers'] -= 1
            processing_s3.remove(ver_id)
    elif kind == Kind.DIRECTORY and not process_hardlink:
        # logging.info(f"mkdir {fpath}")
        try:
            await aiofiles.os.mkdir(fpath, ver_row[7])
        except FileExistsError:
            pass
    elif process_hardlink:  # hard link
        while True:
            if hardlink_dict[fsid_inode]:
                break
            await asyncio.sleep(0.2)
        #logging.info(f"hard link: {hardlink_dict[fsid_inode]}, {fpath}")
        os.link(hardlink_dict[fsid_inode], fpath)
    else:  # Kind.SYMLINK
        # logging.info(f"Creating symlink: {fpath} to {ver_row[10]}")
        try:
            os.symlink(ver_row[10], fpath)
        except OSError as e:
            if e.errno == errno.EEXIST:
                os.remove(fpath)
                os.symlink(ver_row[10], fpath)

    if not process_hardlink:
        # set file/directory attributes
        if kind != Kind.SYMLINK:
            # This will cause an exception for a symlink
            os.chmod(fpath, ver_row[7])
        os.chown(fpath, ver_row[8], ver_row[9], follow_symlinks=False)
        os.utime(fpath, (datetime.datetime.timestamp(ver_row[5]),
                         datetime.datetime.timestamp(ver_row[6])),
                 follow_symlinks=False)
        xattr_dict = eval(ver_row[11])
        for k, v in xattr_dict.items():
            os.setxattr(fpath, k, v, follow_symlinks=False)

        # dispatch children tasks
        if kind == Kind.DIRECTORY:
            while len(processing_db) > config['db_max'] \
                    or len(processing_s3) > config['s3_max']:
                await asyncio.sleep(1)

            # logging.info(f"Will dispatch children: {children_rows}")
            for child_row in children_rows:
                if child_row[5] == 1:  # is_delmarker
                    continue
                while config['num_tasks'] >= config['restore_max']:
                    await asyncio.sleep(1)
                logging.info(f"Dispatching child: {child_row[2]}")
                config['num_tasks'] += 1
                task = asyncio.create_task(restore_obj(
                    fpath, child_row[0],
                    child_row[1], child_row[3], Kind[child_row[4]]))
                task_list.append(task)

        if is_hardlink:
            #logging.info(f"hlink dict set: {fpath}")
            hardlink_dict[fsid_inode] = fpath

    if kind == Kind.FILE and not process_hardlink:
        config['processed_files'] += 1
        config['processed_size'] += ver_row[3]  # file size
    config['num_tasks'] -= 1


async def async_restore():
    """
    async task to restore files and directories
    """
    healthy_db = await check_db()
    healthy_s3 = await check_s3()
    if not healthy_db or not healthy_s3:
        logging.info(f"db or s3 not ready {healthy_db} {healthy_s3}")
        return

    # create database connection pool
    config['db_pool'] = await asyncpg.create_pool(
        config['db_endpoint'], password=config['db_password'],
        command_timeout=config['db_timeout'])

    # Check restore-to directory
    if not os.path.isdir(config['restore_to']):
        logging.error(
            f"{config['restore_to']} directory doesn't exist.  aborting.")
        return

    logging.info(f"restore-to: {config['restore_to']}")

    async with config['db_pool'].acquire() as db:
        restore_target = config['restore_target']

        # convert restore_target to relative from root_dir
        row = await db.fetchrow(
            "SELECT root_dir FROM scan ORDER BY scan_counter DESC;")
        if config['restore_target'].startswith(row[0]):
            restore_target = restore_target.replace(row[0], '')
        if config['restore_target'] == 'all':
            restore_target = ''

        logging.info(
            f"restore-target: {config['restore_target']} ({restore_target})")

        # traverse path
        plist = restore_target.split('/')
        parent_id = -1  # root_dir
        row = await db.fetchrow(
            "SELECT d.id, v.id, d.type FROM dirent d JOIN version v ON d.id=v.dirent_id WHERE v.parent_id=$1 AND v.scan_counter<=$2 ORDER BY v.scan_counter DESC;", parent_id, config['restore_version'])
        for pitem in plist[:-1]:
            parent_id = row[0]
            row = await db.fetchrow(
                "SELECT d.id, v.id, d.type FROM dirent d JOIN version v ON d.id=v.dirent_id WHERE v.parent_id=$1 AND v.scan_counter<=$2 ORDER BY v.scan_counter DESC;", parent_id, config['restore_version'])
            logging.info(f"row tup - {row}")
            if not row:
                logging.error(
                    f"No such file or directory: {config['restore_target']}")
                return
        dirent_id, version_id, kind = row
        kind = Kind[kind]  # convert to Enum.Kind
        #logging.info(f"dent {dirent_id}, ver {version_id}, kind {kind}")

        task = asyncio.create_task(
            restore_obj(config['restore_to'], dirent_id, version_id,
                        parent_id, kind))
        task_list.append(task)
        await asyncio.sleep(1)
        while config['num_tasks'] > 0:
            logging.info(f"Waiting tasks: {config['num_tasks']}")
            await asyncio.sleep(1)
        # await asyncio.sleep(1)
        await asyncio.gather(*task_list)
        # await asyncio.sleep(1)


def main():
    """
    Parse command line, read config file and start event loop
    """
    config['start_time'] = datetime.datetime.now()
    parser = argparse.ArgumentParser(
        description='Backup to S3 storage')
    group = parser.add_mutually_exclusive_group()
    group.add_argument('-l', '--list', action='store_true',
                       help='list backup history')
    group.add_argument('-b', '--backup', action='store_true',
                       help='backup directory specified in bus3.yaml')
    group.add_argument('-r', '--restore', nargs='*',
                       help='restore all|<directory/file-to-restore> to <directory-to-restore-to>')
    group.add_argument('-R', '--restore_db', nargs='?', const='0',
                       help='restore database file')
    args = parser.parse_args()
    #logging.info(f"{args}, {args.restore_db}")
    if args.backup:
        config['runmode'] = RunMode.BACKUP  # backup
    elif args.restore or args.restore == '0':
        config['runmode'] = RunMode.RESTORE  # restore
        if 2 <= len(args.restore) <= 3:
            config['restore_target'] = args.restore[0]
            config['restore_to'] = os.path.abspath(args.restore[1])
            if len(args.restore) == 3:
                config['restore_version'] = args.restore[2]
            else:
                config['restore_version'] = sys.maxsize
        else:
            print(
                f"Usage: bus3.py -r all|<directory/file-to-restore> <directory-to-restore-to> [<bakup history number>]")
            return
    elif args.restore_db or args.restore_db == '0':
        config['runmode'] = RunMode.RESTORE_DB  # restore database file
        #logging.info(f"restore_db - {args.restore_db}")
        try:
            config['dbrestore_rel'] = int(args.restore_db)
            assert(config['dbrestore_rel'] <= 0)
        except:
            print(
                f"Usage: bus3.py --restore_db <num from latest (0,-1..)>")
            return
        #logging.info(f"dbrestore_rel - {config['dbrestore_rel']}")
    else:
        config['runmode'] = RunMode.LIST_HISTORY  # list backup history

    conf_file = Path('bus3.yaml')
    if not conf_file.is_file():
        logging.error(f"bus3.yaml doesn't exist.  aborting.")
        return
    with open("bus3.yaml", 'r') as f:
        loaded = yaml.safe_load(f)
    config.update(loaded['s3_config'])
    config['root_dir'] = loaded['root_dir']

    loop = asyncio.get_event_loop()
    signals = (signal.SIGHUP, signal.SIGTERM, signal.SIGINT)
    for s in signals:
        loop.add_signal_handler(
            s, lambda s=s: asyncio.create_task(shutdown(s, loop)))

    logging.info(f"runmode: {config['runmode'].name}")
    try:
        if config['runmode'] == RunMode.LIST_HISTORY:
            task = loop.create_task(async_list())
        elif config['runmode'] == RunMode.BACKUP:
            task = loop.create_task(async_backup())
        elif config['runmode'] == RunMode.RESTORE_DB:
            task = loop.create_task(async_restoredb())
        else:
            task = loop.create_task(async_restore())
        loop.run_until_complete(task)
    except KeyboardInterrupt:
        logging.info("Process interrupted")
    finally:
        loop.close()
        logging.info("Completed or gracefully terminated")
    config['end_time'] = datetime.datetime.now()
    elapsed_seconds = (config['end_time'] -
                       config['start_time']).total_seconds()
    print(
        f"Processed {config['processed_files']} files in {elapsed_seconds} seconds.")
    print(f" {config['processed_files']/elapsed_seconds} files/sec")
    print(f" {config['processed_size']/elapsed_seconds/1024/1024} MB/s")


if __name__ == "__main__":
    main()
