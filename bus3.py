import os
import errno
import sys
import io
import asyncio
import logging
import random
import signal
import hashlib
import uuid
import datetime
import yaml
import argparse
from enum import Enum
from pathlib import Path

import aiosqlite
import aiofiles
import aiofiles.os
import aioboto3

config = {
    'db_file': 'bus3.db',
    # 'chunksize': 64*1024*1024,  # max object chunk size in S3 (64MB)
    'chunksize': 4*1024*1024,  # max object chunk size in S3 (4MB; for testing)
    'buffersize': 256*1024,  # buffer size for hash calculation (256KB)
    's3_max': 100,  # max number of S3 tasks
    'db_max': 64,  # max number of db tasks
    'lb_max': 16,  # max number of tasks using large buffers
    'restore_max': 96,  # max concurrent restore tasks
    'sqlite_timeout': 180,  # timeout value
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
    'start_time': 0,
    'end_time': 0,
}
processing_db = []  # list of paths to files/dirs
processing_s3 = []  # list of paths to files
task_list = []  # task list

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
    """
    async with aiosqlite.connect(config['db_file'], timeout=config['sqlite_timeout']) as db:
        cur = await db.cursor()

        # dirent table
        await cur.execute(
            "SELECT * FROM dirent WHERE fsid=? AND inode=?",
            (fsid, stat.st_ino))
        dirent_row = await cur.fetchone()
        if not dirent_row:
            await cur.execute(
                "INSERT INTO dirent (id, is_deleted, type, fsid, inode, scan_counter) VALUES (?, 0, ?, ?, ?, ?)",
                (None, kind.name, fsid, stat.st_ino, config['scan_counter']))
            dirent_row_id = cur.lastrowid
        else:
            dirent_row_id = dirent_row[0]
            await cur.execute(
                "UPDATE dirent SET is_deleted = 0, scan_counter = ? WHERE id = ?",
                (config['scan_counter'], dirent_row_id))

        # version table
        version_row_id = -1
        contents_changed = False
        link_path = ""
        if kind == Kind.SYMLINK:
            link_path = os.readlink(path)
        await cur.execute(
            "SELECT * FROM version WHERE dirent_id=? ORDER BY id DESC",
            (dirent_row_id, ))
        version_row = await cur.fetchone()
        if not version_row:
            xattrdic = {}
            names = os.listxattr(path, follow_symlinks=False)
            for name in names:
                xattrdic[name] = os.getxattr(path, name, follow_symlinks=False)
            await cur.execute(
                "INSERT INTO version (id, is_delmarker, name, size, ctime, mtime, atime, permission, uid, gid, link_path, xattr, dirent_id, scan_counter, parent_id) VALUES (?, 0, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (None, os.path.basename(path), stat.st_size,
                 stat.st_ctime, stat.st_mtime, stat.st_atime,
                 stat.st_mode, stat.st_uid, stat.st_gid, link_path,
                 str(xattrdic), dirent_row_id,
                 config['scan_counter'], parent))
            version_row_id = cur.lastrowid
            contents_changed = True
        elif version_row[4] != stat.st_ctime \
                or version_row[5] != stat.st_mtime:
            xattrdic = {}
            names = os.listxattr(path, follow_symlinks=False)
            for name in names:
                xattrdic[name] = os.getxattr(path, name, follow_symlinks=False)
            await cur.execute(
                "INSERT INTO version (id, is_delmarker, name, size, ctime, mtime, atime, permission, uid, gid, link_path, xattr, dirent_id, scan_counter, parent_id) VALUES (?, 0, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (None, os.path.basename(path), stat.st_size,
                 stat.st_ctime, stat.st_mtime, stat.st_atime,
                 stat.st_mode, stat.st_uid, stat.st_gid, link_path,
                 str(xattrdic), dirent_row_id,
                 config['scan_counter'], parent))
            version_row_id = cur.lastrowid
            if version_row[5] != stat.st_mtime:  # contents changed?
                contents_changed = True
        await db.commit()
    return dirent_row_id, version_row_id, contents_changed


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
    async with aioboto3.client(
            's3', endpoint_url=config['s3_endpoint'], verify=False) as s3:
        if size <= config['buffersize']:
            fo = io.BytesIO(contents)
            await s3.upload_fileobj(fo, config['s3_bucket'], object_hash)
            processing_s3.remove(file_path)
            return

        while config['large_buffers'] >= config['lb_max']:
            await asyncio.sleep(1)
        config['large_buffers'] += 1
        logging.info(f"Grab a large buffer: {size}")
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
    processing_s3.remove(file_path)
    logging.info(
        f"Processed s3 write: {file_path} (db:{len(processing_db)},s3:{len(processing_s3)})")


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
        return

    version_wor_id, contents_changed = 1, True
    _, version_row_id, contents_changed = \
        await set_dirent_version(path, parent, fsid, stat, Kind.FILE)
    if not contents_changed:  # no update to file contents?
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
            while size < chunksize:  # calculate hash for the file or up to chunk size
                contents = await f.read(bufsize)
                size += len(contents)
                if not contents:
                    eof = True
                    break
                hash_val.update(contents)
                prev_contents = contents
            object_hash = hash_val.hexdigest()
            async with aiosqlite.connect(config['db_file'], timeout=config['sqlite_timeout']) as db:
                cur = await db.cursor()
                await cur.execute("SELECT * FROM ver_object WHERE object_hash=?",
                                  (object_hash,))
                # find an ver_object_row -> same content object is in S3
                ver_object_row = await cur.fetchone()
                await cur.execute(
                    "INSERT INTO ver_object (ver_id, object_hash) VALUES (?, ?)",
                    (version_row_id, object_hash))
                await db.commit()
            if not ver_object_row and size != 0:
                task = asyncio.create_task(
                    write_to_s3(chunk_index, path, object_hash, size, prev_contents))
                task_list.append(task)
            if eof:
                break
            chunk_index += 1
    processing_db.remove(path)
    logging.info(
        f"Processed file: (db:{len(processing_db)},s3:{len(processing_s3)})")
    config['num_tasks'] -= 1
    config['processed_files'] += 1


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
    _, version_row_id, _ = \
        await set_dirent_version(path, parent, fsid, stat, Kind.DIRECTORY)
    processing_db.remove(path)

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


async def shutdown(signal, loop):
    """Cleanup tasks."""
    logging.info(f"Received exit signal {signal.name}...")
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
    db_file = Path(config['db_file'])
    if not db_file.is_file:
        logging.error(f"{config['db_file']} doesn't exist.  Aborting.")
        return False
    return True


async def async_backup():
    """
    asynchronous backup main task
    Create database tables, kick the scan, wait for all tasks
    and mark deleted files
    """
    healthy = await check_s3()
    if not healthy:
        return

    async with aiosqlite.connect(config['db_file'], timeout=config['sqlite_timeout']) as db:
        cur = await db.cursor()
        await cur.execute("""CREATE TABLE IF NOT EXISTS dirent (
            id integer PRIMARY KEY,
            is_deleted integer NOT NULL,
            type text NOT NULL,
            fsid text NOT NULL,
            inode integer NOT NULL,
            scan_counter integer NOT NULL
            );""")
        await cur.execute("CREATE INDEX Dentidx1 ON dirent(fsid, inode);")
        await cur.execute("""CREATE TABLE IF NOT EXISTS version (
            id integer PRIMARY KEY,
            is_delmarker integer NOT NULL,
            name text NOT NULL,
            size integer NOT NULL,
            ctime timestamp NOT NULL,
            mtime timestamp NOT NULL,
            atime timestamp NOT NULL,
            permission integer NOT NULL,
            uid integer NOT NULL,
            gid integer NOT NULL,
            link_path text,
            xattr text,
            dirent_id integer NOT NULL,
            scan_counter integer NOT NULL,
            parent_id integer NOT NULL,
            FOREIGN KEY (dirent_id) REFERENCES dirent (id)
            );""")
        await cur.execute("CREATE INDEX Veridx1 ON version(dirent_id);")
        await cur.execute("""CREATE TABLE IF NOT EXISTS ver_object (
            ver_id integer NOT NULL,
            object_hash text NOT NULL,
            FOREIGN KEY (ver_id) REFERENCES version (id)
            );""")
        await cur.execute("CREATE INDEX Voidx1 ON ver_object(ver_id);")
        await cur.execute("CREATE INDEX Voidx2 ON ver_object(object_hash);")
        await cur.execute("""CREATE TABLE IF NOT EXISTS scan (
            scan_counter integer PRIMARY KEY,
            start_time timestamp NOT NULL,
            root_dir text NOT NULL
            );""")
        await cur.execute("SELECT MAX(scan_counter) FROM dirent;")
        row = await cur.fetchone()
        if row[0] or row[0] == 0:
            config['scan_counter'] = row[0] + 1
        logging.info(f"scan_counter: {config['scan_counter']}")
        await cur.execute(
            "INSERT INTO scan (scan_counter, start_time, root_dir) VALUES (?, ?, ?)",
            (config['scan_counter'], datetime.datetime.now(), config['root_dir']))
        await db.commit()
    task = asyncio.create_task(process_dir(config['root_dir'], -1))
    task_list.append(task)
    await asyncio.sleep(1)  # wait a while for task_list to be populated
    await asyncio.gather(*task_list)

    # Take care of deleted files and directories
    async with aiosqlite.connect(config['db_file'], timeout=config['sqlite_timeout']) as db:
        cur = await db.cursor()

        # mark dirent as deleted
        await cur.execute("SELECT id FROM dirent WHERE is_deleted = 0 AND scan_counter < ?", (config['scan_counter'],))
        dent_rows = await cur.fetchall()
        for dent_row in dent_rows:
            logging.info(f"{dent_row}")
            await cur.execute("UPDATE dirent SET is_deleted = 1 WHERE id = ?", (dent_row[0],))

            # Insert delete marker version
            await cur.execute("SELECT * FROM version WHERE dirent_id=? ORDER BY id DESC", (dent_row[0],))
            r = await cur.fetchone()
            if r[1] != 1:  # is_delmarker
                logging.info(f"delete row - {r}")
                await cur.execute("INSERT INTO version (id, is_delmarker, name, size, ctime, mtime, atime, permission, uid, gid, dirent_id, scan_counter, parent_id) VALUES (?, 1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", (None, r[2], r[3], r[4], r[5], r[6], r[7], r[8], r[9], r[12], config['scan_counter'], r[14]))

        await db.commit()

    # backup db file to S3
    async with aioboto3.client(
            's3', endpoint_url=config['s3_endpoint'], verify=False) as s3:
        obj_name = '_'.join(
            [config['db_file'], str(config['scan_counter'])])
        await s3.upload_file(
            config['db_file'], config['s3_bucket'], obj_name)

    await asyncio.sleep(5)


async def async_list():
    """
    asynchronous task to list backup history
    """
    healthy = await check_db()
    if not healthy:
        return
    async with aiosqlite.connect(config['db_file'], timeout=config['sqlite_timeout']) as db:
        cur = await db.cursor()
        await cur.execute("SELECT * FROM scan")
        rows = await cur.fetchall()
        print(f"  #: {'date & time'.ljust(19)} backup root directory")
        for row in rows:
            print(f"{row[0]:3d}: {row[1][:19]} {row[2]}")


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
            Prefix=config['db_file'])
        sorted_dbbkups = []
        async for name in dbbkup_objs:
            logging.info(f"name: {str(name)}")
            name = name.key
            sorted_dbbkups.append(name[len(config['db_file'])+1:])
    sorted_dbbkups.sort()
    logging.info(f"sorted_dbbkups - {sorted_dbbkups}")
    try:
        file_name = '_'.join([config['db_file'],
                              sorted_dbbkups[config['dbrestore_rel']-1]])
    except:
        logging.error(f"No such database backup file version.")
        return
    async with aioboto3.client(
            's3', endpoint_url=config['s3_endpoint'], verify=False) as s3:
        try:
            await s3.download_file(
                config['s3_bucket'], file_name, config['db_file'])
            logging.info(f"restored databae: {file_name}")
        except:
            logging.error(f"Can't download {file_name}")


async def restore_obj(restore_to, dent_id, ver_id, parent_id, kind):
    """
    async task to restore a file/directory/symlink version
    """
    # get database info
    processing_db.append(ver_id)
    async with aiosqlite.connect(config['db_file'], timeout=config['sqlite_timeout']) as db:
        cur = await db.cursor()
        await cur.execute("SELECT * FROM dirent WHERE id=?", (dent_id,))
        dent_row = await cur.fetchone()
        await cur.execute("SELECT * FROM version WHERE id=?", (ver_id,))
        ver_row = await cur.fetchone()
        await cur.execute("SELECT * FROM ver_object WHERE ver_id=?",
                          (ver_id,))
        verobjs = await cur.fetchall()
        if kind == Kind.DIRECTORY:
            # Get children to dispatch
            await cur.execute("SELECT d.id, v.id, v.name, v.parent_id, d.type, v.is_delmarker, MAX(v.scan_counter) FROM dirent d JOIN version v ON d.id=v.dirent_id WHERE (SELECT dirent_id FROM version WHERE id = v.parent_id) = ? AND v.scan_counter <= ? GROUP BY v.name ORDER BY v.scan_counter DESC", (dent_id, config['restore_version']))
            children_rows = await cur.fetchall()
    processing_db.remove(ver_id)

    # download file contents
    processing_s3.append(ver_id)
    fpath = os.path.join(restore_to, ver_row[2])
    if kind == Kind.FILE:
        # logging.info(f"fpath: {fpath}")
        remaining_size = ver_row[3]  # file size
        async with aiofiles.open(fpath, mode='wb') as f:
            # download file contents
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
                    # logging.info(f"verobj: {verobj[1]}")
                    fi = io.BytesIO(view)
                    await s3.download_fileobj(
                        config['s3_bucket'], verobj[1], fi)
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
    elif kind == Kind.DIRECTORY:
        # logging.info(f"mkdir {fpath}")
        try:
            await aiofiles.os.mkdir(fpath, ver_row[7])
        except FileExistsError:
            pass
    else:  # Kind.SYMLINK
        # logging.info(f"Creating symlink: {fpath} to {ver_row[10]}")
        try:
            os.symlink(ver_row[10], fpath)
        except OSError as e:
            if e.errno == errno.EEXIST:
                os.remove(fpath)
                os.symlink(ver_row[10], fpath)

    # set file/directory attributes
    if kind != Kind.SYMLINK:
        # This will cause an exception for a symlink
        os.chmod(fpath, ver_row[7], follow_symlinks=False)
    os.chown(fpath, ver_row[8], ver_row[9], follow_symlinks=False)
    os.utime(fpath, (ver_row[5], ver_row[6]), follow_symlinks=False)
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

    config['num_tasks'] -= 1
    config['processed_files'] += 1


async def async_restore():
    """
    async task to restore files and directories
    """
    healthy_db = await check_db()
    healthy_s3 = await check_s3()
    if not healthy_db or not healthy_s3:
        return

    # Check restore-to directory
    if not os.path.isdir(config['restore_to']):
        logging.error(
            f"{config['restore_to']} directory doesn't exist.  aborting.")
        return

    logging.info(f"restore-to: {config['restore_to']}")

    async with aiosqlite.connect(config['db_file'], timeout=config['sqlite_timeout']) as db:
        cur = await db.cursor()
        restore_target = config['restore_target']

        # convert restore_target to relative from root_dir
        await cur.execute(
            "SELECT root_dir FROM scan ORDER BY scan_counter DESC;")
        row = await cur.fetchone()
        if config['restore_target'].startswith(row[0]):
            restore_target = restore_target.replace(row[0], '')
        if config['restore_target'] == 'all':
            restore_target = ''

        logging.info(
            f"restore-target: {config['restore_target']} ({restore_target})")

        # traverse path
        plist = restore_target.split('/')
        parent_id = -1  # root_dir
        await cur.execute(
            "SELECT d.id, v.id, d.type FROM dirent d JOIN version v ON d.id=v.dirent_id WHERE v.parent_id=? AND v.scan_counter<=? ORDER BY v.scan_counter DESC;", (parent_id, config['restore_version']))
        row = await cur.fetchone()
        for pitem in plist[:-1]:
            parent_id = row[0]
            await cur.execute(
                "SELECT d.id, v.id, d.type FROM dirent d JOIN version v ON d.id=v.dirent_id WHERE v.parent_id=? AND v.scan_counter<=? ORDER BY v.scan_counter DESC;", (parent_id, config['restore_version']))
            row = await cur.fetchone()
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
        await asyncio.gather(*task_list)
        await asyncio.sleep(5)


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
    print(
        f"Processed {config['processed_files']} files ({config['processed_files']/((config['end_time']-config['start_time']).total_seconds()-5)} files/sec)")


if __name__ == "__main__":
    main()
