import os
import io
import asyncio
import logging
import random
import signal
import hashlib
import uuid
import datetime
import yaml

import aiosqlite
import aiofiles
import aiofiles.os
import aioboto3

config = {
    'db_file': 'bus3.db',
    # 'chunksize': 64*1024*1024,  # max object chunk size in S3 (64MB)
    'chunksize': 4*1024*1024,  # max object chunk size in S3 (4MB)
    'buffersize': 256*1024,  # buffer size for hash calculation (256KB)
    'scan_counter': 0,  # initial value
}
root_dir = None
processing_db = []  # list of paths to files/dirs
processing_s3 = []  # list of paths to files
large_buffers = 0  # Number of large buffers (up to chunksize) being used
task_list = []  # task list

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s,%(msecs)d %(levelname)s: %(message)s",
    datefmt="%H:%M:%S",
)


async def set_dirent_version(path, parent, fsid, stat, kind):
    """
    Set dirent and version tables
    Return:
        dirent_row_id: dirent id
        version_row_id: version id if created.  -1 if not
        contents_changed: True if file contents changed
    """
    async with aiosqlite.connect(config['db_file']) as db:
        cur = await db.cursor()

        # dirent table
        await cur.execute(
            "SELECT * FROM dirent WHERE fsid=? AND inode=?",
            (fsid, stat.st_ino))
        dirent_row = await cur.fetchone()
        if not dirent_row:
            await cur.execute(
                "INSERT INTO dirent (id, parent_id, type, fsid, inode, is_deleted, scan_counter) VALUES (?, ?, ?, ?, ?, 0, ?)",
                (None, parent, kind, fsid, stat.st_ino, config['scan_counter']))
            dirent_row_id = cur.lastrowid
        else:
            dirent_row_id = dirent_row[0]
            await cur.execute(
                "UPDATE dirent SET parent_id = ?, is_deleted = 0, scan_counter = ? WHERE id = ?",
                (parent, config['scan_counter'], dirent_row_id))

        # version table
        version_row_id = -1
        contents_changed = False
        link_path = ""
        if kind == "SYMLINK":
            link_path = os.readlink(path)
        await cur.execute(
            "SELECT * FROM version WHERE dirent_id=? AND is_latest=1",
            (dirent_row_id, ))
        version_row = await cur.fetchone()
        if not version_row:
            xattrdic = {}
            names = os.listxattr(path, follow_symlinks=False)
            for name in names:
                xattrdic[name] = os.getxattr(path, name, follow_symlinks=False)
            await cur.execute(
                "INSERT INTO version (id, is_latest, name, size, ctime, mtime, atime, permission, uid, gid, link_path, xattr, dirent_id, scan_counter) VALUES (?, 1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (None, os.path.basename(path), stat.st_size,
                 stat.st_ctime, stat.st_mtime, stat.st_atime,
                 stat.st_mode, stat.st_uid, stat.st_gid, link_path,
                 str(xattrdic), dirent_row_id, config['scan_counter']))
            version_row_id = cur.lastrowid
            contents_changed = True
        elif version_row[4] != stat.st_ctime \
                or version_row[5] != stat.st_mtime:
            xattrdic = {}
            names = os.listxattr(path, follow_symlinks=False)
            for name in names:
                xattrdic[name] = os.getxattr(path, name, follow_symlinks=False)
            await cur.execute(
                "UPDATE version SET is_latest=0 WHERE id=?",
                (cur.lastrowid,))
            await cur.execute(
                "INSERT INTO version (id, is_latest, name, size, ctime, mtime, atime, permission, uid, gid, link_path, xattr, dirent_id, scan_counter) VALUES (?, 1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (None, os.path.basename(path), stat.st_size,
                 stat.st_ctime, stat.st_mtime, stat.st_atime,
                 stat.st_mode, stat.st_uid, stat.st_gid, link_path,
                 str(xattrdic), dirent_row_id, config['scan_counter']))
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
        contents: contents if size <= buffer size (ie, don't need to read file)
    """
    global large_buffers
    processing_s3.append(file_path)
    async with aioboto3.client(
            's3', endpoint_url=config['s3_endpoint'], verify=False) as s3:
        if size <= config['buffersize']:
            # logging.info(contents.decode('utf-8'))
            fo = io.BytesIO(contents)
            await s3.upload_fileobj(fo, config['s3_bucket'], object_hash)
            processing_s3.remove(file_path)
            return

        while large_buffers >= 16:
            await asyncio.sleep(1)
        large_buffers += 1
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
        large_buffers -= 1
    processing_s3.remove(file_path)


async def process_file(path, parent, fsid, islink):
    """Process a file.

    Args:
        path: path to file
        parent: parent dirent id
        fsid: filesystem id
        islink: True if symbolic link
    """
    stat = await aiofiles.os.stat(path, follow_symlinks=False)
    processing_db.append(path)
    if islink:  # symbolic link
        logging.info(f"Processing symlink: {path}")
        await set_dirent_version(path, parent, fsid, stat, "SYMLINK")
        processing_db.remove(path)
        return

    logging.info(f"Processing file: {path}")
    _, version_row_id, contents_changed = await set_dirent_version(path, parent, fsid, stat, "FILE")
    if not contents_changed:  # no update to file contents?
        processing_db.remove(path)
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
            async with aiosqlite.connect(config['db_file']) as db:
                cur = await db.cursor()
                await cur.execute("SELECT * FROM ver_object WHERE object_hash=?",
                                  (object_hash,))
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


async def process_dir(path, parent):
    """Process a directory.

    Args:
        path: directory path name
        parent: parent dirent row id
    """
    logging.info(f"Processing dir: {path}")
    # Check if it's in the DB and if updated
    processing_db.append(path)
    fsid = str(os.statvfs(path).f_fsid)  # not async
    stat = await aiofiles.os.stat(path)
    dirent_row_id, _, _ = await set_dirent_version(path, parent, fsid, stat, "DIRECTORY")
    processing_db.remove(path)

    for dent in os.scandir(path):
        while len(processing_db) > 1000 or len(processing_s3) > 1000:
            await asyncio.sleep(1)

        if dent.is_file(follow_symlinks=False):
            task = asyncio.create_task(
                process_file(dent.path, dirent_row_id, fsid, False))
            task_list.append(task)
        elif dent.is_dir(follow_symlinks=False):
            task = asyncio.create_task(
                process_dir(dent.path, dirent_row_id))
            task_list.append(task)
        elif dent.is_symlink():
            task = asyncio.create_task(
                process_file(dent.path, dirent_row_id, fsid, True))
            task_list.append(task)
        else:
            logging.info(f"Not file or dir: {dent.path}  Skipped")


async def shutdown(signal, loop):
    """Cleanup tasks."""
    logging.info(f"Received exit signal {signal.name}...")
    tasks = [t for t in asyncio.all_tasks() if t is not
             asyncio.current_task()]
    [task.cancel() for task in tasks]
    logging.info(f"Cancelling {len(tasks)} outstanding tasks")
    await asyncio.gather(*tasks, return_exceptions=True)
    loop.stop()


async def async_main():
    """
    asynchronous main task
    Create database tables, read config and kick the scan
    """
    async with aiosqlite.connect(config['db_file']) as db:
        cur = await db.cursor()
        await cur.execute("""CREATE TABLE IF NOT EXISTS dirent (
            id integer PRIMARY KEY,
            parent_id integer NOT NULL,
            type text NOT NULL,
            fsid text NOT NULL,
            inode integer NOT NULL,
            is_deleted integer NOT NULL,
            scan_counter integer NOT NULL
            );""")
        await cur.execute("""CREATE TABLE IF NOT EXISTS version (
            id integer PRIMARY KEY,
            is_latest integer NOT NULL,
            name text NOT NULL,
            size integer NOT NULL,
            ctime timestamp NOT NULL,
            mtime timestamp NOT NULL,
            atime timestamp NOT NULL,
            permission text NOT NULL,
            uid integer NOT NULL,
            gid integer NOT NULL,
            link_path text,
            xattr text,
            dirent_id integer NOT NULL,
            scan_counter integer NOT NULL,
            FOREIGN KEY (dirent_id) REFERENCES dirent (id)
            );""")
        await cur.execute("""CREATE TABLE IF NOT EXISTS ver_object (
            ver_id integer NOT NULL,
            object_hash text NOT NULL,
            FOREIGN KEY (ver_id) REFERENCES version (id)
            );""")
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
            (config['scan_counter'], datetime.datetime.now(), root_dir))
        await db.commit()
    task = asyncio.create_task(process_dir(root_dir, -1))
    task_list.append(task)
    await asyncio.sleep(2)  # wait a while for task_list to be populated
    await asyncio.gather(*task_list)

    # Mark deleted files
    async with aiosqlite.connect(config['db_file']) as db:
        cur = await db.cursor()
        await cur.execute(
            "UPDATE dirent SET is_deleted = 1 WHERE scan_counter < ?",
            (config['scan_counter'], ))
        await db.commit()


def main():
    global root_dir
    with open("bus3.yaml", 'r') as f:
        loaded = yaml.safe_load(f)
    config.update(loaded['s3_config'])
    root_dir = loaded['root_dir']
    loop = asyncio.get_event_loop()
    signals = (signal.SIGHUP, signal.SIGTERM, signal.SIGINT)
    for s in signals:
        loop.add_signal_handler(
            s, lambda s=s: asyncio.create_task(shutdown(s, loop)))

    try:
        task = loop.create_task(async_main())
        loop.run_until_complete(task)
    except KeyboardInterrupt:
        logging.info("Process interrupted")
    finally:
        loop.close()
        logging.info("Completed or gracefully terminated")


if __name__ == "__main__":
    main()
