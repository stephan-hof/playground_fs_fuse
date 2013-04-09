import os
import stat
import time
import errno
import collections
import llfuse

class Entry(object):
    def __init__(self, name, inode, parent_inode, mode, uid, gid):
        self.name = name
        self.inode = inode
        self.parent_inode = parent_inode
        self.mode = mode
        self.link_count = 1
        self.uid = uid
        self.gid = 0
        self.rdev = 0

        self.atime = self.mtime = self.ctime = int(time.time())

        # Only directories have this
        self.entries = []

        # Only 'regual files' have this
        self.data = bytearray()

mask = stat.S_IWGRP | stat.S_IRGRP | stat.S_IXGRP | stat.S_IROTH | stat.S_IXOTH

all_entries = {
    llfuse.ROOT_INODE: Entry(
        'root',
        llfuse.ROOT_INODE,
        llfuse.ROOT_INODE,
        stat.S_IFDIR | stat.S_IRWXU | mask,
        os.getuid(),
        os.getgid())
}

inode_count = llfuse.ROOT_INODE
active_inodes = collections.defaultdict(int)

class Operations(llfuse.Operations):
    def statfs(self):
        st = llfuse.StatvfsData()

        st.f_bsize = 512
        st.f_frsize = 512

        size = sum([len(x.data) for x in all_entries.values()])
        st.f_blocks = size // st.f_frsize
        st.f_bfree = max(size // st.f_frsize, 1024)
        st.f_bavail = st.f_bfree

        inodes = len(all_entries)
        st.f_files = inodes
        st.f_ffree = max(inodes , 100)
        st.f_favail = st.f_ffree

        return st

    def opendir(self, inode):
        """Just to check access, dont care about access => return inode"""
        print 'opendir'
        return inode

    def access(self, inode, mode, ctx):
        """Again this fs does not care about access"""
        print 'access', inode
        return True

    def lookup(self, parent_inode, name):
        print 'lookup', parent_inode, name
        if name == '.':
            inode = parent_inode
        elif name == '..':
            inode = all_entries[parent_inode].parent_inode
        else:
            for record in all_entries[parent_inode].entries:
                if record.name == name:
                    inode = record.inode
                    break
            else:
                raise llfuse.FUSEError(errno.ENOENT)

        print 'lookup success', inode
        return self.getattr(inode)

    def readdir(self, inode, off):
        entry = all_entries[inode]
        print 'readarrd', inode, entry.name

        for index, record in enumerate(entry.entries[off:]):
            yield (record.name, self.getattr(record.inode), off + index + 1)

    def getattr(self, inode):
        """This is the 'stat' syscall"""
        entry = all_entries[inode]
        print 'getattr', inode, entry.name

        attr = llfuse.EntryAttributes()
        attr.st_ino = entry.inode
        attr.generation = 0
        attr.entry_timeout = 300
        attr.attr_timeout = 300
        attr.st_mode = entry.mode
        attr.st_nlink = entry.link_count

        attr.st_uid = entry.uid
        attr.st_gid = entry.gid
        attr.st_rdev = entry.rdev
        attr.st_size = len(entry.data)

        attr.st_blksize = 512
        attr.st_blocks = 1
        attr.st_atime = entry.atime
        attr.st_mtime = entry.mtime
        attr.st_ctime = entry.ctime

        print 'getattr success', attr.st_ino, entry.name
        return attr

    def readlink(self, inode):
        print 'readlink called'
        return super(Operations, self).readlink(inode)

    def symlink(self, parent_inode, name, target, ctx):
        print 'symlink'
        return super(Operations, self).symlink(parent_inode, name, target, ctx)

    def rename(self, old_p_inode, old_name, new_p_inode, new_name):
        print 'rename', old_name, new_name
        attr = self.lookup(old_p_inode, old_name)
        entry_old = all_entries[attr.st_ino]

        try:
            attr = self.lookup(new_p_inode, new_name)
            entry_new = all_entries[attr.st_ino]
        except llfuse.FUSEError as error:
            if error.errno != errno.ENOENT:
                raise
            target_exists = False
        else:
            target_exists = True

        if target_exists:
            if len(entry_new.entries) > 0:
                raise llfuse.FUSEError(errno.ENOTEMPTY)

        self._remove_from_parent(entry_old)
        entry_old.parent = new_p_inode
        entry_old.name = new_name
        all_entries[new_p_inode].entries.append(entry_old)

        if target_exists:
            self._remove(entry_new)

    def link(self, inode, new_p_inode, new_name):
        print 'link'
        return super(Operations, self).link(inode, new_p_inode, new_name)

    def mknod(self, inode_p, name, mode, rdev, ctx):
        print 'mknod'
        return super(Operations, self).mknod(inode_p, name, mode, rdev, ctx)

    def mkdir(self, parent_inode, name, mode, ctx):
        print 'mkdir'
        entry = self._create(parent_inode, name, mode, ctx)
        return self.getattr(entry.inode)

    def create(self, inode_parent, name, mode, flags, ctx):
        print 'create', inode_parent, name, mode
        entry = self._create(inode_parent, name, mode, ctx)
        active_inodes[entry.inode] += 1
        print 'create success', entry.inode
        return entry.inode, self.getattr(entry.inode)

    def _create(self, parent_inode, name, mode, ctx):
        global inode_count
        inode_count += 1
        entry = Entry(
            name,
            inode_count,
            parent_inode,
            mode,
            ctx.uid,
            ctx.gid)

        all_entries[entry.inode] = entry
        all_entries[parent_inode].entries.append(entry)
        return entry

    def open(self, inode, flags):
        print 'open'
        active_inodes[inode] += 1
        return inode

    def setattr(self, inode, attr):
        print 'setattr'
        entry = all_entries[inode]

        if attr.st_size is not None:
            cur_size = len(entry.data)
            if cur_size < attr.st_size:
                entry.data.extend('0' * (attr.st_size - cur_size))
            else:
                entry.data = entry.data[:attr.st_size]

        to_set = [
            'st_mode',
            'st_uid',
            'st_gid',
            'st_rdev',
            'st_atime',
            'st_mtime',
            'st_ctime'
        ]

        for attr_name in to_set:
            val = getattr(attr, attr_name, None)
            if val is not None:
                target = attr_name[3:]
                setattr(entry, target, val)

        return self.getattr(entry.inode)

    def unlink(self, parent_inode, name):
        print 'unlink', parent_inode, name
        attr = self.lookup(parent_inode, name)
        entry = all_entries[attr.st_ino]

        if stat.S_ISDIR(entry.mode):
            raise llfuse.FUSEError(errno.EISDIR)

        self._remove(entry)

    def rmdir(self, parent_inode, name):
        print 'rmdir'
        attr = self.lookup(parent_inode, name)
        entry = all_entries[attr.st_ino]

        if not stat.S_ISDIR(entry.mode):
            raise llfuse.FUSEError(errno.ENOTDIR)

        if len(entry.entries) > 0:
            raise llfuse.FUSEError(errno.ENOTEMPTY)

        self._remove(entry)

    def _remove(self, entry):
        self._remove_from_parent(entry)
        if entry.link_count > 1:
            return

        if not entry.inode in active_inodes:
            print 'remove entry', entry.inode
            del all_entries[entry.inode]

    def _remove_from_parent(self, entry):
        parent = all_entries[entry.parent_inode]
        to_keep = []
        for record in parent.entries:
            if record.inode != entry.inode:
                to_keep.append(record)
        parent.entries = to_keep


    def read(self, inode, offset, length):
        print 'read', inode, offset, length
        entry = all_entries[inode]
        return str(entry.data[offset:(offset+length)])

    def write(self, inode, offset, data):
        if os.path.isfile("slow_write"):
            print 'start sleep'
            time.sleep(30)
            print 'end sleep'

        print 'write', inode, offset, len(data)
        entry = all_entries[inode]
        if len(entry.data) < offset:
            entry.data.extend('\0' * (offset - len(entry.data)))
        entry.data[offset:(offset + len(data))] = data
        return len(data)

    def release(self, inode):
        print 'release', inode
        active_inodes[inode] -= 1

        if active_inodes[inode] < 1:
            del active_inodes[inode]
            entry = all_entries[inode]
            if entry.link_count == 0:
                self._remove(entry)


if __name__ == '__main__':

    mountpoint = "/home/joki/Desktop/slow_disk_with_fuse/mountpoint"
    operations = Operations()

    llfuse.init(
        operations,
        mountpoint,
        ['fsname=sh_test', 'nonempty'])

    try:
        # single=False means multiple threads can call operations
        llfuse.main(single=False)
    finally:
        llfuse.close()
