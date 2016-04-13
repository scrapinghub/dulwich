from io import BytesIO

from dulwich.errors import NoIndexPresent
from dulwich.mysqlconnection import replenishing_cursor, set_db_url
from dulwich.object_store import BaseObjectStore
from dulwich.objects import sha_to_hex
from dulwich.pack import (PackData, PackInflater, write_pack_header,
                          write_pack_object, PackIndexer, PackStreamCopier,
                          compute_file_sha)
from dulwich.repo import BaseRepo
from dulwich.refs import RefsContainer, SYMREF


class MysqlObjectStore(BaseObjectStore):
    """Object store that keeps all objects in a mysql database."""

    statements = {
        "HAS": "SELECT EXISTS(SELECT 1 FROM objs WHERE `oid`=%s AND `repo`=%s)",
        "ALL": "SELECT `oid` FROM objs WHERE `repo`=%s",
        "GET": "SELECT `type`, UNCOMPRESS(`data`) FROM objs WHERE `oid`=%s AND `repo`=%s",
        "ADD": "INSERT IGNORE INTO objs values(%s, %s, %s, COMPRESS(%s), %s)",
        "DEL": "DELETE FROM objs WHERE `oid`=%s AND `repo`=%s",
    }

    def __init__(self, repo):
        super(MysqlObjectStore, self).__init__()
        self._repo = repo

    def _to_hexsha(self, sha):
        if len(sha) == 40:
            return sha
        elif len(sha) == 20:
            return sha_to_hex(sha)
        else:
            raise ValueError("Invalid sha %r" % (sha,))

    @replenishing_cursor
    def _has_sha(self, sha, cursor):
        """Look for the sha in the database."""
        cursor.execute(MysqlObjectStore.statements["HAS"], (sha, self._repo))
        row = cursor.fetchone()
        return row[0] == 1

    @replenishing_cursor
    def _all_shas(self, cursor):
        """Return all db sha keys."""
        cursor.execute(MysqlObjectStore.statements["ALL"], (self._repo,))
        shas = (t[0] for t in cursor.fetchall())
        return shas

    def contains_loose(self, sha):
        """Check if a particular object is present by SHA1 and is loose."""
        return self._has_sha(self._to_hexsha(sha))

    def contains_packed(self, sha):
        """Check if a particular object is present by SHA1 and is packed."""
        return False

    def __iter__(self):
        """Iterate over the SHAs that are present in this store."""
        return self._all_shas()

    @property
    def packs(self):
        """List with pack objects."""
        return []

    @replenishing_cursor
    def get_raw(self, name, cursor):
        """Obtain the raw text for an object.

        :param name: sha for the object.
        :return: tuple with numeric type and object contents.
        """
        cursor.execute(MysqlObjectStore.statements["GET"],
                       (self._to_hexsha(name), self._repo))
        row = cursor.fetchone()
        return row

    def _add_object(self, obj, cursor):
        data = obj.as_raw_string()
        oid = obj.id
        tnum = obj.get_type()
        cursor.execute(MysqlObjectStore.statements["ADD"],
                       (oid, tnum, len(data), data, self._repo))

    @replenishing_cursor
    def add_object(self, obj, cursor):
        self._add_object(obj, cursor)

    @replenishing_cursor
    def add_objects(self, objects, cursor):
        """Add a set of objects to this object store.

        :param objects: Iterable over a list of objects.
        """
        data = ((o.id, o.get_type(), len(o.as_raw_string()), o.as_raw_string(),
                 self._repo) for (o, _) in objects)
        cursor.executemany(MysqlObjectStore.statements["ADD"], data)

    @replenishing_cursor
    def delete_objects(self, object_ids, cursor):
        cursor.executemany(MysqlObjectStore.statements["DEL"],
                           ((oid, self._repo) for oid in object_ids))

    @replenishing_cursor
    def add_pack(self, cursor):
        """Add a new pack to this object store.

        Because this object store doesn't support packs, we extract and add the
        individual objects.

        :return: Fileobject to write to and a commit function to
            call when the pack is finished.
        """
        f = BytesIO()

        def commit():
            p = PackData.from_file(BytesIO(f.getvalue()), f.tell())
            f.close()
            for obj in PackInflater.for_pack_data(p):
                self._add_object(obj, cursor)

        def abort():
            pass
        return f, commit, abort

    def _complete_thin_pack(self, f, indexer):
        """Complete a thin pack by adding external references.

        :param f: Open file object for the pack.
        :param indexer: A PackIndexer for indexing the pack.
        """
        entries = list(indexer)

        # Update the header with the new number of objects.
        f.seek(0)
        write_pack_header(f, len(entries) + len(indexer.ext_refs()))

        # Rescan the rest of the pack, computing the SHA with the new header.
        new_sha = compute_file_sha(f, end_ofs=-20)

        # Complete the pack.
        for ext_sha in indexer.ext_refs():
            assert len(ext_sha) == 20
            type_num, data = self.get_raw(ext_sha)
            write_pack_object(f, type_num, data, sha=new_sha)
        pack_sha = new_sha.digest()
        f.write(pack_sha)

    def add_thin_pack(self, read_all, read_some):
        """Add a new thin pack to this object store.

        Thin packs are packs that contain deltas with parents that exist
        outside the pack. Because this object store doesn't support packs, we
        extract and add the individual objects.

        :param read_all: Read function that blocks until the number of
            requested bytes are read.
        :param read_some: Read function that returns at least one byte, but may
            not return the number of bytes requested.
        """
        f, commit, abort = self.add_pack()
        try:
            indexer = PackIndexer(f, resolve_ext_ref=self.get_raw)
            copier = PackStreamCopier(read_all, read_some, f,
                                      delta_iter=indexer)
            copier.verify()
            self._complete_thin_pack(f, indexer)
        except:
            abort()
            raise
        else:
            commit()


class MysqlRefsContainer(RefsContainer):
    """RefsContainer backed by MySql.

    This container does not support packed references.
    """

    statements = {
        "DEL": "DELETE FROM `refs` WHERE `ref`=%s AND `repo`=%s",
        "ALL": "SELECT `ref` FROM `refs` WHERE `repo`=%s",
        "GET": "SELECT `value` FROM `refs` WHERE `ref` = %s AND `repo`=%s",
        "ADD": "REPLACE INTO `refs` VALUES(%s, %s, %s)",
    }

    def __init__(self, repo):
        super(MysqlRefsContainer, self).__init__()
        self._repo = repo
        self._peeled = {}

    @replenishing_cursor
    def allkeys(self, cursor):
        cursor.execute(MysqlRefsContainer.statements["ALL"], (self._repo,))
        return (t[0] for t in cursor.fetchall())

    @replenishing_cursor
    def read_loose_ref(self, name, cursor):
        cursor.execute(MysqlRefsContainer.statements["GET"],
                       (name, self._repo))
        row = cursor.fetchone()
        return row[0] if row else None

    def get_packed_refs(self):
        return {}

    def _update_ref(self, name, value, cursor):
        cursor.execute(MysqlRefsContainer.statements["ADD"],
                       (name, value, self._repo))

    @replenishing_cursor
    def set_if_equals(self, name, old_ref, new_ref, cursor):
        if old_ref is not None:
            current_ref = self.read_loose_ref(name)
            if old_ref != current_ref:
                return False
        realname, _ = self._follow(name)
        self._check_refname(realname)
        self._update_ref(realname, new_ref, cursor)
        return True

    @replenishing_cursor
    def set_symbolic_ref(self, name, other, cursor):
        self._update_ref(name, SYMREF + other)

    @replenishing_cursor
    def add_if_new(self, name, ref, cursor):
        if self.read_loose_ref(name):
            return False
        self._update_ref(name, ref)
        return True

    def _remove_ref(self, name, cursor):
        cursor.execute(MysqlRefsContainer.statements["DEL"],
                       (name, self._repo))

    @replenishing_cursor
    def remove_if_equals(self, name, old_ref, cursor):
        if old_ref is not None:
            current_ref = self.read_loose_ref(name)
            if current_ref != old_ref:
                return False
        self._remove_ref(name, cursor)
        return True

    def get_peeled(self, name):
        return self._peeled.get(name)


class MysqlRepo(BaseRepo):
    """Repo that stores refs, objects, and named files in MySql.

    MySql repos are always bare: they have no working tree and no index, since
    those have a stronger dependency on the filesystem.
    """

    def __init__(self, name):
        self._name = name
        BaseRepo.__init__(self, MysqlObjectStore(name),
                          MysqlRefsContainer(name))
        self.bare = True

    def open_index(self):
        """Fail to open index for this repo, since it is bare.

        :raise NoIndexPresent: Raised when no index is present
        """
        raise NoIndexPresent()

    def head(self):
        """Return the SHA1 pointed at by HEAD."""
        return self.refs['refs/heads/master']

    @classmethod
    @replenishing_cursor
    def _init_db(cls, cursor):

        # Object store table.
        sql = ('CREATE TABLE IF NOT EXISTS `objs` ('
               '  `oid` binary(40) NOT NULL DEFAULT "",'
               '  `type` tinyint(1) unsigned NOT NULL,'
               '  `size` bigint(20) unsigned NOT NULL,'
               '  `data` longblob NOT NULL,'
               '  `repo` varchar(64) NOT NULL,'
               '  PRIMARY KEY (`oid`, `repo`),'
               '  KEY `type` (`type`),'
               '  KEY `size` (`size`)'
               ') ENGINE="InnoDB" DEFAULT CHARSET=utf8 COLLATE=utf8_bin')
        cursor.execute(sql)

        # Reference store table.
        sql = ('CREATE TABLE IF NOT EXISTS `refs` ('
               '  `ref` varchar(100) NOT NULL DEFAULT "",'
               '  `value` binary(40) NOT NULL,'
               '  `repo` varchar(64) NOT NULL,'
               '  PRIMARY KEY (`ref`, `repo`),'
               '  KEY `value` (`value`)'
               ') ENGINE="InnoDB" DEFAULT CHARSET=utf8 COLLATE=utf8_bin')
        cursor.execute(sql)

    @classmethod
    def setup(cls, location):
        set_db_url(location)

    @classmethod
    def init_bare(cls, name):
        """Create a new bare repository.
        """
        return cls(name)

    @classmethod
    def open(cls, name):
        """Open an existing repository.
        """
        return cls(name)

    @classmethod
    @replenishing_cursor
    def repo_exists(cls, name, cursor):
        """Checks if a repository exists.
        """
        cursor.execute("SELECT EXISTS(SELECT 1 FROM `objs` "
                       "WHERE `repo`=%s)", (name,))
        row = cursor.fetchone()
        return row[0] == 1

    @classmethod
    @replenishing_cursor
    def list_repos(cls, cursor):
        """List all repository names.
        """
        cursor.execute("SELECT DISTINCT `repo` FROM `objs`")
        return [t[0] for t in cursor.fetchall()]

    @classmethod
    @replenishing_cursor
    def delete_repo(cls, name, cursor):
        """Deletes a repository.
        """
        cursor.execute("DELETE FROM `objs` WHERE `repo`=%s", (name,))
        cursor.execute("DELETE FROM `refs` WHERE `repo`=%s", (name,))
