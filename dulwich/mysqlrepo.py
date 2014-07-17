from dulwich.object_store import BaseObjectStore
from dulwich.repo import BaseRepo
from dulwich.refs import RefsContainer
from dulwich.mysqlconnection import dbcursor


class MysqlObjectStore(BaseObjectStore):
    """Object store that keeps all objects in a mysql database."""

    def __init__(self, table):
        super(MysqlObjectStore, self).__init__()
        self._table = table
        self._statements = self._create_sql_statements(table)

    def _to_hexsha(self, sha):
        if len(sha) == 40:
            return sha
        elif len(sha) == 20:
            return sha_to_hex(sha)
        else:
            raise ValueError("Invalid sha %r" % (sha,))

    def _create_sql_statements(self, tablename):
        statements = {
            "HAS": "SELECT EXISTS(SELECT 1 FROM `{}` WHERE `oid` = %s)",
            "ALL": "SELECT `oid` FROM `{}`",
            "GET": "SELECT `type`, UNCOMPRESS(`data`) FROM `{}` WHERE `oid` = %s",
            "ADD": "INSERT IGNORE INTO `{}` VALUES(%s, %s, %s, COMPRESS(%s))",
        }
        for k, s in statements.iteritems():
            statements[k] = s.format(tablename)
        return statements

    @dbcursor
    def _has_sha(self, sha, cursor):
        """Look for the sha in the database."""
        cursor.execute(self._statements["HAS"], (sha,))
        row = cursor.fetchone()
        return row[0] == 1

    @dbcursor
    def _all_shas(self, cursor):
        """Return all db sha keys."""
        cursor.execute(self._statements["ALL"])
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

    @dbcursor
    def get_raw(self, name, cursor):
        """Obtain the raw text for an object.

        :param name: sha for the object.
        :return: tuple with numeric type and object contents.
        """
        cursor.execute(self._statements["GET"], (self._to_hexsha(name),))
        row = cursor.fetchone()
        return row

    def _add_object(self, obj, cursor):
        data = obj.as_raw_string()
        oid = obj.id
        tnum = obj.get_type()        
        cursor.execute(self._statements["ADD"], (oid, tnum, len(data), data))

    @dbcursor
    def add_object(self, obj, cursor):
        self._add_object(obj, cursor)

    @dbcursor
    def add_objects(self, objects, cursor):
        """Add a set of objects to this object store.

        :param objects: Iterable over a list of objects.
        """
        for obj, path in objects:
            self._add_object(obj, cursor)

    @dbcursor
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

        Thin packs are packs that contain deltas with parents that exist outside
        the pack. Because this object store doesn't support packs, we extract
        and add the individual objects.

        :param read_all: Read function that blocks until the number of requested
            bytes are read.
        :param read_some: Read function that returns at least one byte, but may
            not return the number of bytes requested.
        """
        f, commit, abort = self.add_pack()
        try:
            indexer = PackIndexer(f, resolve_ext_ref=self.get_raw)
            copier = PackStreamCopier(read_all, read_some, f, delta_iter=indexer)
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

    def __init__(self, table):
        super(MysqlRefsContainer, self).__init__()
        self._table = table
        self._peeled = {}
        self._statements = self._create_sql_statements(table)

    def _create_sql_statements(self, tablename):
        statements = {
            "DEL": "DELETE FROM `{}` WHERE `ref` = %s",
            "ALL": "SELECT `ref` FROM `{}`",
            "GET": "SELECT `value` FROM `{}` WHERE `ref` = %s",
            "ADD": "REPLACE INTO `{}` VALUES(%s, %s)",
        }
        for k, s in statements.iteritems():
            statements[k] = s.format(tablename)
        return statements

    @dbcursor    
    def allkeys(self, cursor):
        cursor.execute(self._statements["ALL"])
        return (t[0] for t in cursor.fetchall())

    @dbcursor
    def read_loose_ref(self, name, cursor):
        cursor.execute(self._statements["GET"], (name,))
        row = cursor.fetchone()
        return row[0] if row else None

    def get_packed_refs(self):
        return {}

    def _update_ref(self, name, value, cursor):
        cursor.execute(self._statements["ADD"], (name, value))

    @dbcursor
    def set_if_equals(self, name, old_ref, new_ref, cursor):
        if old_ref is not None:
            current_ref = self.read_loose_ref(name)
            if old_ref != current_ref:
                return False
        realname, _ = self._follow(name)
        self._check_refname(realname)
        self._update_ref(realname, new_ref, cursor)
        return True

    @dbcursor
    def set_symbolic_ref(self, name, other, cursor):
        self._update_ref(name, SYMREF + other)

    @dbcursor
    def add_if_new(self, name, ref, cursor):
        if self.read_loose_ref(name):
            return False
        self._update_ref(name, ref)
        return True

    def _remove_ref(self, name, cursor):
        cursor.execute(self._statements["DEL"], (name,))

    @dbcursor
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

    def __init__(self, name, create_repo=False):
        self._name = name
        ot_name, rt_name = MysqlRepo._table_names(name)
        if create_repo:
            self._create_repo(ot_name, rt_name)
        BaseRepo.__init__(self,
            MysqlObjectStore(ot_name), MysqlRefsContainer(rt_name))
        self.bare = True

    def open_index(self):
        """Fail to open index for this repo, since it is bare.

        :raise NoIndexPresent: Raised when no index is present
        """
        raise NoIndexPresent()

    def head(self):
        """Return the SHA1 pointed at by HEAD."""
        return self.refs['refs/heads/master']

    @dbcursor
    def _create_repo(self, ot_name, rt_name, cursor):
        
        # Object store table.
        sql = ('CREATE TABLE `%s` ('
            '  `oid` binary(40) NOT NULL DEFAULT "",'
            '  `type` tinyint(1) unsigned NOT NULL,'
            '  `size` bigint(20) unsigned NOT NULL,'
            '  `data` longblob NOT NULL,'
            '  PRIMARY KEY (`oid`),'
            '  KEY `type` (`type`),'
            '  KEY `size` (`size`)'
            ') ENGINE="InnoDB" DEFAULT CHARSET=utf8 COLLATE=utf8_bin') % ot_name
        cursor.execute(sql)
        
        # Reference store table.
        sql = ('CREATE TABLE `%s` ('
            '  `ref` varchar(100) NOT NULL DEFAULT "",'
            '  `value` binary(40) NOT NULL,'
            '  PRIMARY KEY (`ref`),'
            '  KEY `value` (`value`)'
            ') ENGINE="InnoDB" DEFAULT CHARSET=utf8 COLLATE=utf8_bin') % rt_name
        cursor.execute(sql)
        
        return ot_name, rt_name

    @classmethod
    def _table_names(cls, name):
        return name + '_portiarepo_obj', name + '_portiarepo_ref'

    @classmethod
    def init_bare(cls, name):
        """Create a new bare repository in Mysql.
        """
        return cls(name, create_repo=True)

    @classmethod
    def open(cls, name):
        """Create a new bare repository in Mysql.
        """
        return cls(name)

    @classmethod
    @dbcursor
    def repo_exists(cls, name, cursor):
        """Checks if a repo exists.
        """
        table_name, _ =  MysqlRepo._table_names(name)
        cursor.execute("SELECT EXISTS(SELECT 1 FROM information_schema.tables "
                       "WHERE `table_name` = %s)", (table_name,))
        row = cursor.fetchone()
        return row[0] == 1

    @classmethod
    @dbcursor
    def list_repos(cls, cursor):
        """List all repo names.
        """
        cursor.execute("SELECT `table_name` FROM information_schema.tables "
                       "WHERE `table_name` LIKE '%_portiarepo_obj%'")
        names = [t[0][0:-len('_portiarepo_obj')] for t in cursor.fetchall()]   
        return names

    @classmethod
    @dbcursor
    def delete_repo(cls, name, cursor):
        """Deletes a repo.
        """
        obj_table, ref_table =  MysqlRepo._table_names(name)
        cursor.execute("DROP TABLES {}, {}".format(obj_table, ref_table))
