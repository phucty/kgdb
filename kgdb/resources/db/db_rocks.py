# import gc
# import os.path
# import random
# import shutil
# from collections import defaultdict
# from dataclasses import asdict
# from typing import Any, Callable, List, Optional

# import numpy
# import rocksdb
# from pyroaring import BitMap
# from tqdm import tqdm

# from kgdb.config import config as cf
# from kgdb.resources.db.db_lmdb import LMDBWorker
# from kgdb.resources.db.utils import (
#     DBSpec,
#     OperationRocksdb,
#     OperatorUpdateCounter,
#     OperatorUpdateSetBitMap,
#     ToBytes,
#     deserialize_key,
#     deserialize_value,
#     serialize_key,
#     serialize_value,
# )
# from kgdb.utils import io_worker as iw


# class RocksDBWorker:
#     def __init__(
#         self,
#         dir_db: str,
#         db_schema: Optional[List[DBSpec]] = None,
#         read_only: bool = False,
#         buff_limit: int = cf.BUFF_LIMIT,
#         create_new: bool = False,
#     ):
#         if dir_db[-1] != "/":
#             dir_db = f"{dir_db}/"

#         if create_new:
#             if os.path.exists(dir_db):
#                 shutil.rmtree(dir_db)
#         else:
#             if not os.path.exists(dir_db):
#                 create_new = True

#         self.dir_db = dir_db
#         iw.create_dir(self.dir_db)

#         self.metadata_file = f"{self.dir_db}metadata.json"
#         iw.create_dir(self.metadata_file)

#         if create_new:
#             if db_schema is None:
#                 raise ValueError
#             self.db_schema = {db_spec.name: db_spec for db_spec in db_schema}
#             self.buff_limit = buff_limit
#             json_obj = {
#                 "db_schema": [asdict(db_i) for db_i in self.db_schema.values()],
#                 "buff_limit": self.buff_limit,
#             }
#             self._save_metadata_info(json_obj)
#         else:
#             self.db_schema, self.buff_limit = self._load_metadata_info()

#         self.db_schema["db_size"] = DBSpec(name="db_size")
#         self.read_only = read_only
#         opts = rocksdb.Options(
#             create_if_missing=True,
#             write_buffer_size=self.buff_limit,
#             max_open_files=-1,
#             max_background_flushes=8,
#             max_background_compactions=8,
#             target_file_size_base=512 * 1024 * 1024,
#             # compression=rocksdb.CompressionType.no_compression,
#             compression=rocksdb.CompressionType.lz4hc_compression,
#             max_write_buffer_number=6,
#             min_write_buffer_number_to_merge=2,
#         )
#         if create_new:
#             self.env = rocksdb.DB(dir_db, opts, read_only=read_only)
#         else:
#             column_families = self.get_column_family_opts()
#             self.env = rocksdb.DB(
#                 dir_db,
#                 opts,
#                 read_only=read_only,
#                 column_families=column_families,
#             )

#         self.dbs = self.init_column_families()
#         self.init_db_sizes()

#         self.buff_size = 0

#         self.buff_db_size = {
#             column_name: 0
#             for column_name in self.dbs.keys()
#             if column_name != "db_size"
#         }
#         self.buff_batch = rocksdb.WriteBatch()

#     def _copy_from(self, old_db):
#         for column_name in old_db.get_column_names():
#             if column_name in {b"default", b"db_size"}:
#                 continue
#             column_name = column_name.decode(cf.ENCODING)
#             for k, v in tqdm(
#                 old_db.iter_db(column_name),
#                 total=old_db.get_column_size(column_name),
#                 desc=str(column_name),
#             ):
#                 self.put_buff(column_name, k, v)
#         self.save_buff()

#     def get_column_names(self):
#         return {handle.name for handle in self.env.column_families}

#     def get_db(self, column_name: str):
#         if self.dbs.get(column_name) is None:
#             raise ValueError
#         return self.dbs[column_name]

#     def get_db_schema(self, column_name: str):
#         if self.db_schema.get(column_name) is None:
#             raise ValueError
#         return self.db_schema[column_name]

#     def _save_metadata_info(self, json_obj: dict):
#         iw.save_json_file(self.metadata_file, json_obj)

#     def _load_metadata_info(self):
#         json_obj = iw.read_json_file(self.metadata_file)
#         db_schema = {obj["name"]: DBSpec(**obj) for obj in json_obj["db_schema"]}
#         buff_limit = json_obj["buff_limit"]
#         return db_schema, buff_limit

#     def add_column_specs(self, col_specs: List[DBSpec]):
#         if self.read_only:
#             raise ValueError("Error: Read only")
#         for col_spec in col_specs:
#             if self.db_schema.get(col_spec.name):
#                 continue
#             self.db_schema[col_spec.name] = col_spec
#         json_obj = {
#             "db_schema": [asdict(db_i) for db_i in self.db_schema.values()],
#             "buff_limit": self.buff_limit,
#         }
#         self._save_metadata_info(json_obj)
#         self.dbs = self.init_column_families()
#         self.init_db_sizes()

#     def get_column_family_opts(self):
#         db_dict = {}
#         col_opts = rocksdb.ColumnFamilyOptions()
#         col_opts.merge_operator = OperatorUpdateCounter()
#         db_dict[b"db_size"] = col_opts

#         for db_spec in self.db_schema.values():
#             if db_spec.name == "db_size":
#                 continue

#             # if db_spec.integerkey:
#             #     col_opts = rocksdb.ColumnFamilyOptions(comparator=IntegerComparator())
#             # else:
#             col_opts = rocksdb.ColumnFamilyOptions(
#                 # compression=rocksdb.CompressionType.no_compression,
#                 compression=rocksdb.CompressionType.lz4hc_compression,
#                 write_buffer_size=self.buff_limit,
#                 target_file_size_base=512 * 1024 * 1024,
#                 # max_write_buffer_number=6,
#                 # min_write_buffer_number_to_merge=2,
#             )

#             # if db_spec.prefix_len:
#             #     col_opts.prefix_extractor = StaticPrefix(prefix_len=db_spec.prefix_len)

#             if db_spec.bytes_value == ToBytes.INT_BITMAP:
#                 col_opts.merge_operator = OperatorUpdateSetBitMap()

#             db_dict[db_spec.name.encode(cf.ENCODING)] = col_opts
#         return db_dict

#     def init_column_families(self):
#         db_dict = {}
#         column = self.env.get_column_family(b"db_size")
#         if column:
#             db_dict["db_size"] = column
#         else:
#             col_opts = rocksdb.ColumnFamilyOptions()
#             col_opts.merge_operator = OperatorUpdateCounter()
#             db_dict["db_size"] = self.env.create_column_family(b"db_size", col_opts)

#         for db_spec in self.db_schema.values():
#             if db_spec.name == "db_size":
#                 continue
#             column = self.env.get_column_family(db_spec.name.encode(cf.ENCODING))
#             if column:
#                 db_dict[db_spec.name] = column
#                 continue
#             # if db_spec.integerkey:
#             #     col_opts = rocksdb.ColumnFamilyOptions(comparator=IntegerComparator())
#             # else:
#             col_opts = rocksdb.ColumnFamilyOptions(
#                 # compression=rocksdb.CompressionType.no_compression,
#                 compression=rocksdb.CompressionType.lz4hc_compression,
#                 write_buffer_size=self.buff_limit,
#                 target_file_size_base=512 * 1024 * 1024,
#                 # max_write_buffer_number=6,
#                 # min_write_buffer_number_to_merge=2,
#             )

#             # if db_spec.prefix_len:
#             #     col_opts.prefix_extractor = StaticPrefix(prefix_len=db_spec.prefix_len)

#             if db_spec.bytes_value == ToBytes.INT_BITMAP:
#                 col_opts.merge_operator = OperatorUpdateSetBitMap()

#             db_dict[db_spec.name] = self.env.create_column_family(
#                 db_spec.name.encode(cf.ENCODING), col_opts
#             )
#         return db_dict

#     def init_db_sizes(self):
#         for db_spec in self.db_schema.values():
#             if db_spec.name != "db_size":
#                 cur_value = self.get_value("db_size", db_spec.name)
#                 if cur_value is None:
#                     self.put("db_size", key=db_spec.name, value=0)

#     def get_column_size(self, column_name: str):
#         # db_size = int(
#         #     self.env.get_property(b"rocksdb.estimate-num-keys").decode(cf.ENCODING)
#         # )
#         db_schema = self.get_db_schema(column_name)
#         db_size = self.get_value("db_size", key_obj=db_schema.name)
#         return db_size

#     def view(self):
#         stats = {
#             column_name: self.get_column_size(column_name)
#             for column_name in self.dbs.keys()
#             if column_name != "db_size"
#         }
#         stats = sorted(stats.items())
#         for k, v in stats:
#             iw.print_status(f"{k}: {v:,}")
#         return stats

#     def update_db_size(self):
#         for column_name in self.dbs.keys():
#             if column_name != "db_size":
#                 db_size = 0
#                 for _ in tqdm(
#                     self.iter_db(column_name, get_values=False),
#                     total=self.get_column_size(column_name),
#                 ):
#                     db_size += 1
#                 print(f"{column_name}: {db_size:,}")
#                 self.put("db_size", column_name, db_size)

#     def compact(self, update_db_size=False):
#         print(self.dir_db)
#         self.view()
#         self.env.compact_range()
#         if update_db_size:
#             self.update_db_size()
#             self.view()

#     def close(self):
#         self.env.close()

#     def to_lmdb(self, compress_columns=None, map_size=cf.SIZE_1GB * 10, step=100_000):
#         if compress_columns is None:
#             compress_columns = []

#         # Update db_schema
#         for k in compress_columns:
#             if k not in self.db_schema:
#                 continue
#             self.db_schema[k].compress_value = True

#         lmdb_obj = LMDBWorker(
#             db_file=self.dir_db,
#             db_schema=list(self.db_schema.values()),
#             readonly=False,
#             buff_limit=cf.LMDB_BUFF_LIMIT,  # 10000,  # ,  #
#             map_size=map_size,
#         )
#         iw.print_status(self.dir_db)
#         for column_name in self.dbs.keys():
#             if column_name == "db_size":
#                 continue

#             def update_desc():
#                 return f"{column_name} - {lmdb_obj.buff_size / lmdb_obj.buff_limit * 100:.0f}%"

#             db_schema = self.get_db_schema(column_name)
#             p_bar = tqdm(total=self.get_column_size(column_name))
#             for i, (k, v) in enumerate(self.iter_db(column_name)):
#                 if i and i % step == 0:
#                     p_bar.update(step)
#                     p_bar.set_description(desc=update_desc())
#                 k = serialize_key(
#                     k,
#                     integerkey=db_schema.integerkey,
#                     combinekey=db_schema.combinekey,
#                     is_64bit=db_schema.is_64bit,
#                 )
#                 v = serialize_value(
#                     v,
#                     bytes_value=db_schema.bytes_value,
#                     compress_value=db_schema.compress_value,
#                 )
#                 lmdb_obj.add(column=column_name, key=k, value=v)
#             lmdb_obj.save_buff()
#             p_bar.close()
#         lmdb_obj.compress()

#     def get_random_item(self, column_name: str):
#         key = self.get_random_key(column_name)
#         value = self.get_value(column_name, key)
#         return key, value

#     def get_random_value(self, column_name: str):
#         key = self.get_random_key(column_name)
#         value = self.get_value(column_name, key)
#         return value

#     def get_random_key(self, column_name: str):
#         db = self.get_db(column_name)
#         db_schema = self.get_db_schema(column_name)
#         db_size = self.get_column_size(column_name)
#         random_index = random.randint(0, db_size)

#         cur = self.env.iterkeys(db)
#         cur.seek_to_first()
#         n_steps = random_index
#         i = 0
#         db_item = None
#         key = None
#         for item in cur:
#             if i >= n_steps:
#                 db_item = item
#                 break
#             i += 1

#         if isinstance(db_item, tuple):
#             key = db_item[1]

#         if key is None:
#             return None
#         key = deserialize_key(
#             key,
#             integerkey=db_schema.integerkey,
#             combinekey=db_schema.combinekey,
#             is_64bit=db_schema.is_64bit,
#         )
#         return key

#     def get_values(
#         self,
#         column_name: str,
#         key_objs: Any,
#         get_deserialize: bool = True,
#         to_list: bool = False,
#         get_values_only: bool = False,
#     ):
#         db = self.get_db(column_name)
#         db_schema = self.get_db_schema(column_name)
#         if isinstance(key_objs, numpy.ndarray) or isinstance(key_objs, BitMap):
#             key_objs = list(key_objs)

#         if get_values_only:
#             responds = set()
#         else:
#             responds = dict()

#         if not (
#             isinstance(key_objs, list)
#             or isinstance(key_objs, set)
#             or isinstance(key_objs, tuple)
#         ):
#             return responds

#         if not isinstance(key_objs[0], bytes):
#             key_objs_bytes = [
#                 (
#                     db,
#                     serialize_key(
#                         k,
#                         integerkey=db_schema.integerkey,
#                         combinekey=db_schema.combinekey,
#                         is_64bit=db_schema.is_64bit,
#                     ),
#                 )
#                 for k in key_objs
#             ]
#         else:
#             key_objs_bytes = key_objs
#         values = self.env.multi_get(key_objs_bytes)
#         for i, v in enumerate(values.values()):
#             if v is None:
#                 continue
#             if get_deserialize:
#                 try:
#                     v = deserialize_value(
#                         v,
#                         bytes_value=db_schema.bytes_value,
#                         compress_value=db_schema.compress_value,
#                     )
#                     if to_list and db_schema.bytes_value in {
#                         ToBytes.INT_BITMAP,
#                         ToBytes.INT_NUMPY,
#                     }:
#                         v = list(v)
#                 except Exception as message:
#                     print(message)
#             if get_values_only:
#                 responds.add(v)
#             else:
#                 responds[key_objs[i]] = v

#         return responds

#     def get_value(
#         self,
#         column_name: str,
#         key_obj: Any,
#         get_deserialize: bool = True,
#         get_memory_size: bool = False,
#         to_list: bool = False,
#     ):
#         db_schema = self.get_db_schema(column_name)
#         db = self.get_db(column_name)
#         if not isinstance(key_obj, bytes):
#             key_obj = serialize_key(
#                 key_obj,
#                 integerkey=db_schema.integerkey,
#                 combinekey=db_schema.combinekey,
#                 is_64bit=db_schema.is_64bit,
#             )
#         if get_memory_size:
#             responds = 0
#         else:
#             responds = None

#         if not key_obj:
#             return responds

#         try:
#             value_obj = self.env.get((db, key_obj))
#             if not value_obj:
#                 return responds
#             responds = value_obj
#             if get_memory_size:
#                 return len(responds)
#             if get_deserialize:
#                 responds = deserialize_value(
#                     value_obj,
#                     bytes_value=db_schema.bytes_value,
#                     compress_value=db_schema.compress_value,
#                 )
#                 if to_list and db_schema.bytes_value in {
#                     ToBytes.INT_BITMAP,
#                     ToBytes.INT_NUMPY,
#                 }:
#                     responds = list(responds)
#         except Exception as message:
#             print(message)
#         return responds

#     def head(
#         self,
#         column_name: str,
#         n: int,
#         from_i: int = 0,
#     ):
#         respond = defaultdict()
#         for i, (k, v) in enumerate(self.iter_db(column_name, from_i=from_i)):
#             respond[k] = v
#             if i == n - 1:
#                 break
#         return respond

#     def iter_db_prefix(
#         self, column_name: str, prefix: Any, get_values=True, to_list=False
#     ):
#         db = self.get_db(column_name)
#         db_schema = self.get_db_schema(column_name)

#         if get_values:
#             cur = self.env.iteritems(db)
#         else:
#             cur = self.env.iterkeys(db)

#         if not isinstance(prefix, bytes):
#             prefix = serialize_key(
#                 prefix,
#                 integerkey=db_schema.integerkey,
#                 combinekey=db_schema.combinekey,
#                 is_64bit=db_schema.is_64bit,
#             )
#         cur.seek(prefix)
#         for db_obj in cur:
#             key, value = None, None
#             if get_values:
#                 key, value = db_obj
#             else:
#                 key = db_obj
#             key = key[1]
#             if not key.startswith(prefix):
#                 break
#             try:
#                 key = deserialize_key(
#                     key,
#                     integerkey=db_schema.integerkey,
#                     combinekey=db_schema.combinekey,
#                     is_64bit=db_schema.is_64bit,
#                 )
#                 if get_values:
#                     value = deserialize_value(
#                         value,
#                         bytes_value=db_schema.bytes_value,
#                         compress_value=db_schema.compress_value,
#                     )
#                     if to_list and db_schema.bytes_value in {
#                         ToBytes.INT_BITMAP,
#                         ToBytes.INT_NUMPY,
#                     }:
#                         value = list(value)
#                     yield key, value
#                 else:
#                     yield key
#             except Exception as message:
#                 print(message)

#     def iter_db(
#         self,
#         column_name: str,
#         get_keys: bool = True,
#         get_values: bool = True,
#         deserialize_obj: bool = True,
#         from_i: int = 0,
#         to_i: int = -1,
#         to_list: bool = False,
#     ):
#         db = self.get_db(column_name)
#         db_schema = self.get_db_schema(column_name)

#         if to_i == -1:
#             to_i = self.get_column_size(column_name)

#         if get_values and get_keys:
#             cur = self.env.iteritems(db)
#         elif get_values:
#             cur = self.env.itervalues(db)
#         else:
#             cur = self.env.iterkeys(db)

#         if db_schema.integerkey and from_i != 0:
#             cur.seek(
#                 serialize_key(from_i, integerkey=True, combinekey=db_schema.combinekey)
#             )
#             i = from_i
#         else:
#             i = 0
#             cur.seek_to_first()

#         for db_obj in cur:
#             if i < from_i:
#                 i += 1
#                 continue
#             if i >= to_i:
#                 break
#             key, value = None, None

#             try:
#                 if get_values and get_keys:
#                     key, value = db_obj
#                     key = key[1]
#                 elif get_values:
#                     value = db_obj
#                 else:
#                     key = db_obj
#                     key = key[1]
#                 if key is not None:
#                     key = deserialize_key(
#                         key,
#                         integerkey=db_schema.integerkey,
#                         combinekey=db_schema.combinekey,
#                         is_64bit=db_schema.is_64bit,
#                     )
#                 if value is not None and deserialize_obj:
#                     value = deserialize_value(
#                         value,
#                         bytes_value=db_schema.bytes_value,
#                         compress_value=db_schema.compress_value,
#                     )
#                     if to_list and db_schema.bytes_value in {
#                         ToBytes.INT_BITMAP,
#                         ToBytes.INT_NUMPY,
#                     }:
#                         value = list(value)

#                 if get_values and get_keys:
#                     return_obj = (key, value)
#                     yield return_obj
#                 elif get_values:
#                     yield value
#                 else:
#                     yield key
#             except UnicodeDecodeError:
#                 print(f"UnicodeDecodeError: {i}")
#             except Exception:
#                 print(i)
#                 raise Exception
#             i += 1

#     def is_available(self, column_name: str, key_obj: Any):
#         db = self.get_db(column_name)
#         db_schema = self.get_db_schema(column_name)
#         if not isinstance(key_obj, bytes):
#             key_obj = serialize_key(
#                 key_obj,
#                 integerkey=db_schema.integerkey,
#                 combinekey=db_schema.combinekey,
#                 is_64bit=db_schema.is_64bit,
#             )
#         if key_obj:
#             try:
#                 value_obj = self.env.get((db, key_obj))
#                 if value_obj:
#                     return True
#             except Exception as message:
#                 print(message)
#         return False

#     def _call_back_rocksdb_func(
#         self, func: Callable, column_name: bool, key: Any, value: Any = None
#     ):
#         db = self.get_db(column_name)
#         db_schema = self.get_db_schema(column_name)
#         key = serialize_key(
#             key,
#             integerkey=db_schema.integerkey,
#             combinekey=db_schema.combinekey,
#             is_64bit=db_schema.is_64bit,
#         )
#         if value is not None:
#             value = serialize_value(
#                 value,
#                 bytes_value=db_schema.bytes_value,
#                 compress_value=db_schema.compress_value,
#             )
#             func((db, key), value)
#         else:
#             func((db, key))

#     def _call_back_rocksdb_func_batch(
#         self,
#         operation: OperationRocksdb,
#         items: Any,
#         message: str = "",
#         buff_limit: int = cf.SIZE_1GB,
#         show_progress: bool = True,
#         step: int = 1000,
#     ):
#         batch = rocksdb.WriteBatch()
#         buff_size = 0
#         p_bar = None

#         def update_desc():
#             return f"{message} buffer: {buff_size / buff_limit * 100:.0f}%"

#         if show_progress:
#             p_bar = tqdm(desc=update_desc(), total=len(items))

#         for i, item in enumerate(items):
#             if show_progress:
#                 p_bar.update()
#                 if i and i % step == 0:
#                     p_bar.set_description(desc=update_desc())

#             if operation == OperationRocksdb.DELETE:
#                 k = item
#                 batch.delete(k)
#             else:
#                 k, v = item
#                 buff_size += len(k) + len(v)
#                 if operation == OperationRocksdb.PUT:
#                     batch.put(k, v)
#                 elif operation == OperationRocksdb.MERGE:
#                     batch.merge(k, v)

#             if buff_size >= buff_limit:
#                 self.env.write(batch)
#                 batch.clear()
#                 buff_size = 0

#         if buff_size:
#             self.env.write(batch)
#             batch.clear()

#         if show_progress:
#             p_bar.set_description(desc=update_desc())
#             p_bar.close()

#     def _call_back_buff_func(
#         self,
#         func: Callable,
#         column_name: str,
#         key: Any,
#         value: Any,
#         check_exist: bool = False,
#         update_db_size: int = 1,
#     ):
#         db_schema = self.get_db_schema(column_name)
#         db = self.get_db(column_name)

#         if not isinstance(key, bytes):
#             key = serialize_key(
#                 key,
#                 integerkey=db_schema.integerkey,
#                 combinekey=db_schema.combinekey,
#                 is_64bit=db_schema.is_64bit,
#             )

#         if not check_exist or (check_exist and not self.is_available(column_name, key)):
#             self.buff_db_size[column_name] += update_db_size

#         if not isinstance(value, bytes):
#             value = serialize_value(
#                 value,
#                 bytes_value=db_schema.bytes_value,
#                 compress_value=db_schema.compress_value,
#             )
#         self.buff_size += len(key) + len(value)
#         func((db, key), value)

#         if self.buff_size > self.buff_limit:
#             self.save_buff()

#     def put(self, column_name: str, key: Any, value: Any, update_db_size: bool = True):
#         is_available = self.is_available(column_name, key)
#         self._call_back_rocksdb_func(self.env.put, column_name, key, value)
#         if update_db_size and column_name != "db_size" and not is_available:
#             self.merge("db_size", column_name, 1)

#     def merge(
#         self, column_name: str, key: Any, value: Any, update_db_size: bool = True
#     ):
#         is_available = self.is_available(column_name, key)
#         self._call_back_rocksdb_func(self.env.merge, column_name, key, value)
#         if update_db_size and column_name != "db_size" and not is_available:
#             self.merge("db_size", column_name, 1)

#     def delete(self, column_name: str, key: Any, update_db_size: bool = True):
#         is_available = self.is_available(column_name, key)
#         self._call_back_rocksdb_func(self.env.delete, column_name, key)
#         if update_db_size and column_name != "db_size" and is_available:
#             self.merge("db_size", column_name, -1)

#     def drop_column(self, column_name: str):
#         db = self.get_db(column_name)
#         self.env.drop_column_family(db)
#         self.delete("db_size", column_name)

#         if self.db_schema.get(column_name):
#             del self.db_schema[column_name]
#             json_obj = {
#                 "db_schema": [asdict(db_i) for db_i in self.db_schema.values()],
#                 "buff_limit": self.buff_limit,
#             }
#             self._save_metadata_info(json_obj)

#         if self.dbs.get(column_name):
#             del self.dbs[column_name]

#     def save_buff(self):
#         self.env.write(self.buff_batch)
#         self.buff_batch.clear()
#         self.buff_size = 0
#         # del self.buff_batch
#         gc.collect()
#         # self.buff_batch = rocksdb.WriteBatch()

#         # Update db size
#         for column_name in self.dbs.keys():
#             if column_name == "db_size" or not self.buff_db_size[column_name]:
#                 continue
#             self.merge("db_size", column_name, self.buff_db_size[column_name])
#             self.buff_db_size[column_name] = 0

#     def put_buff(
#         self,
#         column_name: str,
#         key: Any,
#         value: Any,
#         check_exist: bool = False,
#         update_db_size=1,
#     ):
#         self._call_back_buff_func(
#             func=self.buff_batch.put,
#             column_name=column_name,
#             key=key,
#             value=value,
#             check_exist=check_exist,
#             update_db_size=update_db_size,
#         )

#     def merge_buff(self, column_name: str, key: Any, value: Any):
#         update_db_size = 1
#         if self.is_available(column_name, key):
#             exist_value = self.get_value(column_name, key)
#             value = exist_value | value
#             if len(value) <= len(exist_value):
#                 return
#             update_db_size = 0

#         self.put_buff(column_name, key, value, update_db_size=update_db_size)

#         # self._call_back_buff_func(
#         #     func=self.buff_batch.merge,
#         #     column_name=column_name,
#         #     key=key,
#         #     value=value,
#         #     check_exist=check_exist,
#         #     update_db_size=1,
#         # )

#     def delete_buff(
#         self, column_name: str, key: Any, value: Any, check_exist: bool = False
#     ):
#         self._call_back_buff_func(
#             func=self.buff_batch.delete,
#             column_name=column_name,
#             key=key,
#             value=value,
#             check_exist=check_exist,
#             update_db_size=-1,
#         )
