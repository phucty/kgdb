# from __future__ import annotations

# import gc
# import os
# import random
# from collections import defaultdict
# from dataclasses import asdict, dataclass
# from typing import Any, List, Optional

# import lmdb
# import numpy
# from tqdm import tqdm

# from kgdb.config import config as cf
# from kgdb.resources.db.utils import (
#     DBSpec,
#     OperationRocksdb,
#     OperatorUpdateCounter,
#     OperatorUpdateSetBitMap,
#     ToBytes,
#     deserialize,
#     deserialize_key,
#     deserialize_value,
#     preprocess_data_before_dump,
#     serialize,
#     serialize_key,
#     serialize_value,
# )
# from kgdb.utils import io_worker as iw


# class LMDBWorker:
#     def __init__(
#         self,
#         db_file: str,
#         db_schema: Optional[List[DBSpec]] = None,
#         map_size: int = cf.LMDB_MAP_SIZE,
#         readonly: bool = True,
#         buff_limit: int = cf.LMDB_BUFF_LIMIT,
#     ):
#         if db_file.endswith("/"):
#             db_file = db_file[:-1]
#         if not db_file.endswith(".lmdb"):
#             db_file += ".lmdb"
#         self.db_file = db_file
#         iw.create_dir(self.db_file)

#         self.metadata_file = db_file + ".json"
#         iw.create_dir(self.metadata_file)
#         if db_schema:
#             self.schema = {db_spec.name: db_spec for db_spec in db_schema}
#             self.buff_limit = buff_limit
#             self.save_metadata_info(db_schema, buff_limit)
#         else:
#             self.schema, self.buff_limit = self.load_metadata_info()

#         self.max_db = len(self.schema)
#         self.map_size = map_size
#         self.readonly = readonly
#         self.env = lmdb.open(
#             self.db_file,
#             map_async=True,
#             writemap=True,
#             map_size=self.map_size,
#             subdir=False,
#             lock=False,
#             max_dbs=self.max_db,
#             readonly=self.readonly,
#         )
#         self.dbs = self.init_sub_databases()

#         self.buff = defaultdict(list)
#         self.buff_size = 0

#     def save_metadata_info(self, db_schema: List[DBSpec], buff_limit: int):
#         json_obj = {
#             "db_schema": [asdict(db_i) for db_i in db_schema],
#             "buff_limit": buff_limit,
#         }
#         iw.save_json_file(self.metadata_file, json_obj)

#     def load_metadata_info(self):
#         json_obj = iw.read_json_file(self.metadata_file)
#         db_schema = {obj["name"]: DBSpec(**obj) for obj in json_obj["db_schema"]}
#         buff_limit = json_obj["buff_limit"]
#         return db_schema, buff_limit

#     def init_sub_databases(self):
#         db_dict = {}
#         for db_spec in self.schema.values():
#             db_dict[db_spec.name] = self.env.open_db(
#                 db_spec.name.encode(cf.ENCODING), integerkey=db_spec.integerkey
#             )
#         return db_dict

#     def get_db_size(self):
#         tmp = self.env.info().get("map_size")
#         if not tmp:
#             return "Unknown"

#         return iw.get_size_of_file(tmp)

#     def close(self):
#         self.save_buff()
#         self.env.close()

#     def compress(self):
#         """
#         Copy current env to new one (reduce file size)
#         :return:
#         :rtype:
#         """
#         # print(self.env.stat())
#         iw.print_status(f"Compress {self.db_file}")
#         old_size = 0
#         if self.env.info().get("map_size"):
#             old_size = self.env.info().get("map_size")
#             iw.print_status(f"Old Size: { iw.get_size_of_file(old_size)}")
#         new_dir = self.db_file + ".copy"
#         self.env.copy(path=new_dir, compact=True)
#         try:
#             if os.path.exists(self.db_file):
#                 os.remove(self.db_file)
#         except Exception as message:
#             print(message)
#         os.rename(new_dir, self.db_file)
#         new_file = LMDBWorker(self.db_file)
#         new_size = self.env.info().get("map_size")
#         iw.print_status(
#             f"New file: {iw.get_size_of_file(new_size)} - compressed:{(old_size - new_size)/ old_size*100:.2f}%"
#         )

#     def get_random_key(self, db_name: str):
#         with self.env.begin(db=self.dbs[db_name], write=False) as txn:
#             random_index = random.randint(0, self.get_column_size(db_name))
#             cur = txn.cursor()
#             cur.first()
#             key = deserialize_key(
#                 cur.key(),
#                 integerkey=self.schema[db_name].integerkey,
#                 is_64bit=self.schema[db_name].is_64bit,
#             )
#             for i, k in enumerate(cur.iternext(values=False)):
#                 if i == random_index:
#                     key = deserialize_key(k, integerkey=self.schema[db_name].integerkey)
#                     break
#         return key

#     def get_iter_integerkey(
#         self, db_name: str, from_i: int = 0, to_i: int = -1, get_values: bool = True
#     ):
#         with self.env.begin(db=self.dbs[db_name], write=False) as txn:
#             if to_i == -1:
#                 to_i = self.get_column_size(db_name)
#             cur = txn.cursor()
#             cur.set_range(
#                 serialize_key(
#                     from_i, integerkey=True, is_64bit=self.schema[db_name].is_64bit
#                 )
#             )
#             for item in cur.iternext(values=get_values):
#                 if get_values:
#                     key, value = item
#                 else:
#                     key = item
#                 key = deserialize_key(
#                     key, integerkey=True, is_64bit=self.schema[db_name].is_64bit
#                 )
#                 if key > to_i:
#                     break
#                 if get_values:
#                     value = deserialize_value(
#                         value,
#                         bytes_value=self.schema[db_name].bytes_value,
#                         compress_value=self.schema[db_name].compress_value,
#                     )
#                     yield key, value
#                 else:
#                     yield key
#             cur.next()

#     def get_iter_with_prefix(self, db_name: str, prefix: str, get_values: bool = True):
#         with self.env.begin(db=self.dbs[db_name], write=False) as txn:
#             cur = txn.cursor()
#             prefix = serialize_key(
#                 prefix,
#                 integerkey=self.schema[db_name].integerkey,
#                 is_64bit=self.schema[db_name].is_64bit,
#             )
#             cur.set_range(prefix)

#             while cur.key().startswith(prefix) is True:
#                 try:
#                     if cur.key() and not cur.key().startswith(prefix):
#                         continue
#                     key = deserialize_key(
#                         cur.key(),
#                         integerkey=self.schema[db_name].integerkey,
#                         is_64bit=self.schema[db_name].is_64bit,
#                     )
#                     if get_values:
#                         value = deserialize_value(
#                             cur.value(),
#                             bytes_value=self.schema[db_name].bytes_value,
#                             compress_value=self.schema[db_name].compress_value,
#                         )
#                         yield key, value
#                     else:
#                         yield key
#                 except Exception as message:
#                     print(message)
#                 cur.next()

#     def is_available(self, db_name: str, key_obj: str | int | bytes):
#         with self.env.begin(db=self.dbs[db_name]) as txn:
#             key_obj = serialize_key(
#                 key_obj,
#                 integerkey=self.schema[db_name].integerkey,
#                 is_64bit=self.schema[db_name].is_64bit,
#             )
#             if key_obj:
#                 try:
#                     value_obj = txn.get(key_obj)
#                     if value_obj:
#                         return True
#                 except Exception as message:
#                     print(message)
#         return False

#     def get_memory_size(self, db_name: str, key_obj: str | int | bytes):
#         with self.env.begin(db=self.dbs[db_name], buffers=True) as txn:
#             key_obj = serialize_key(
#                 key_obj,
#                 integerkey=self.schema[db_name].integerkey,
#                 is_64bit=self.schema[db_name].is_64bit,
#             )
#             responds = None
#             if key_obj:
#                 try:
#                     value_obj = txn.get(key_obj)
#                     if value_obj:
#                         return len(value_obj)
#                 except Exception as message:
#                     print(message)

#             return responds

#     def get_values(
#         self,
#         db_name: str,
#         key_objs: List[: str | int | bytes],
#         get_deserialize: bool = True,
#     ):
#         with self.env.begin(db=self.dbs[db_name], buffers=True) as txn:
#             if isinstance(key_objs, numpy.ndarray):
#                 key_objs = key_objs.tolist()
#             responds = dict()

#             if not (
#                 isinstance(key_objs, list)
#                 or isinstance(key_objs, set)
#                 or isinstance(key_objs, tuple)
#             ):
#                 return responds

#             key_objs = [
#                 serialize_key(k, integerkey=self.schema[db_name].integerkey)
#                 for k in key_objs
#             ]
#             for k, v in txn.cursor(self.dbs[db_name]).getmulti(key_objs):
#                 if not v:
#                     continue
#                 k = deserialize_key(
#                     k,
#                     integerkey=self.schema[db_name].integerkey,
#                     is_64bit=self.schema[db_name].is_64bit,
#                 )
#                 if get_deserialize:
#                     try:
#                         v = deserialize_value(
#                             v,
#                             bytes_value=self.schema[db_name].bytes_value,
#                             compress_value=self.schema[db_name].compress_value,
#                         )
#                     except Exception as message:
#                         print(message)
#                 responds[k] = v

#         return responds

#     def get_value(
#         self, db_name: str, key_obj: str | int | bytes, get_deserialize: bool = True
#     ):
#         with self.env.begin(db=self.dbs[db_name], buffers=True) as txn:
#             key_obj = serialize_key(
#                 key_obj,
#                 integerkey=self.schema[db_name].integerkey,
#                 is_64bit=self.schema[db_name].is_64bit,
#             )
#             responds = None
#             if not key_obj:
#                 return responds
#             try:
#                 value_obj = txn.get(key_obj)
#                 if not value_obj:
#                     return responds
#                 responds = value_obj
#                 if get_deserialize:
#                     responds = deserialize_value(
#                         value_obj,
#                         bytes_value=self.schema[db_name].bytes_value,
#                         compress_value=self.schema[db_name].compress_value,
#                     )

#             except Exception as message:
#                 print(message)

#         return responds

#     def head(
#         self,
#         db_name: str,
#         n: int = 1,
#         from_i: int = 0,
#     ):
#         respond = defaultdict()
#         for i, (k, v) in enumerate(self.get_db_iter(db_name, from_i=from_i)):
#             respond[k] = v
#             if i == n - 1:
#                 break
#         return respond

#     def get_db_iter(
#         self,
#         db_name: str,
#         get_values: bool = True,
#         deserialize_obj: bool = True,
#         from_i: bool = 0,
#         to_i: bool = -1,
#     ):

#         if to_i == -1:
#             to_i = self.get_column_size(self.dbs[db_name])

#         with self.env.begin(db=self.dbs[db_name]) as txn:
#             cur = txn.cursor()
#             for i, db_obj in enumerate(cur.iternext(values=get_values)):
#                 if i < from_i:
#                     continue
#                 if i >= to_i:
#                     break

#                 if get_values:
#                     key, value = db_obj
#                 else:
#                     key = db_obj
#                 try:

#                     if deserialize_obj:
#                         key = deserialize_key(
#                             key,
#                             integerkey=self.schema[db_name].integerkey,
#                             is_64bit=self.schema[db_name].is_64bit,
#                         )
#                         if get_values:
#                             value = deserialize_value(
#                                 value,
#                                 bytes_value=self.schema[db_name].bytes_value,
#                                 compress_value=self.schema[db_name].compress_value,
#                             )
#                     if get_values:
#                         return_obj = (key, value)
#                         yield return_obj
#                     else:
#                         yield key
#                 except UnicodeDecodeError:
#                     print(f"UnicodeDecodeError: {i}")
#                 except Exception:
#                     print(i)
#                     raise Exception

#     def get_column_size(self, db_name: str):
#         with self.env.begin(db=self.dbs[db_name]) as txn:
#             return txn.stat()["entries"]

#     def delete(self, db_name: str, key: str | int | bytes, with_prefix: bool = False):
#         if not (
#             isinstance(key, list) or isinstance(key, set) or isinstance(key, tuple)
#         ):
#             key = [key]

#         if with_prefix:
#             true_key = set()
#             for k in key:
#                 for tmp_k in self.get_iter_with_prefix(db_name, k, get_values=False):
#                     true_key.add(tmp_k)
#             if true_key:
#                 key = list(true_key)

#         deleted_items = 0
#         with self.env.begin(db=self.dbs[db_name], write=True, buffers=True) as txn:
#             for k in key:
#                 try:
#                     status = txn.delete(
#                         serialize_key(
#                             k,
#                             integerkey=self.schema[db_name].integerkey,
#                             is_64bit=self.schema[db_name].is_64bit,
#                         )
#                     )
#                     if status:
#                         deleted_items += 1
#                 except Exception as message:
#                     print(message)
#         return deleted_items

#     @staticmethod
#     def write(
#         env,
#         db,
#         data,
#         sort_key: bool = True,
#         integerkey: bool = False,
#         is_64bit: bool = False,
#         bytes_value: ToBytes = ToBytes.OBJ,
#         compress_value: bool = False,
#         one_sample_write: bool = False,
#     ):
#         data = preprocess_data_before_dump(
#             data,
#             bytes_value=bytes_value,
#             integerkey=integerkey,
#             is_64bit=is_64bit,
#             compress_value=compress_value,
#             sort_key=sort_key,
#         )
#         added_items = 0
#         try:
#             with env.begin(db=db, write=True, buffers=True) as txn:
#                 if not one_sample_write:
#                     _, added_items = txn.cursor().putmulti(data)
#                 else:
#                     for k, v in data:
#                         txn.put(k, v)
#                         added_items += 1
#         except lmdb.MapFullError:
#             curr_limit = env.info()["map_size"]
#             new_limit = curr_limit + cf.SIZE_1GB * 5
#             env.set_mapsize(new_limit)
#             return LMDBWorker.write(env, db, data, sort_key=False)
#         except lmdb.BadValsizeError:
#             print(lmdb.BadValsizeError)
#         except lmdb.BadTxnError:
#             if one_sample_write:
#                 return LMDBWorker.write(
#                     env,
#                     db,
#                     data,
#                     sort_key=False,
#                     one_sample_write=True,
#                 )
#         except Exception:
#             raise Exception
#         return added_items

#     @staticmethod
#     def write_with_buffer(
#         env: lmdb.Environment,
#         db: Any,
#         data: Any,
#         sort_key: bool = True,
#         integerkey: bool = False,
#         is_64bit: bool = False,
#         bytes_value: bool = ToBytes.OBJ,
#         compress_value: bool = False,
#         show_progress: bool = True,
#         step: int = 10000,
#         message: str = "DB Write",
#     ):
#         data = preprocess_data_before_dump(
#             data,
#             bytes_value=bytes_value,
#             integerkey=integerkey,
#             is_64bit=is_64bit,
#             compress_value=compress_value,
#             sort_key=sort_key,
#         )

#         def update_desc():
#             return f"{message} buffer: {buff_size / cf.LMDB_BUFF_LIMIT * 100:.0f}%"

#         p_bar = None
#         buff_size = 0
#         i_pre = 0
#         if show_progress:
#             p_bar = tqdm(total=len(data))

#         for i, (k, v) in enumerate(data):
#             if show_progress and i and i % step == 0:
#                 p_bar.update(step)
#                 p_bar.set_description(desc=update_desc())
#             buff_size += len(k) + len(v)

#             if buff_size >= cf.LMDB_BUFF_LIMIT:
#                 c = LMDBWorker.write(env, db, data[i_pre:i], sort_key=False)
#                 if c != len(data[i_pre:i]):
#                     print(
#                         f"WriteError: Missing data. Expected: {len(data[i_pre:i])} - Actual: {c}"
#                     )
#                 i_pre = i
#                 buff_size = 0

#         if buff_size:
#             LMDBWorker.write(env, db, data[i_pre:], sort_key=False)

#         if show_progress:
#             p_bar.update(len(data) % step)
#             p_bar.set_description(desc=update_desc())
#             p_bar.close()

#     def update_bulk_with_buffer(
#         self,
#         db_name,
#         data,
#         update_type=cf.DBUpdateType.SET,
#         show_progress=True,
#         step=10000,
#         message="",
#         buff_limit=cf.LMDB_BUFF_LIMIT,
#     ):
#         buff = []
#         p_bar = None
#         c_skip, c_update, c_new, c_buff = 0, 0, 0, 0

#         def update_desc():
#             return (
#                 f"{message}"
#                 f"|Skip:{c_skip:,}"
#                 f"|New:{c_new:,}"
#                 f"|Update:{c_update:,}"
#                 f"|Buff:{c_buff / buff_limit * 100:.0f}%"
#             )

#         if show_progress:
#             p_bar = tqdm(total=len(data), desc=update_desc())

#         for i, (k, v) in enumerate(data.items()):
#             if show_progress and i and i % step == 0:
#                 p_bar.update(step)
#                 p_bar.set_description(update_desc())

#             db_obj = self.get_value(db_name, k)
#             if update_type == cf.DBUpdateType.SET:
#                 if db_obj:
#                     db_obj = set(db_obj)
#                     v = set(v)
#                     if db_obj and len(v) <= len(db_obj) and db_obj.issuperset(v):
#                         c_skip += 1
#                         continue
#                     if db_obj:
#                         v.update(db_obj)
#                         c_update += 1
#                     else:
#                         c_new += 1
#                 else:
#                     c_new += 1
#             else:
#                 if db_obj:
#                     v += db_obj
#                     c_update += 1
#                 else:
#                     c_new += 1

#             k, v = serialize(
#                 k,
#                 v,
#                 integerkey=self.schema[db_name].integerkey,
#                 is_64bit=self.schema[db_name].is_64bit,
#                 bytes_value=self.schema[db_name].bytes_value,
#                 compress_value=self.schema[db_name].compress_value,
#             )

#             c_buff += len(k) + len(v)
#             buff.append((k, v))

#             if c_buff >= buff_limit:
#                 LMDBWorker.write(self.env, self.dbs[db_name], buff)
#                 buff = []
#                 c_buff = 0

#         if buff:
#             LMDBWorker.write(self.env, self.dbs[db_name], buff)
#         if show_progress:
#             p_bar.set_description(desc=update_desc())
#             p_bar.close()

#     def drop_db(self, db):
#         with self.env.begin(write=True) as in_txn:
#             in_txn.drop(db)
#             print(in_txn.stat())

#     def save_buff(self):
#         while len(self.buff):
#             db_name, buff = self.buff.popitem()
#             self.write(
#                 self.env,
#                 self.dbs[db_name],
#                 buff,
#                 integerkey=self.schema[db_name].integerkey,
#                 is_64bit=self.schema[db_name].is_64bit,
#                 bytes_value=self.schema[db_name].bytes_value,
#                 compress_value=self.schema[db_name].compress_value,
#             )

#         del self.buff
#         gc.collect()
#         self.buff = defaultdict(list)
#         self.buff_size = 0

#     def add(self, column, key, value):
#         if not isinstance(value, bytes):
#             value = serialize_value(
#                 value,
#                 bytes_value=self.schema[column].bytes_value,
#                 compress_value=self.schema[column].compress_value,
#             )
#         self.buff_size += len(value)
#         self.buff[column].append([key, value])
#         if self.buff_size > self.buff_limit:
#             self.save_buff()
