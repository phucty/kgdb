import pickle
import struct
from dataclasses import dataclass
from typing import Any

import msgpack
import numpy
from lz4 import frame
from pyroaring import BitMap

from kgdb.config import config as cf

# from rocksdb.interfaces import AssociativeMergeOperator, Comparator, SliceTransform


class OperationRocksdb:
    PUT = 1
    DELETE = 2
    MERGE = 3


class ToBytes:
    OBJ = 0
    INT_NUMPY = 1
    INT_BITMAP = 2
    BYTES = 3
    PICKLE = 4


class DBUpdateType:
    SET = 0
    COUNTER = 1


# class OperatorUpdateSetBitMap(AssociativeMergeOperator):
#     def merge(self, key: bytes, existing_value: bytes, value: bytes):
#         if existing_value:
#             existing_value = deserialize_value(
#                 existing_value, bytes_value=ToBytes.INT_BITMAP
#             )
#             value = deserialize_value(value, bytes_value=ToBytes.INT_BITMAP)

#             return_obj = existing_value | value
#             return_obj = serialize_value(return_obj, bytes_value=ToBytes.INT_BITMAP)
#             return_obj = (True, return_obj)
#         else:
#             return_obj = (True, value)
#         return return_obj

#     def name(self):
#         return b"updateBITMAP"


# class OperatorUpdateCounter(AssociativeMergeOperator):
#     def merge(self, key: bytes, existing_value: bytes, value: bytes):
#         if existing_value:
#             existing_value = deserialize_value(existing_value)
#             value = deserialize_value(value)

#             return_obj = existing_value + value
#             return_obj = serialize_value(return_obj)
#             return_obj = (True, return_obj)
#         else:
#             return_obj = (True, value)
#         return return_obj

#     def name(self):
#         return b"updateCounter"


# class StaticPrefix(SliceTransform):
#     def __init__(self, prefix_len: int = 5):
#         self.prefix_len = prefix_len

#     def name(self):
#         return b"static"

#     def transform(self, src):
#         return_obj = (0, self.prefix_len)
#         return return_obj

#     def in_domain(self, src):
#         return len(src) >= self.prefix_len

#     def in_range(self, dst):
#         return len(dst) == self.prefix_len


# class IntegerComparator(Comparator):
#     def compare(self, a: bytes, b: bytes):
#         a = deserialize_key(a, integerkey=True)
#         b = deserialize_key(b, integerkey=True)
#         if a < b:
#             return -1
#         if a > b:
#             return 1
#         if a == b:
#             return 0

#     def name(self):
#         return b"IntegerComparator"


# @dataclass
# class DBSpec:
#     name: str
#     integerkey: bool = False
#     is_64bit: bool = False
#     bytes_value: bool = ToBytes.OBJ
#     compress_value: bool = False
#     prefix_len: int = 0
#     combinekey: bool = False


def is_byte_obj(obj: Any):
    if isinstance(obj, bytes) or isinstance(obj, bytearray):
        return True
    return False


def set_default(obj: Any):
    if isinstance(obj, set):
        return sorted(list(obj))
    raise TypeError


# def deserialize_key(
#     key: Any, integerkey: bool = False, combinekey: bool = False, is_64bit: bool = False
# ):
#     if combinekey:
#         step = 8 if is_64bit else 4
#         cur = 0
#         key_parts = []
#         while cur <= len(key):
#             start = cur
#             end = cur + step
#             if end > len(key):
#                 break
#             key_parts.append(
#                 deserialize_key(key[start:end], integerkey=True, is_64bit=is_64bit)
#             )
#             cur = end + 1
#         return tuple(key_parts)

#     if not integerkey:
#         if isinstance(key, memoryview):
#             key = key.tobytes()
#         return key.decode(cf.ENCODING)
#     if is_64bit:
#         return struct.unpack("Q", key)[0]
#     else:
#         return struct.unpack("I", key)[0]


# def deserialize_value(
#     value: Any, bytes_value: ToBytes = ToBytes.OBJ, compress_value: bool = False
# ):
#     if bytes_value == ToBytes.INT_NUMPY:
#         value = numpy.frombuffer(value, dtype=numpy.uint32)

#     elif bytes_value == ToBytes.INT_BITMAP:
#         if not isinstance(value, bytes):
#             value = bytes(value)
#         value = BitMap.deserialize(value)

#     elif bytes_value == ToBytes.BYTES:
#         if isinstance(value, memoryview):
#             value = value.tobytes()

#     else:  # mode == "msgpack"
#         if compress_value:
#             try:
#                 value = frame.decompress(value)
#             except RuntimeError:
#                 pass
#         if bytes_value == ToBytes.PICKLE:
#             value = pickle.loads(value)
#         else:
#             value = msgpack.unpackb(value, strict_map_key=False)
#     return value


# def deserialize(
#     key: Any,
#     value: Any,
#     integerkey: bool = False,
#     combinekey: bool = False,
#     is_64bit: bool = False,
#     bytes_value: ToBytes = ToBytes.OBJ,
#     compress_value: bool = False,
# ):
#     key = deserialize_key(
#         key=key, integerkey=integerkey, combinekey=combinekey, is_64bit=is_64bit
#     )
#     value = deserialize_value(
#         value=value, bytes_value=bytes_value, compress_value=compress_value
#     )
#     res_obj = (key, value)
#     return res_obj


# def serialize_key(
#     key: Any,
#     integerkey: bool = False,
#     combinekey: bool = False,
#     is_64bit: bool = False,
#     deliminator="|",
#     get_postfix_deliminator=False,
# ):
#     if combinekey:
#         results = b""
#         for k in key:
#             if len(results):
#                 results += serialize_key(deliminator)
#             results += serialize_key(k, integerkey=True, is_64bit=is_64bit)
#         if get_postfix_deliminator:
#             results += serialize_key(deliminator)
#         return results

#     if not integerkey:
#         if not isinstance(key, str):
#             key = str(key)
#         return key.encode(cf.ENCODING)
#     else:
#         if (
#             not isinstance(key, int)
#             and hasattr(key, "is_integer")
#             and not key.is_integer()
#         ):
#             raise TypeError
#     if is_64bit:
#         return struct.pack("Q", key)
#     else:
#         return struct.pack("I", key)


# def serialize_value(
#     value: Any,
#     bytes_value: ToBytes = ToBytes.OBJ,
#     compress_value: bool = False,
#     sort_values: bool = True,
# ):
#     if bytes_value == ToBytes.INT_NUMPY:
#         if sort_values:
#             value = sorted(list(value))
#         if not isinstance(value, numpy.ndarray):
#             value = numpy.array(value, dtype=numpy.uint32)
#         value = value.tobytes()

#     elif bytes_value == ToBytes.INT_BITMAP:
#         value = BitMap(value).serialize()

#     else:  # mode == "msgpack"
#         if bytes_value == ToBytes.PICKLE:
#             value = pickle.dumps(value)
#         else:
#             if not isinstance(value, bytes) and not isinstance(value, bytearray):
#                 value = msgpack.packb(value, default=set_default)
#         if compress_value:
#             value = frame.compress(
#                 value, compression_level=frame.COMPRESSIONLEVEL_MINHC
#             )

#     return value


# def serialize(
#     key: Any,
#     value: Any,
#     integerkey: bool = False,
#     combinekey: bool = False,
#     is_64bit: bool = False,
#     bytes_value: ToBytes = ToBytes.OBJ,
#     compress_value: bool = False,
# ):
#     key = serialize_key(
#         key=key, integerkey=integerkey, combinekey=combinekey, is_64bit=is_64bit
#     )
#     value = serialize_value(
#         value=value, bytes_value=bytes_value, compress_value=compress_value
#     )
#     res_obj = (key, value)
#     return res_obj


def is_wikidata_item(wd_id):
    if (
        not wd_id
        or len(wd_id) < 2
        or wd_id[0].upper() not in {"Q", "P"}
        or " " in wd_id
        or not wd_id[1:].isdigit()
    ):
        return False
    return True


# def preprocess_data_before_dump(
#     data,
#     integerkey=False,
#     combinekey=False,
#     is_64bit=False,
#     bytes_value=ToBytes.OBJ,
#     compress_value=False,
#     sort_key=True,
# ):
#     if isinstance(data, dict):
#         data = list(data.items())

#     if sort_key and integerkey:
#         data.sort(key=lambda x: x[0])

#     first_key, first_value = data[0]
#     to_bytes_key = not is_byte_obj(first_key)
#     to_bytes_value = not is_byte_obj(first_value)

#     for i in range(len(data)):
#         k, v = data[i]
#         if k is None:
#             continue
#         if to_bytes_key:
#             data[i][0] = serialize_key(
#                 key=k,
#                 integerkey=integerkey,
#                 combinekey=combinekey,
#                 is_64bit=is_64bit,
#             )
#         if to_bytes_value:
#             data[i][1] = serialize_value(
#                 value=v,
#                 bytes_value=bytes_value,
#                 compress_value=compress_value,
#             )

#     if sort_key and not integerkey:
#         data.sort(key=lambda x: x[0])

#     if not isinstance(data[0], tuple):
#         data = [(k, v) for k, v in data]
#     return data


# def preprocess_data_before_dump(
#     data: Any,
#     integerkey: bool = False,
#     combinekey: bool = False,
#     is_64bit: bool = False,
#     bytes_value: ToBytes = ToBytes.OBJ,
#     compress_value: bool = False,
#     sort_key: bool = False,
# ):
#     if isinstance(data, dict):
#         data = list(data.items())
#
#     if sort_key and integerkey:
#         data.sort(key=lambda x: x[0])
#
#     first_key, first_value = data[0]
#     to_bytes_key = not is_byte_obj(first_key)
#     to_bytes_value = not is_byte_obj(first_value)
#
#     tmp_data = []
#     for k, v in data:
#         if k is None:
#             continue
#         if to_bytes_key:
#             k = serialize_key(
#                 key=k, integerkey=integerkey, combinekey=combinekey, is_64bit=is_64bit,
#             )
#         if to_bytes_value:
#             v = serialize_value(
#                 value=v, bytes_value=bytes_value, compress_value=compress_value,
#             )
#         tmp_data.append((k, v))
#     data = tmp_data
#
#     if sort_key and not integerkey:
#         data.sort(key=lambda x: x[0])
#
#     return data
