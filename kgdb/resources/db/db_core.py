import gc
import sys
from collections.abc import Iterable
from numbers import Number
from typing import Any, List, Optional, Union

from freaddb.db_lmdb import DBSpec, FReadDB, serialize_value

from kgdb.config import config as cf
from kgdb.utils import io_worker as iw

ID_LID = "ID_LID"
LID_ID = "LID_ID"
REDIRECT = "REDIRECT"
REDIRECT_OF = "REDIRECT_OF"


class DBCore(FReadDB):
    def __init__(
        self,
        db_file: str = cf.DIR_DPDB,
        db_schema: Optional[List[DBSpec]] = None,  # type: ignore
        readonly=True,
        buff_limit: int = cf.BUFF_LIMIT,
        map_size: int = cf.SIZE_1GB,
        split_subdatabases: bool = True,
    ):
        super().__init__(
            db_file=db_file,
            db_schema=db_schema,
            readonly=readonly,
            buff_limit=buff_limit,
            map_size=map_size,
            split_subdatabases=split_subdatabases,
        )
        self.max_lid_id = self.get_number_items_from(ID_LID)
        self.buff_lid = dict()
        self.buff_size_lid = 0

    def save_buff_lid(self):
        for k, v in self.buff_lid.items():
            # add back lid to db
            self.add_buff(ID_LID, k, v)
            self.add_buff(LID_ID, v, k)
        del self.buff_lid
        gc.collect()
        self.buff_lid = dict()
        self.buff_size_lid = 0
        self.save_buff()

    def get_lid(self, db_id: Any, create_new=False) -> int:
        # Check buff
        if db_id in self.buff_lid:
            return self.buff_lid[db_id]

        # Check db
        result = self.get_value(ID_LID, db_id)
        if result is not None:
            return result

        if not create_new:
            return None
        # Create new id
        result = self.max_lid_id
        self.max_lid_id += 1
        self.buff_lid[db_id] = result

        # Check full buffer --> dump data to disk
        self.buff_size_lid += sys.getsizeof(db_id) + sys.getsizeof(result)
        if self.buff_size + self.buff_size_lid > self.buff_limit:
            self.save_buff_lid()
        return result

    def get_ids(self, lids: Iterable):
        resutls = self.get_values(LID_ID, lids)
        if resutls is None:
            return resutls
        return list(resutls.values())

    def get_id(self, lid: int) -> Any:
        if not isinstance(lid, Number) or lid < 0:
            return None
        # check buff
        if LID_ID in self.buff and lid in self.buff[LID_ID]:
            return self.buff[LID_ID]

        # check db
        result = self.get_value(LID_ID, lid)
        return result

    def add_buff_with_lid(
        self,
        db_name: str,
        key: Any,
        value: Any,
        is_serialize_value: bool = True,
        encode_key: bool = False,
        encode_value: bool = False,
    ) -> bool:
        if key is None or value is None:
            return
        if encode_key:
            if isinstance(key, tuple):
                key = tuple(self.get_lid(i, create_new=True) for i in key)
            else:
                key = self.get_lid(key, create_new=True)
        if encode_value:
            if not isinstance(value, str) and isinstance(value, Iterable):
                value = [self.get_lid(v, create_new=True) for v in value]
            else:
                value = self.get_lid(value, create_new=True)

        if is_serialize_value:
            value = serialize_value(value, **self.db_schema[db_name].get_value_args())

        if key and value:
            self.buff[db_name].append([key, value])
            self.buff_size += sys.getsizeof(key) + sys.getsizeof(value)

        if self.buff_size > self.buff_limit:
            self.save_buff()

    def _get_db_item(
        self,
        db_name: str,
        key: Any,
        get_redirect: bool = True,
        decode_value: bool = True,
    ):
        key_args = self.db_schema[db_name].get_key_args()

        if key is None:
            return None

        if key_args["combinekey"]:
            key_lids = []
            for i in key:
                if not isinstance(i, Number):
                    i = self.get_lid(i)
                key_lids.append(i)
            key = tuple(key_lids)
        else:
            if key_args["integerkey"] and not isinstance(key, Number):
                key = self.get_lid(key)
                if key is None:
                    return None

        results = self.get_value(db_name, key)
        if results is None and get_redirect:
            # Try redirect item
            try:
                if isinstance(key, tuple):
                    key_redirect = tuple(
                        self.get_redirect(key, decode=False) for i in key
                    )
                else:
                    key_redirect = self.get_redirect(key, decode=False)
                if key_redirect and key_redirect != key:
                    results = self.get_value(db_name, key_redirect)
            except Exception as message:
                iw.print_status(message, is_screen=False)
        if results is None:
            return None

        if not decode_value:
            return results

        if isinstance(results, Number):
            return self.get_id(results)

        if not isinstance(results, str) and isinstance(results, Iterable):
            results = self.get_ids(results)
            return results

        return None

    def _get_db_item_prefix(
        self,
        db_name: str,
        key: Any,
        get_redirect: bool = True,
        decode_value: bool = True,
    ):
        key_args = self.db_schema[db_name].get_key_args()

        if key is None:
            return None

        if key_args["combinekey"]:
            key_lids = []
            for i in key:
                if not isinstance(i, Number):
                    i = self.get_lid(i)
                key_lids.append(i)
            key = tuple(key_lids)
        else:
            if key_args["integerkey"] and not isinstance(key, Number):
                key = self.get_lid(key)
                if key is None:
                    return None

        iter_items = self.get_iter_with_prefix(db_name, key)

        if iter_items is None and get_redirect:
            # Try redirect item
            try:
                if isinstance(key, tuple):
                    key_redirect = tuple(
                        self.get_redirect(i, decode=False) for i in key
                    )
                else:
                    key_redirect = self.get_redirect(key, decode=False)
                if key_redirect and key_redirect != key:
                    iter_items = self.get_value(db_name, key_redirect)
            except Exception as message:
                iw.print_status(message, is_screen=False)
        if iter_items is None:
            return None

        results = dict()
        for key_i, values_i in iter_items:
            key_i = self.get_id(key_i[-1])
            if decode_value:
                values_i = self.get_ids(values_i)
            results[key_i] = values_i
        return results

    def update_desc(self, column_name: str, message: str):
        n_items = self.get_number_items_from(column_name) + len(self.buff[column_name])

        return f"Parse {message} - {n_items:,} - buff: {(self.buff_size + self.buff_size_lid)/ self.buff_limit *100:.2f} %"

    def keys(self):
        return self.get_db_iter(ID_LID, get_values=False)

    def get_item(self, db_id):
        return

    def items(self):
        for k in self.keys():
            v = self.get_item(k)
            yield k, v

    def get_redirect(self, item_id: Union[str, int], decode_value: bool = True):
        result = self._get_db_item(
            REDIRECT, item_id, decode_value=decode_value, get_redirect=False
        )
        if result is None:
            result = item_id
        return result

    def get_redirect_of(self, item_id: Union[str, int], decode_value: bool = True):
        return self._get_db_item(REDIRECT_OF, item_id, decode_value=decode_value)
