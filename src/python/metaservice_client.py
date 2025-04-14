import ctypes
import json
import time
from typing import List, Tuple, Any

class MetaServiceClient:
    def __init__(self):
        self.lib = ctypes.CDLL("./build/libMetaServiceClient.so")
        self._setup_bindings()

    def _setup_bindings(self):
        # Error message type
        self.lib.MetaServiceClient_FreeString.argtypes = [ctypes.c_char_p]

        # Initialization
        self.lib.MetaServiceClient_Initialize.argtypes = [ctypes.c_char_p]
        self.lib.MetaServiceClient_Initialize.restype = ctypes.c_char_p

        # Single key operations
        self.lib.MetaServiceClient_Get.argtypes = [
            ctypes.c_char_p,
            ctypes.POINTER(ctypes.c_char_p)
        ]
        self.lib.MetaServiceClient_Get.restype = ctypes.c_char_p

        self.lib.MetaServiceClient_Set.argtypes = [
            ctypes.c_char_p,
            ctypes.c_char_p,
            ctypes.c_char_p
        ]
        self.lib.MetaServiceClient_Set.restype = ctypes.c_char_p

        self.lib.MetaServiceClient_Delete.argtypes = [ctypes.c_char_p]
        self.lib.MetaServiceClient_Delete.restype = ctypes.c_char_p

        self.lib.MetaServiceClient_RemoveKeyFromSet.argtypes = [
            ctypes.c_char_p,
            ctypes.c_char_p
        ]
        self.lib.MetaServiceClient_RemoveKeyFromSet.restype = ctypes.c_char_p

        self.lib.MetaServiceClient_ZReadScore.argtypes = [
            ctypes.c_char_p,
            ctypes.c_char_p,
            ctypes.POINTER(ctypes.c_char_p)
        ]
        self.lib.MetaServiceClient_ZReadScore.restype = ctypes.c_char_p

        self.lib.MetaServiceClient_ZReadRange.argtypes = [
            ctypes.c_char_p,
            ctypes.c_int,
            ctypes.POINTER(ctypes.c_char_p)
        ]
        self.lib.MetaServiceClient_ZReadRange.restype = ctypes.c_char_p

        self.lib.MetaServiceClient_ZWrite.argtypes = [
            ctypes.c_char_p,
            ctypes.c_char_p,
            ctypes.c_double,
            ctypes.c_char_p
        ]
        self.lib.MetaServiceClient_ZWrite.restype = ctypes.c_char_p

        self.lib.MetaServiceClient_ZDelete.argtypes = [
            ctypes.c_char_p,
            ctypes.c_char_p
        ]
        self.lib.MetaServiceClient_ZDelete.restype = ctypes.c_char_p

        # Batch operations
        self.lib.MetaServiceClient_BatchWrite.argtypes = [
            ctypes.POINTER(ctypes.c_char_p),
            ctypes.POINTER(ctypes.c_char_p),
            ctypes.c_int
        ]
        self.lib.MetaServiceClient_BatchWrite.restype = ctypes.c_char_p

        self.lib.MetaServiceClient_BatchRead.argtypes = [
            ctypes.POINTER(ctypes.c_char_p),
            ctypes.c_int,
            ctypes.POINTER(ctypes.POINTER(ctypes.c_char_p)),
            ctypes.POINTER(ctypes.c_int)
        ]
        self.lib.MetaServiceClient_BatchRead.restype = ctypes.c_char_p

        self.lib.MetaServiceClient_BatchDelete.argtypes = [
            ctypes.POINTER(ctypes.c_char_p),
            ctypes.c_int
        ]
        self.lib.MetaServiceClient_BatchDelete.restype = ctypes.c_char_p

        self.lib.MetaServiceClient_BatchZWrite.argtypes = [
            ctypes.POINTER(ctypes.c_char_p),
            ctypes.POINTER(ctypes.c_char_p),
            ctypes.POINTER(ctypes.c_double),
            ctypes.c_int
        ]
        self.lib.MetaServiceClient_BatchZWrite.restype = ctypes.c_char_p

        self.lib.MetaServiceClient_BatchZRead.argtypes = [
            ctypes.POINTER(ctypes.c_char_p),
            ctypes.c_int,
            ctypes.POINTER(ctypes.c_char_p)
        ]
        self.lib.MetaServiceClient_BatchZRead.restype = ctypes.c_char_p

        # Advanced operations
        self.lib.MetaServiceClient_ScanInstanceKeys.argtypes = [
            ctypes.c_char_p,
            ctypes.c_int,
            ctypes.POINTER(ctypes.POINTER(ctypes.c_char_p)),
            ctypes.POINTER(ctypes.c_int)
        ]
        self.lib.MetaServiceClient_ScanInstanceKeys.restype = ctypes.c_char_p

        self.lib.MetaServiceClient_GetHottestKeys.argtypes = [
            ctypes.c_char_p,
            ctypes.c_int,
            ctypes.c_int,
            ctypes.POINTER(ctypes.POINTER(ctypes.c_char_p)),
            ctypes.POINTER(ctypes.c_int)
        ]
        self.lib.MetaServiceClient_GetHottestKeys.restype = ctypes.c_char_p

        self.lib.MetaServiceClient_GetKeysInSet.argtypes = [
            ctypes.c_char_p,
            ctypes.POINTER(ctypes.POINTER(ctypes.c_char_p)),
            ctypes.POINTER(ctypes.c_int)
        ]
        self.lib.MetaServiceClient_GetKeysInSet.restype = ctypes.c_char_p

        self.lib.MetaServiceClient_GetAliveInstanceList.argtypes = [
            ctypes.POINTER(ctypes.POINTER(ctypes.c_char_p)),
            ctypes.POINTER(ctypes.c_int)
        ]
        self.lib.MetaServiceClient_GetAliveInstanceList.restype = ctypes.c_char_p

        # Memory management
        self.lib.MetaServiceClient_FreeArray.argtypes = [
            ctypes.POINTER(ctypes.c_char_p),
            ctypes.c_int
        ]
        self.libc = ctypes.CDLL(None)
        self.libc.free.argtypes = [ctypes.c_void_p]
        self.libc.free.restype = None

    def _handle_error(self, err: bytes) -> None:
        if err:
            error_msg = err.decode()
            self.lib.MetaServiceClient_FreeString(err)
            raise RuntimeError(error_msg)

    def initialize(self, config_path: str) -> None:
        err = self.lib.MetaServiceClient_Initialize(config_path.encode())
        self._handle_error(err)

    def get(self, key: str) -> str:
        value = ctypes.c_char_p()
        err = self.lib.MetaServiceClient_Get(
            key.encode(),
            ctypes.byref(value)
        )
        self._handle_error(err)
        result = value.value.decode()
        self.lib.MetaServiceClient_FreeString(value)
        return result

    def set(self, key: str, value: str, set_name: str = "instance*") -> None:
        err = self.lib.MetaServiceClient_Set(
            key.encode(),
            value.encode(),
            set_name.encode()
        )
        self._handle_error(err)

    def zreadscore(self, zset_key: str, member: str) -> float:
        score = ctypes.c_char_p()
        err = self.lib.MetaServiceClient_ZReadScore(
            zset_key.encode(),
            member.encode(),
            ctypes.byref(score)
        )
        self._handle_error(err)
        result = score.value.decode()
        self.lib.MetaServiceClient_FreeString(score)
        return result

    def zreadrange(self, zset_key, topN):
        key_bytes = zset_key.encode('utf-8')
        result_ptr = ctypes.c_char_p()
        error = self.lib.MetaServiceClient_ZReadRange(
            key_bytes,
            topN,
            ctypes.byref(result_ptr)
        )

        self._handle_error(error)

        if not result_ptr.value:
            return []

        json_str = ctypes.string_at(result_ptr.value).decode('utf-8')
        self.lib.MetaServiceClient_FreeString(result_ptr)

        try:
            data = json.loads(json_str)
            return [(item["member"], item["score"]) for item in data]
        except json.JSONDecodeError as e:
            raise RuntimeError(f"Failed to parse response: {e}")

    def zwrite(self, zset_key: str, member: str, score: float, set_name: str = "") -> None:
        err = self.lib.MetaServiceClient_ZWrite(
            zset_key.encode(),
            member.encode(),
            ctypes.c_double(score),
            set_name.encode()
        )
        self._handle_error(err)

    def zdelete(self, key: str, member: str) -> None:
        err = self.lib.MetaServiceClient_ZDelete(key.encode(), member.encode())
        self._handle_error(err)

    def set_delete(self, set_name: str, key: str) -> None:
        err = self.lib.MetaServiceClient_RemoveKeyFromSet(set_name.encode(), key.encode())
        self._handle_error(err)

    def delete(self, key: str) -> None:
        err = self.lib.MetaServiceClient_Delete(key.encode())
        self._handle_error(err)

    def batch_write(self, items: List[tuple]) -> None:
        keys = [k.encode() for k, _ in items]
        values = [v.encode() for _, v in items]

        keys_array = (ctypes.c_char_p * len(items))(*keys)
        values_array = (ctypes.c_char_p * len(items))(*values)

        err = self.lib.MetaServiceClient_BatchWrite(
            keys_array,
            values_array,
            len(items)
        )
        self._handle_error(err)

    def batch_read(self, keys: List[str]) -> List[str]:
        key_array = (ctypes.c_char_p * len(keys))()
        for i, key in enumerate(keys):
            key_array[i] = key.encode()

        results = ctypes.POINTER(ctypes.c_char_p)()
        count = ctypes.c_int()

        err = self.lib.MetaServiceClient_BatchRead(
            key_array,
            len(keys),
            ctypes.byref(results),
            ctypes.byref(count)
        )
        self._handle_error(err)

        output = [results[i].decode() for i in range(count.value)]
        self.lib.MetaServiceClient_FreeArray(results, count.value)
        return output

    def batch_delete(self, keys: List[str]) -> None:
        key_array = (ctypes.c_char_p * len(keys))()
        for i, key in enumerate(keys):
            key_array[i] = key.encode()

        err = self.lib.MetaServiceClient_BatchDelete(
            key_array,
            len(keys)
        )
        self._handle_error(err)

    def batch_zwrite(self, entries: List[Tuple[str, str, float]]) -> None:
        """
        批量写入有序集合数据
        :param entries: [(zset_key, member, score), ...]
        """
        if not entries:
            return

        keys, members, scores = zip(*entries)

        c_keys = (ctypes.c_char_p * len(entries))()
        c_members = (ctypes.c_char_p * len(entries))()
        c_scores = (ctypes.c_double * len(entries))()

        for i, (key, member, score) in enumerate(entries):
            c_keys[i] = key.encode('utf-8')
            c_members[i] = member.encode('utf-8')
            c_scores[i] = ctypes.c_double(score)

        error = self.lib.MetaServiceClient_BatchZWrite(
            c_keys, c_members, c_scores, len(entries)
        )

        if error:
            error_msg = ctypes.string_at(error).decode('utf-8')
            self.libc.free(error)
            raise RuntimeError(f"BatchZWrite failed: {error_msg}")

    def batch_zread(self, keys: list) -> dict:
        """
        批量读取有序集合数据
        :param keys: 要读取的zset键列表
        :return: 字典 {zset_key: [(member, score), ...]}
        """
        c_keys = (ctypes.c_char_p * len(keys))()
        for i, key in enumerate(keys):
            c_keys[i] = key.encode('utf-8')

        result_json = ctypes.c_char_p()

        error = self.lib.MetaServiceClient_BatchZRead(
            c_keys,
            len(keys),
            ctypes.byref(result_json)
        )

        if error:
            error_msg = ctypes.string_at(error).decode('utf-8')
            self.lib.MetaServiceClient_FreeString(result_json)
            raise RuntimeError(f"BatchZRead failed: {error_msg}")

        try:
            json_str = ctypes.string_at(result_json.value).decode('utf-8')
            self.lib.MetaServiceClient_FreeString(result_json)
            raw_data = json.loads(json_str)
            return {
                zset_key: [
                    (entry["member"], entry["score"])
                    for entry in entries
                ]
                for zset_key, entries in raw_data.items()
            }
        except json.JSONDecodeError as e:
            raise RuntimeError(f"Invalid response format: {e}")

    @staticmethod
    def _create_c_array(data: List[Any], c_type) -> Any:
        arr = (c_type * len(data))()
        for i, item in enumerate(data):
            arr[i] = item
        return arr

    def scan_keys(self, pattern: str = "instance*", batch_size: int = 1000) -> List[str]:
        keys = ctypes.POINTER(ctypes.c_char_p)()
        count = ctypes.c_int()

        err = self.lib.MetaServiceClient_ScanInstanceKeys(
            pattern.encode(),
            batch_size,
            ctypes.byref(keys),
            ctypes.byref(count)
        )
        self._handle_error(err)

        result = [keys[i].decode() for i in range(count.value)]
        self.lib.MetaServiceClient_FreeArray(keys, count.value)
        return result

    def get_hottest_keys(self, prefix: str = "instance*",
                       batch_size: int = 1000, top_n: int = 10) -> List[str]:
        keys = ctypes.POINTER(ctypes.c_char_p)()
        count = ctypes.c_int()

        err = self.lib.MetaServiceClient_GetHottestKeys(
            prefix.encode(),
            batch_size,
            top_n,
            ctypes.byref(keys),
            ctypes.byref(count)
        )
        self._handle_error(err)

        result = [keys[i].decode() for i in range(count.value)]
        self.lib.MetaServiceClient_FreeArray(keys, count.value)
        return result

    def get_alive_instance_list(self) -> List[str]:
        keys = ctypes.POINTER(ctypes.c_char_p)()
        count = ctypes.c_int()

        err = self.lib.MetaServiceClient_GetAliveInstanceList(
            ctypes.byref(keys),
            ctypes.byref(count))
        self._handle_error(err)

        result = [keys[i].decode() for i in range(count.value)]
        self.lib.MetaServiceClient_FreeArray(keys, count.value)
        return result