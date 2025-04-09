import ctypes
import time
from typing import List

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
            ctypes.c_char_p
        ]
        self.lib.MetaServiceClient_Set.restype = ctypes.c_char_p

        self.lib.MetaServiceClient_Delete.argtypes = [ctypes.c_char_p]
        self.lib.MetaServiceClient_Delete.restype = ctypes.c_char_p

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

        # Memory management
        self.lib.MetaServiceClient_FreeArray.argtypes = [
            ctypes.POINTER(ctypes.c_char_p),
            ctypes.c_int
        ]

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

    def set(self, key: str, value: str) -> None:
        err = self.lib.MetaServiceClient_Set(
            key.encode(),
            value.encode()
        )
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