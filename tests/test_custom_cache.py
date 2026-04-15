from kernel.cache import CustomTTLCache
import pandas as pd
import time


def test_cache_set_get_and_copy():
    cache = CustomTTLCache(copy_on_read=True, copy_on_write=True)
    df = pd.DataFrame([{"a": 1}])
    cache.set("k", df, ttl_seconds=5)

    out = cache.get("k")
    assert out.equals(df)

    out.loc[0, "a"] = 999
    out2 = cache.get("k")
    assert out2.loc[0, "a"] == 1


def test_cache_expiry():
    cache = CustomTTLCache()
    cache.set("k", {"x": 1}, ttl_seconds=1)
    assert cache.has("k")
    time.sleep(1.2)
    assert not cache.has("k")
