from modbot.cleanup import DISCORD_EPOCH_MS, snowflake_ts, sweep

from .conftest import FakeGateway, cleanup_status


def sf(ts: int, n: int = 0) -> int:
    """Snowflake for a unix timestamp; n keeps ids unique within a second."""
    return ((ts * 1000 - DISCORD_EPOCH_MS) << 22) + n


NOW = 1_800_000_000


def test_snowflake_roundtrip():
    assert snowflake_ts(sf(NOW, 5)) == NOW


async def enqueue(db, ids, channel=10):
    for i in ids:
        await db.enqueue_cleanup(i, channel, snowflake_ts(i))


async def test_only_due_messages_swept(db):
    gw = FakeGateway()
    old, young = sf(NOW - 301, 1), sf(NOW - 100, 2)
    await enqueue(db, [old, young])
    r = await sweep(db, gw, NOW)
    assert gw.deleted == [old] and r.deleted == 1
    assert await cleanup_status(db, young) == "pending"


async def test_single_uses_single_delete(db):
    gw = FakeGateway()
    i = sf(NOW - 400)
    await enqueue(db, [i])
    await sweep(db, gw, NOW)
    assert gw.bulk_calls == [] and gw.deleted == [i]


async def test_chunking_101(db):
    gw = FakeGateway()
    ids = [sf(NOW - 400, n) for n in range(101)]
    await enqueue(db, ids)
    await sweep(db, gw, NOW)
    assert [len(c) for c in gw.bulk_calls] == [100]  # 101st goes via single delete
    assert len(gw.deleted) == 101


async def test_two_uses_bulk(db):
    gw = FakeGateway()
    ids = [sf(NOW - 400, n) for n in range(2)]
    await enqueue(db, ids)
    await sweep(db, gw, NOW)
    assert gw.bulk_calls == [ids]


async def test_older_than_14_days_goes_single(db):
    gw = FakeGateway()
    ids = [sf(NOW - 15 * 86400, n) for n in range(3)]
    await enqueue(db, ids)
    await sweep(db, gw, NOW)
    assert gw.bulk_calls == [] and sorted(gw.deleted) == sorted(ids)


async def test_failed_chunk_stays_pending_then_fails_after_5(db):
    gw = FakeGateway()
    ids = [sf(NOW - 400, n) for n in range(2)]
    gw.fail_ids = set(ids)
    await enqueue(db, ids)
    for _attempt in range(4):
        r = await sweep(db, gw, NOW)
        assert r.failed == []
        assert await cleanup_status(db, ids[0]) == "pending"
    r = await sweep(db, gw, NOW)
    assert sorted(r.failed) == sorted(ids)
    assert await cleanup_status(db, ids[0]) == "failed"


async def test_single_gone_marked_gone(db):
    gw = FakeGateway()
    i = sf(NOW - 400)
    gw.gone_ids = {i}
    await enqueue(db, [i])
    r = await sweep(db, gw, NOW)
    assert r.gone == 1 and await cleanup_status(db, i) == "gone"


async def test_enqueue_is_idempotent_and_restart_safe(db):
    gw = FakeGateway()
    i = sf(NOW - 400)
    await enqueue(db, [i])
    await enqueue(db, [i])  # catch-up scan re-finds it
    await sweep(db, gw, NOW)
    await sweep(db, gw, NOW)
    assert gw.deleted == [i]
