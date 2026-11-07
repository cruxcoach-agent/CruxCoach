package com.cruxcoach.android.communityclient

import android.content.Context
import app.cash.sqldelight.db.SqlDriver
import com.cruxcoach.android.data.SqlCipherKeyManager
import com.cruxcoach.communityclient.CommunityCache
import com.cruxcoach.communityclient.CommunityEvent
import com.cruxcoach.data.CommunityDriverFactory
import com.cruxcoach.db.community.CommunityDatabase
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext

/**
 * The community cache (#73) in its own SQLCipher file per identity, keyed from the app's
 * database master key with its own HKDF label. It only saves time: losing it loses nothing.
 */
internal class SqlCommunityCache(
    private val context: Context,
    private val keyManager: SqlCipherKeyManager,
    private val pubkeyHex: String,
) : CommunityCache {
    // Opening derives a key (Keystore, HKDF) and runs SQLCipher's key derivation: on first
    // use and on the IO dispatcher, never on the thread that created the client.
    private var driver: SqlDriver? = null
    private var queries: com.cruxcoach.db.community.CommunityCacheQueries? = null
    private var closed = false
    private val lock = Any()

    private fun queries(): com.cruxcoach.db.community.CommunityCacheQueries = synchronized(lock) {
        check(!closed) { "community cache closed" }
        queries ?: run {
            val key = keyManager.getDerivedCommunityKeyForPubkey(pubkeyHex)
            val opened = try {
                CommunityDriverFactory(context, key).createDriver(fileName(pubkeyHex))
            } finally {
                key.fill(0)
            }
            driver = opened
            CommunityDatabase(opened).communityCacheQueries.also { queries = it }
        }
    }

    override suspend fun events(bucket: String, channelId: String?, limit: Int): List<CommunityEvent> = withContext(Dispatchers.IO) {
        val q = queries()
        val rows = if (channelId == null) q.byBucket(bucket, limit.toLong()).executeAsList()
        else q.byRoom(bucket, channelId, limit.toLong()).executeAsList()
        rows.mapNotNull { CommunityEvent.fromJson(it) }
    }

    override suspend fun replace(bucket: String, events: List<CommunityEvent>) = withContext(Dispatchers.IO) {
        val q = queries()
        q.transaction {
            q.clearBucket(bucket)
            events.forEach { q.insert(bucket, it.channelId.orEmpty(), it) }
        }
    }

    override suspend fun add(bucket: String, channelId: String, events: List<CommunityEvent>, keep: Int) = withContext(Dispatchers.IO) {
        if (events.isEmpty()) return@withContext
        val q = queries()
        q.transaction {
            events.forEach { q.insert(bucket, channelId, it) }
            q.trimRoom(bucket, channelId, keep.toLong())
        }
    }

    override suspend fun replaceRoom(bucket: String, channelId: String, events: List<CommunityEvent>) = withContext(Dispatchers.IO) {
        val q = queries()
        q.transaction {
            q.clearRoom(bucket, channelId)
            events.forEach { q.insert(bucket, channelId, it) }
        }
    }

    private fun com.cruxcoach.db.community.CommunityCacheQueries.insert(bucket: String, channelId: String, event: CommunityEvent) {
        upsert(event.id, channelId, event.kind.toLong(), event.createdAt, event.toJson(), bucket)
    }

    /** Closes the file; later calls fail (and the client treats a failing cache as empty). */
    fun close() = synchronized(lock) {
        closed = true
        driver?.close()
        driver = null
        queries = null
    }

    companion object {
        fun fileName(pubkeyHex: String): String = "cruxcoach_community_${pubkeyHex.take(16)}.db"
    }
}
