package com.cruxcoach.communityclient

/**
 * Platform seams of the community client. The protocol core only talks to these;
 * the Android app implements them with OkHttp and its Nostr signer (local key or Amber).
 */

/** Signs events with the user's key. */
interface CommunitySigner {
    /** Hex public key of the signing identity. */
    val pubkey: String

    /** Signs [event]; throws if the user or the external signer refuses. */
    suspend fun sign(event: UnsignedEvent): CommunityEvent
}

/** Checks an event's id and signature. */
fun interface EventVerifier {
    fun verify(event: CommunityEvent): Boolean
}

/** Callbacks of one WebSocket connection. Called from any thread. */
interface CommunitySocketListener {
    fun onOpen()
    fun onMessage(text: String)

    /** The connection is gone, by error or by close. Called once. */
    fun onClosed(error: Throwable?)
}

/** One WebSocket connection. */
interface CommunitySocket {
    /** Queues [text]; false if the socket is not open. */
    fun send(text: String): Boolean
    fun close()
}

/** Opens WebSocket connections. */
fun interface CommunitySocketFactory {
    fun open(url: String, listener: CommunitySocketListener): CommunitySocket
}

/** A plain HTTP response. */
data class HttpResult(val status: Int, val body: String)

/** Minimal HTTP for the community's REST endpoints. Throws on transport errors. */
interface CommunityHttp {
    suspend fun get(url: String, headers: Map<String, String> = emptyMap()): HttpResult
    suspend fun post(url: String, body: String, headers: Map<String, String> = emptyMap()): HttpResult
}

/** Wall clock in unix seconds; injectable for tests. */
fun interface UnixClock {
    fun nowSeconds(): Long
}

/** Hex SHA-256 of [bytes]. */
expect fun sha256Hex(bytes: ByteArray): String

/** NIP-44 encryption to the user's own key, for data only the user's devices read. */
interface SelfCipher {
    suspend fun encryptToSelf(plaintext: String): String
    suspend fun decryptFromSelf(ciphertext: String): String
}

/** This installation's read state as it is kept between runs. */
data class StoredReadState(val clientId: String, val slotId: String, val contexts: Map<String, Long>)

/** Where the client keeps its read state and installation ids between runs. */
interface ReadStateStore {
    fun load(): StoredReadState
    fun save(state: StoredReadState)
}

/** Read state kept in memory only, e.g. in tests. */
class MemoryReadStateStore(
    private var state: StoredReadState = StoredReadState(ReadStateBlob.newSlotId(), ReadStateBlob.newSlotId(), emptyMap()),
) : ReadStateStore {
    override fun load(): StoredReadState = state
    override fun save(state: StoredReadState) {
        this.state = state
    }
}

/**
 * Events kept between runs, so the last room list and recent messages show at once and
 * offline. Events are stored as the relay sent them; they were verified on arrival.
 */
interface CommunityCache {
    /** The newest [limit] events of [bucket], of one room when [channelId] is set. */
    suspend fun events(bucket: String, channelId: String?, limit: Int): List<CommunityEvent>

    /** Replaces everything in [bucket] with [events]. */
    suspend fun replace(bucket: String, events: List<CommunityEvent>)

    /** Adds [events] to one room's part of [bucket], keeping its newest [keep]. */
    suspend fun add(bucket: String, channelId: String, events: List<CommunityEvent>, keep: Int)

    /** Replaces one room's part of [bucket] with [events]. */
    suspend fun replaceRoom(bucket: String, channelId: String, events: List<CommunityEvent>)

    companion object {
        const val ROOMS = "rooms"
        const val TIMELINE = "timeline"
        const val PROFILES = "profile"
    }
}
