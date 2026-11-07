package com.cruxcoach.communityclient

import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.intOrNull
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.longOrNull
import kotlinx.serialization.json.put
import kotlinx.serialization.json.putJsonObject
import kotlin.random.Random

/**
 * Cross-device read state as NIP-RS (docs/nips/NIP-RS.md at the pinned version) defines it:
 * one encrypted kind:30078 per installation, a map from context to "read up to" time.
 *
 * The two Buzz apps read different marks at the pinned version: the desktop app the room
 * (`<room>`), `thread:<root>` and `msg:<id>`; the phone app also its catch-up marks
 * (`activity:<room>`, `thread-activity:<root>`; mobile/lib/features/channels/reading_marks.dart).
 * So this client writes the room and the thread marks of both, reads all of them, and writes
 * frontier entries only. It neither reads nor writes the manual-unread override layer (`ov_*`),
 * so it fetches within the 7-day horizon.
 */
data class ReadStateBlob(val clientId: String, val contexts: Map<String, Long>) {

    /** The plaintext to encrypt, NIP-RS schema version 1. */
    fun toJson(): String = buildJsonObject {
        put("v", 1)
        put("client_id", clientId)
        putJsonObject("contexts") { contexts.forEach { (key, at) -> put(escapeKey(key), at) } }
    }.toString()

    companion object {
        const val D_PREFIX = "read-state:"
        const val T_TAG = "read-state"
        const val HORIZON_SECONDS = 7L * 24 * 60 * 60
        private const val MAX_CONTEXTS = 10_000
        private const val MAX_CONTEXT_BYTES = 256
        private const val MAX_TIMESTAMP = 4_294_967_295L

        /**
         * The blob in a decrypted plaintext, or null if NIP-RS says to discard or ignore it.
         * Bad single entries are dropped; `ov_*` override entries are left out (this client
         * does not use that layer) and one leading `esc:` is stripped from frontier keys.
         */
        fun parse(plaintext: String): ReadStateBlob? {
            val obj = runCatching { CommunityJson.parseToJsonElement(plaintext).jsonObject }.getOrNull() ?: return null
            val version = (obj["v"] as? JsonPrimitive)?.takeIf { !it.isString }?.intOrNull ?: return null
            if (version != 1) return null
            val clientId = (obj["client_id"] as? JsonPrimitive)?.takeIf { it.isString }?.content ?: return null
            if (clientId.isEmpty() || clientId.codePointCount() > 64) return null
            val contexts = obj["contexts"] as? JsonObject ?: return null
            if (contexts.size > MAX_CONTEXTS) return null
            val out = LinkedHashMap<String, Long>()
            for ((key, value) in contexts) {
                if (key.encodeToByteArray().size > MAX_CONTEXT_BYTES) continue
                if (key.startsWith("ov_")) continue
                val at = (value as? JsonPrimitive)?.takeIf { !it.isString }?.longOrNull ?: continue
                if (at !in 0..MAX_TIMESTAMP) continue
                out[unescapeKey(key)] = at
            }
            return ReadStateBlob(clientId, out)
        }

        /** Catch-up mark for a room's ordinary top-level messages (the Buzz phone app). */
        fun activityKey(channelId: String): String = "activity:$channelId"

        /** Catch-up mark for a thread's replies (the Buzz phone app). */
        fun threadActivityKey(rootId: String): String = "thread-activity:$rootId"

        /** NIP-RS thread context. */
        fun threadKey(rootId: String): String = "thread:$rootId"

        /** NIP-RS per-message context. */
        fun msgKey(eventId: String): String = "msg:$eventId"

        /**
         * The contexts this client reads: room marks, NIP-RS thread and message marks (only
         * with a 64-hex id, as NIP-RS says), and the two catch-up marks.
         */
        fun isUsedContext(key: String): Boolean =
            !key.contains(':') || key.startsWith("activity:") || key.startsWith("thread-activity:") ||
                (key.startsWith("thread:") && key.removePrefix("thread:").isHex(64)) ||
                (key.startsWith("msg:") && key.removePrefix("msg:").isHex(64))

        /**
         * What this device keeps, as the Buzz phone app prunes it: room and catch-up marks always;
         * thread and message marks only from the last 7 days, the newest [maxPrunable] of them.
         */
        fun prune(contexts: Map<String, Long>, now: Long, maxPrunable: Int = 1_000): Map<String, Long> {
            val (prunable, kept) = contexts.entries.partition { it.key.startsWith("thread:") || it.key.startsWith("msg:") }
            val fresh = prunable.filter { it.value >= now - HORIZON_SECONDS }
                .sortedByDescending { it.value }
                .take(maxPrunable)
            return (kept + fresh).associate { it.key to it.value }
        }

        /**
         * The marks to publish, at most [max]: never message marks (this client does not write
         * them, and like the Buzz phone app it does not republish other devices' ones); room marks
         * first, then the most recent of the rest, so a new read always syncs.
         */
        fun forPublishing(contexts: Map<String, Long>, max: Int): Map<String, Long> {
            val candidates = contexts.filterKeys { !it.startsWith("msg:") }
            if (candidates.size <= max) return candidates
            return candidates.entries
                .sortedWith(compareBy<Map.Entry<String, Long>> { if (it.key.contains(':')) 1 else 0 }.thenByDescending { it.value }.thenBy { it.key })
                .take(max)
                .associate { it.key to it.value }
        }

        /** NIP-RS escapes raw ids that start with the reserved `ov_` or `esc:`. */
        fun escapeKey(raw: String): String = if (raw.startsWith("ov_") || raw.startsWith("esc:")) "esc:$raw" else raw

        fun unescapeKey(wire: String): String = if (wire.startsWith("esc:")) wire.removePrefix("esc:") else wire

        /** A new primary coordinate id: exactly 32 lowercase hex characters. */
        fun newSlotId(random: Random = Random.Default): String =
            random.nextBytes(16).joinToString("") { (it.toInt() and 0xff).toString(16).padStart(2, '0') }

        fun isSlotId(value: String): Boolean = value.length == 32 && value.all { it in '0'..'9' || it in 'a'..'f' }

        /** The slot id of a read-state event, or null if NIP-RS says to ignore the event. */
        fun slotOf(event: CommunityEvent): String? {
            if (event.kind != CommunityKinds.APP_DATA) return null
            val d = event.tags.filter { it.size >= 2 && it[0] == "d" }.singleOrNull()?.get(1) ?: return null
            if (event.tags.count { it.size >= 2 && it[0] == "t" && it[1] == T_TAG } != 1) return null
            if (!d.startsWith(D_PREFIX)) return null
            return d.removePrefix(D_PREFIX).takeIf { isSlotId(it) }
        }
    }
}

/** The grow-only merge: for each context the newest "read up to" time wins. */
fun mergeReadState(a: Map<String, Long>, b: Map<String, Long>): Map<String, Long> {
    if (b.isEmpty()) return a
    val out = HashMap(a)
    b.forEach { (key, at) -> if ((out[key] ?: -1) < at) out[key] = at }
    return out
}

/** The read-state event for one slot (kind:30078, NIP-78 app data). */
fun CommunityEventTemplates.readState(slotId: String, ciphertext: String, createdAt: Long): UnsignedEvent {
    require(ReadStateBlob.isSlotId(slotId)) { "not a slot id: $slotId" }
    return UnsignedEvent(
        createdAt = createdAt,
        kind = CommunityKinds.APP_DATA,
        tags = listOf(listOf("d", ReadStateBlob.D_PREFIX + slotId), listOf("t", ReadStateBlob.T_TAG)),
        content = ciphertext,
    )
}

/** Every read-state blob of [me] from the last 7 days. */
fun CommunityFilters.readStates(me: String, now: Long): RelayFilter = RelayFilter(
    kinds = listOf(CommunityKinds.APP_DATA),
    authors = listOf(me),
    tags = mapOf('t' to listOf(ReadStateBlob.T_TAG)),
    since = now - ReadStateBlob.HORIZON_SECONDS,
)

/** This installation's own blob, for read-before-write. */
fun CommunityFilters.ownReadState(me: String, slotId: String): RelayFilter = RelayFilter(
    kinds = listOf(CommunityKinds.APP_DATA),
    authors = listOf(me),
    tags = mapOf('d' to listOf(ReadStateBlob.D_PREFIX + slotId)),
)

private fun String.codePointCount(): Int {
    var count = 0
    var i = 0
    while (i < length) {
        i += if (this[i].isHighSurrogate() && i + 1 < length && this[i + 1].isLowSurrogate()) 2 else 1
        count++
    }
    return count
}

/**
 * Which messages make a room unread, and which read marks cover them, as the Buzz phone app
 * decides it at the pinned version (mobile/lib/features/channels/unread_badge/).
 */
object UnreadRules {
    /** The kinds that count: stream messages, forum topics and comments. */
    val KINDS = listOf(CommunityKinds.STREAM_MESSAGE, CommunityKinds.STREAM_MESSAGE_V2, CommunityKinds.FORUM_POST, CommunityKinds.FORUM_COMMENT)

    /**
     * One unread message, with what is needed to check it against later read marks. [inThread]:
     * a reply that shows only inside its thread, not in the room's timeline.
     */
    data class Item(val id: String, val createdAt: Long, val rootId: String?, val channelCatchUp: Boolean, val inThread: Boolean)

    /**
     * The unread item for [event], or null if it does not count: it must be by someone other
     * than [me] and be a broadcast, name [me], be top-level (a message or a topic), or answer
     * in a thread [me] started or answered ([myThreads]).
     */
    fun item(event: CommunityEvent, me: String, myThreads: Set<String>): Item? {
        if (event.kind !in KINDS || event.pubkey == me) return null
        val thread = ThreadRef.of(event)
        val broadcast = event.tags.any { it.size >= 2 && it[0] == "broadcast" && it[1] == "1" }
        val namesMe = event.tags.any { it.size >= 2 && it[0] == "p" && it[1] == me }
        val highPriority = broadcast || namesMe
        if (!highPriority && thread != null && thread.rootId !in myThreads) return null
        // The catch-up mark reads ordinary top-level messages only, never mentions or broadcasts.
        // A broadcast reply shows in the room, so like the phone app it is not read with its thread.
        return Item(
            id = event.id,
            createdAt = event.createdAt,
            rootId = thread?.rootId?.takeIf { !broadcast },
            channelCatchUp = thread == null && !highPriority,
            inThread = thread != null && !broadcast,
        )
    }

    /** The threads [me] started or answered, as far as [events] show. */
    fun threadsOf(events: Collection<CommunityEvent>, me: String): Set<String> =
        events.filter { it.pubkey == me && it.kind in KINDS }.mapTo(HashSet()) { ThreadRef.of(it)?.rootId ?: it.id }

    /** The newest mark that reads [item] in [channelId]: the room, its own, its thread's, or the catch-up mark. */
    fun readAt(item: Item, channelId: String, marks: Map<String, Long>): Long? = listOfNotNull(
        marks[channelId],
        marks[ReadStateBlob.msgKey(item.id)],
        item.rootId?.let { marks[ReadStateBlob.threadKey(it)] },
        item.rootId?.let { marks[ReadStateBlob.threadActivityKey(it)] },
        if (item.channelCatchUp) marks[ReadStateBlob.activityKey(channelId)] else null,
    ).maxOrNull()

    fun isUnread(item: Item, channelId: String, marks: Map<String, Long>): Boolean =
        item.createdAt > (readAt(item, channelId, marks) ?: -1)
}
