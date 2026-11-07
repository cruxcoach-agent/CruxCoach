package com.cruxcoach.communityclient

import com.cruxcoach.communityclient.CommunityTestEvents.BOB
import com.cruxcoach.communityclient.CommunityTestEvents.CHANNEL
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.cancel
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.test.StandardTestDispatcher
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * A cache in memory: per bucket, events with the room they were stored under. Like the SQL
 * cache it files events by the room it is given, so reactions without an `h` tag come back too.
 */
private class MemoryCache(initial: Map<String, List<CommunityEvent>> = emptyMap()) : CommunityCache {
    val buckets = HashMap(initial.mapValues { (_, events) -> events.map { it.channelId to it }.toMutableList() })

    fun ids(bucket: String): List<String> = buckets[bucket].orEmpty().map { it.second.id }

    override suspend fun events(bucket: String, channelId: String?, limit: Int): List<CommunityEvent> =
        buckets[bucket].orEmpty().filter { channelId == null || it.first == channelId }.map { it.second }.sortedByDescending { it.createdAt }.take(limit)

    override suspend fun replace(bucket: String, events: List<CommunityEvent>) {
        buckets[bucket] = events.map { it.channelId to it }.toMutableList()
    }

    override suspend fun add(bucket: String, channelId: String, events: List<CommunityEvent>, keep: Int) {
        buckets.getOrPut(bucket) { mutableListOf() }.addAll(events.map { channelId to it })
    }

    override suspend fun replaceRoom(bucket: String, channelId: String, events: List<CommunityEvent>) {
        val list = buckets.getOrPut(bucket) { mutableListOf() }
        list.removeAll { it.first == channelId }
        list.addAll(events.map { channelId to it })
    }
}

/** A network that never answers: the client has only its cache. */
private object SilentHttp : CommunityHttp {
    override suspend fun get(url: String, headers: Map<String, String>): HttpResult = awaitCancellation()
    override suspend fun post(url: String, body: String, headers: Map<String, String>): HttpResult = awaitCancellation()
}

@OptIn(ExperimentalCoroutinesApi::class)
class CommunityCacheTest {

    @Test
    fun cachedRoomsAndMessages_showBeforeTheNetworkAnswers() = runTest {
        val scope = CoroutineScope(SupervisorJob() + StandardTestDispatcher(testScheduler))
        val cache = MemoryCache(
            mapOf(
                CommunityCache.ROOMS to listOf(CommunityTestEvents.groupMetadata, CommunityTestEvents.groupMembers),
                CommunityCache.TIMELINE to listOf(CommunityTestEvents.message),
            ),
        )
        val client = CommunityClient(
            endpoint = CommunityEndpoint("community.example"),
            socketFactory = FakeSocketFactory(),
            http = SilentHttp,
            signer = FakeSigner(BOB),
            verifier = { true },
            clock = { 1_760_100_000 },
            scope = scope,
            cache = cache,
        )
        client.start()
        runCurrent()
        val room = client.rooms.value.rooms.single { it.channel.id == CHANNEL }
        assertTrue(room.isMember)
        assertEquals("member", room.myRole)

        val first = client.streamTimeline(CHANNEL).first()
        assertEquals(listOf(CommunityTestEvents.message.id), first.messages.map { it.id })
        assertTrue(first.loading, "still loading: the network page has not come")
        client.stop()
        scope.cancel()
    }

    @Test
    fun theFirstNetworkPage_replacesWhatTheCacheShowed() = runTest {
        val scope = CoroutineScope(SupervisorJob() + StandardTestDispatcher(testScheduler))
        // Cached: a message that was deleted meanwhile, and a reaction taken back meanwhile on one that stays.
        val stays = CommunityTestEvents.event(9, listOf(listOf("h", CHANNEL)), "stays", id = CommunityTestEvents.id(40), createdAt = 1_760_025_900)
        val takenBack = CommunityTestEvents.event(7, listOf(listOf("e", stays.id)), "🔥", pubkey = BOB, id = CommunityTestEvents.id(41))
        val cache = MemoryCache()
        cache.replaceRoom(CommunityCache.TIMELINE, CHANNEL, listOf(CommunityTestEvents.message, stays, takenBack))
        // The relay's page has only the message that stays, without the reaction.
        val page = listOf(stays, CommunityTestEvents.windowBounds(hasMore = false, next = null))
        val http = object : CommunityHttp {
            override suspend fun get(url: String, headers: Map<String, String>) = HttpResult(200, """{"self":"${CommunityTestEvents.RELAY}"}""")
            override suspend fun post(url: String, body: String, headers: Map<String, String>) =
                HttpResult(200, page.joinToString(",", "[", "]") { it.toJson() })
        }
        val sockets = FakeSocketFactory()
        val signer = FakeSigner(BOB)
        val client = CommunityClient(
            endpoint = CommunityEndpoint("community.example"),
            socketFactory = sockets,
            http = http,
            signer = signer,
            verifier = { true },
            clock = { 1_760_100_000 },
            scope = scope,
            cache = cache,
        )
        client.start()
        runCurrent()
        sockets.last.listener.onOpen()
        sockets.last.listener.onMessage("""["AUTH","c"]""")
        runCurrent()
        sockets.last.listener.onMessage("""["OK","${signer.signed.last().id}",true,""]""")
        runCurrent()

        val timeline = client.streamTimeline(CHANNEL)
        // First the cache, with the reaction (the check below is not empty by accident) ...
        val cached = timeline.first()
        assertTrue(cached.loading)
        assertEquals(mapOf("🔥" to setOf(BOB)), cached.messages.single { it.id == stays.id }.reactions)
        // ... then the page replaces it.
        val shown = timeline.first { !it.loading }
        assertEquals(listOf(stays.id), shown.messages.map { it.id })
        assertTrue(shown.messages.single().reactions.isEmpty(), "the reaction taken back is gone")
        assertEquals(listOf(stays.id), cache.ids(CommunityCache.TIMELINE), "the cache follows the page")
        client.stop()
        scope.cancel()
    }
}
