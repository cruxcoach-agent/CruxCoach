package com.cruxcoach.communityclient

import com.cruxcoach.communityclient.CommunityTestEvents.BOB
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
import kotlinx.coroutines.cancel
import kotlinx.coroutines.test.StandardTestDispatcher
import kotlinx.coroutines.test.TestScope
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

/** "Encrypts" by marking the text, so the tests can read what was published. */
private object MarkingCipher : SelfCipher {
    override suspend fun encryptToSelf(plaintext: String): String = "enc:$plaintext"
    override suspend fun decryptFromSelf(ciphertext: String): String = ciphertext.removePrefix("enc:")
}

@OptIn(ExperimentalCoroutinesApi::class)
class ReadStateTest {

    // Plaintexts of the NIP-RS test vectors (docs/nips/NIP-RS.md at the pinned version).
    private val deviceA = """{"v":1,"client_id":"client-aabbccdd","contexts":{"group:general":1700001000,"group:dev":1700000500}}"""
    private val deviceB = """{"v":1,"client_id":"client-11223344","contexts":{"group:general":1700001200,"group:random":1700000800}}"""

    @Test
    fun parse_andMerge_matchTheNipRsVectors() {
        val a = assertNotNull(ReadStateBlob.parse(deviceA))
        val b = assertNotNull(ReadStateBlob.parse(deviceB))
        assertEquals("client-aabbccdd", a.clientId)
        assertEquals(
            mapOf("group:general" to 1_700_001_200L, "group:dev" to 1_700_000_500L, "group:random" to 1_700_000_800L),
            mergeReadState(a.contexts, b.contexts),
        )
        // Round trip, with the reserved prefixes escaped on the wire and unescaped once on receive.
        val blob = ReadStateBlob("me", mapOf("room" to 5L, "ov_s:x" to 6L, "esc:y" to 7L))
        assertTrue(blob.toJson().contains("\"esc:ov_s:x\":6"))
        assertEquals(blob, ReadStateBlob.parse(blob.toJson()))
    }

    @Test
    fun parse_rejectsWhatNipRsRejects() {
        assertNull(ReadStateBlob.parse("not json"))
        assertNull(ReadStateBlob.parse("""{"v":2,"client_id":"c","contexts":{}}"""))
        assertNull(ReadStateBlob.parse("""{"v":"1","client_id":"c","contexts":{}}"""))
        assertNull(ReadStateBlob.parse("""{"v":1,"contexts":{}}"""))
        assertNull(ReadStateBlob.parse("""{"v":1,"client_id":"","contexts":{}}"""))
        assertNull(ReadStateBlob.parse("""{"v":1,"client_id":"${"x".repeat(65)}","contexts":{}}"""))
        assertNull(ReadStateBlob.parse("""{"v":1,"client_id":"c","contexts":[]}"""))
        val many = (0..10_000).joinToString(",") { "\"c$it\":1" }
        assertNull(ReadStateBlob.parse("""{"v":1,"client_id":"c","contexts":{$many}}"""))
        // Bad entries are dropped one by one; override entries are not this client's business.
        val mixed = assertNotNull(
            ReadStateBlob.parse(
                """{"v":1,"client_id":"c","extra":true,"contexts":{"ok":10,"ctx:AAA":"yesterday","neg":-1,"big":4294967296,""" +
                    """"${"k".repeat(257)}":1,"ov_c:ok":3}}""",
            ),
        )
        assertEquals(mapOf("ok" to 10L), mixed.contexts)
    }

    @Test
    fun slotOf_followsTheDAndTTagRules() {
        fun ev(tags: List<List<String>>) = CommunityTestEvents.event(CommunityKinds.APP_DATA, tags, "x", pubkey = BOB)
        val good = listOf(listOf("d", "read-state:1a2b3c4d5e6f7a8b9c0d1e2f3a4b5c6d"), listOf("t", "read-state"))
        assertEquals("1a2b3c4d5e6f7a8b9c0d1e2f3a4b5c6d", ReadStateBlob.slotOf(ev(good)))
        assertNull(ReadStateBlob.slotOf(ev(listOf(listOf("t", "read-state")))))
        assertNull(ReadStateBlob.slotOf(ev(listOf(listOf("d", "backup:1a2b3c4d5e6f7a8b9c0d1e2f3a4b5c6d"), listOf("t", "read-state")))))
        assertNull(ReadStateBlob.slotOf(ev(listOf(listOf("d", "read-state:ABC"), listOf("t", "read-state")))))
        assertNull(ReadStateBlob.slotOf(ev(good + listOf(listOf("t", "read-state")))))
        assertNull(ReadStateBlob.slotOf(ev(good + listOf(listOf("d", "read-state:1a2b3c4d5e6f7a8b9c0d1e2f3a4b5c6d")))))
        assertTrue(ReadStateBlob.isSlotId(ReadStateBlob.newSlotId()))
    }

    private fun TestScope.readySession(factory: FakeSocketFactory, signer: FakeSigner): Pair<CommunitySession, CoroutineScope> {
        val scope = CoroutineScope(SupervisorJob() + StandardTestDispatcher(testScheduler))
        val session = CommunitySession(CommunityEndpoint("community.example"), factory, signer, { true }, { 1_700_001_200 }, scope)
        session.start()
        runCurrent()
        factory.last.listener.onOpen()
        factory.last.listener.onMessage("""["AUTH","c"]""")
        runCurrent()
        factory.last.listener.onMessage("""["OK","${signer.signed.last().id}",true,""]""")
        runCurrent()
        return session to scope
    }

    @Test
    fun publish_readsBeforeWriting_andStaysAheadOfTheNewestOwnBlob() = runTest {
        val factory = FakeSocketFactory()
        val signer = FakeSigner(BOB)
        val (session, scope) = readySession(factory, signer)
        val slot = "aaa111aaa111aaa111aaa111aaa111aa"
        val store = MemoryReadStateStore(StoredReadState("client-A", slot, mapOf("room-1" to 1_700_000_100L)))
        val sync = ReadStateSync(session, BOB, MarkingCipher, store, { 1_700_001_200 }, scope)

        val result = async { sync.publish() }
        runCurrent()
        val subId = factory.last.frames("REQ").last().substringAfter("[\"REQ\",\"").substringBefore('"')
        // Clock skew vector: the newest own blob is ahead of the local clock.
        val own = CommunityTestEvents.event(
            CommunityKinds.APP_DATA,
            listOf(listOf("d", "read-state:$slot"), listOf("t", "read-state")),
            "enc:" + """{"v":1,"client_id":"client-A","contexts":{"room-2":1700000900}}""",
            pubkey = BOB,
            createdAt = 1_700_001_500,
        )
        factory.last.listener.onMessage("""["EVENT","$subId",${own.toJson()}]""")
        factory.last.listener.onMessage("""["EOSE","$subId"]""")
        runCurrent()
        val published = signer.signed.last()
        assertEquals(CommunityKinds.APP_DATA, published.kind)
        assertEquals(1_700_001_501, published.createdAt)
        assertEquals(listOf(listOf("d", "read-state:$slot"), listOf("t", "read-state")), published.tags)
        val blob = assertNotNull(ReadStateBlob.parse(published.content.removePrefix("enc:")))
        assertEquals("client-A", blob.clientId)
        assertEquals(mapOf("room-1" to 1_700_000_100L, "room-2" to 1_700_000_900L), blob.contexts)
        factory.last.listener.onMessage("""["OK","${published.id}",true,""]""")
        runCurrent()
        assertTrue(assertNotNull(result.await()).accepted)
        scope.cancel()
    }

    @Test
    fun publish_movesToANewSlot_whenAnotherInstallationOwnsOurs() = runTest {
        val factory = FakeSocketFactory()
        val signer = FakeSigner(BOB)
        val (session, scope) = readySession(factory, signer)
        val slot = "aaa111aaa111aaa111aaa111aaa111aa"
        val store = MemoryReadStateStore(StoredReadState("client-A", slot, mapOf("room-1" to 5L)))
        val sync = ReadStateSync(session, BOB, MarkingCipher, store, { 1_700_001_200 }, scope)

        val result = async { sync.publish() }
        runCurrent()
        val subId = factory.last.frames("REQ").last().substringAfter("[\"REQ\",\"").substringBefore('"')
        val foreign = CommunityTestEvents.event(
            CommunityKinds.APP_DATA,
            listOf(listOf("d", "read-state:$slot"), listOf("t", "read-state")),
            "enc:" + """{"v":1,"client_id":"client-B","contexts":{"room-2":9}}""",
            pubkey = BOB,
            createdAt = 1_700_009_999,
        )
        factory.last.listener.onMessage("""["EVENT","$subId",${foreign.toJson()}]""")
        factory.last.listener.onMessage("""["EOSE","$subId"]""")
        runCurrent()
        val published = signer.signed.last()
        val newSlot = published.tags.first { it[0] == "d" }[1].removePrefix("read-state:")
        assertNotEquals(slot, newSlot, "conflict vector: never publish to a coordinate another client owns")
        assertEquals(newSlot, store.load().slotId)
        assertEquals(1_700_001_200, published.createdAt, "a fresh coordinate has nothing to be newer than")
        // client-B's read times are still merged in.
        assertEquals(mapOf("room-1" to 5L, "room-2" to 9L), sync.readAt.value)
        factory.last.listener.onMessage("""["OK","${published.id}",true,""]""")
        runCurrent()
        result.await()
        scope.cancel()
    }

    @Test
    fun markRead_onlyMovesForward() = runTest {
        val scope = CoroutineScope(SupervisorJob() + StandardTestDispatcher(testScheduler))
        val session = CommunitySession(CommunityEndpoint("community.example"), FakeSocketFactory(), FakeSigner(BOB), { true }, { 1 }, scope)
        val store = MemoryReadStateStore()
        val sync = ReadStateSync(session, BOB, cipher = null, store = store, clock = { 1 }, scope = scope)
        sync.markRead(listOf("room"), 100)
        sync.markRead(listOf("room"), 50)
        runCurrent()
        assertEquals(mapOf("room" to 100L), sync.readAt.value)
        assertEquals(mapOf("room" to 100L), store.load().contexts)
        scope.cancel()
    }

    @Test
    fun contexts_arePrunedAndPublishedAsTheBuzzPhoneAppDoes() {
        val root = CommunityTestEvents.id(1)
        val message = CommunityTestEvents.id(2)
        assertTrue(ReadStateBlob.isUsedContext("thread:$root") && ReadStateBlob.isUsedContext("msg:$message"))
        // NIP-RS: thread: and msg: without a 64-hex id are opaque, and this client does not read them.
        assertTrue(!ReadStateBlob.isUsedContext("thread:general") && !ReadStateBlob.isUsedContext("group:dev"))

        // Kept: room and catch-up marks always; thread and message marks of the last 7 days, the newest first.
        val now = 1_700_000_000L
        val old = now - ReadStateBlob.HORIZON_SECONDS - 1
        val marks = mapOf("msg:$message" to now - 5, "activity:room" to old, "thread:$root" to old, "room" to old, "thread:${CommunityTestEvents.id(3)}" to now - 9)
        assertEquals(setOf("msg:$message", "activity:room", "room", "thread:${CommunityTestEvents.id(3)}"), ReadStateBlob.prune(marks, now).keys)
        assertEquals(setOf("msg:$message", "activity:room", "room"), ReadStateBlob.prune(marks, now, maxPrunable = 1).keys)
        // Published: never message marks; room marks first, then the most recent.
        assertEquals(setOf("activity:room", "thread:$root", "room", "thread:${CommunityTestEvents.id(3)}"), ReadStateBlob.forPublishing(marks, 10).keys)
        assertEquals(setOf("room", "thread:${CommunityTestEvents.id(3)}"), ReadStateBlob.forPublishing(marks, 2).keys)
    }

    @Test
    fun unread_countsWhatTheBuzzPhoneAppCounts() {
        val room = CommunityTestEvents.CHANNEL
        val me = BOB
        val other = CommunityTestEvents.ALICE
        val myRoot = CommunityTestEvents.id(100)
        val foreignRoot = CommunityTestEvents.id(101)
        fun message(n: Int, vararg tags: List<String>, pubkey: String = other) =
            CommunityTestEvents.event(CommunityKinds.STREAM_MESSAGE, listOf(listOf("h", room)) + tags, "m$n", pubkey = pubkey, createdAt = 1_000L + n, id = CommunityTestEvents.id(n))
        fun reply(root: String) = listOf("e", root, "", "reply")

        val topLevel = message(1)
        val mine = message(2, pubkey = me)
        val inForeignThread = message(3, reply(foreignRoot))
        val inMyThread = message(4, reply(myRoot))
        val namingMe = message(5, reply(foreignRoot), listOf("p", me))
        val broadcast = message(6, reply(foreignRoot), listOf("broadcast", "1"))

        val myThreads = UnreadRules.threadsOf(listOf(message(7, reply(myRoot), pubkey = me), mine), me)
        assertEquals(setOf(myRoot, mine.id), myThreads)
        val items = listOf(topLevel, mine, inForeignThread, inMyThread, namingMe, broadcast).mapNotNull { UnreadRules.item(it, me, myThreads) }
        assertEquals(listOf(topLevel.id, inMyThread.id, namingMe.id, broadcast.id), items.map { it.id })

        fun unread(marks: Map<String, Long>) = items.filter { UnreadRules.isUnread(it, room, marks) }.map { it.id }
        assertEquals(items.map { it.id }, unread(emptyMap()))
        // The phone app's catch-up mark reads ordinary top-level messages only.
        assertEquals(listOf(inMyThread.id, namingMe.id, broadcast.id), unread(mapOf(ReadStateBlob.activityKey(room) to 2_000L)))
        // The room mark reads everything up to it; thread and message marks read their own.
        assertEquals(listOf(namingMe.id, broadcast.id), unread(mapOf(room to 1_004L)))
        assertEquals(listOf(topLevel.id, namingMe.id, broadcast.id), unread(mapOf(ReadStateBlob.threadKey(myRoot) to 2_000L)))
        assertEquals(listOf(topLevel.id, inMyThread.id, broadcast.id), unread(mapOf(ReadStateBlob.msgKey(namingMe.id) to namingMe.createdAt)))
        // A broadcast reply shows in the room, so like the phone app its thread's marks do not read it.
        assertEquals(listOf(topLevel.id, broadcast.id), unread(mapOf(ReadStateBlob.threadActivityKey(myRoot) to 2_000L, ReadStateBlob.threadActivityKey(foreignRoot) to 2_000L)))
        // Only replies that the room's timeline does not show are "in a thread".
        assertEquals(listOf(inMyThread.id, namingMe.id), items.filter { it.inThread }.map { it.id })
    }
}
