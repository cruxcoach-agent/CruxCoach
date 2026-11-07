package com.cruxcoach.communityclient

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Job
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.channels.awaitClose
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.SharingStarted
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.flow.callbackFlow
import kotlinx.coroutines.flow.channelFlow
import kotlinx.coroutines.flow.combine
import kotlinx.coroutines.flow.collectLatest
import kotlinx.coroutines.flow.distinctUntilChanged
import kotlinx.coroutines.flow.emptyFlow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.conflate
import kotlinx.coroutines.flow.filter
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.stateIn
import kotlinx.coroutines.flow.update
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withTimeoutOrNull

/** The room list as the screen shows it. */
data class RoomsState(
    val rooms: List<RoomListItem> = emptyList(),
    val loading: Boolean = true,
    val error: String? = null,
    /** Unix seconds from start to the first loaded list, for the report. */
    val firstListAfterSeconds: Double? = null,
)

/**
 * The community client: one session with one community, the room list and the
 * views of single rooms. Nothing connects before [start].
 */
class CommunityClient(
    val endpoint: CommunityEndpoint,
    socketFactory: CommunitySocketFactory,
    http: CommunityHttp,
    private val signer: CommunitySigner,
    verifier: EventVerifier,
    private val clock: UnixClock,
    private val scope: CoroutineScope,
    /** Monotonic milliseconds, for timing the first room list. */
    private val elapsedMillis: () -> Long = { clock.nowSeconds() * 1000 },
    /** Steps to the first room list, with milliseconds since [start] (diagnostics only). */
    private val onTiming: (step: String, millis: Long) -> Unit = { _, _ -> },
    /** Encrypts the read state to the user's own key; without it, read state stays on this device. */
    cipher: SelfCipher? = null,
    readStore: ReadStateStore = MemoryReadStateStore(),
    /** Keeps the last rooms, messages and profiles between runs; null keeps nothing. */
    private val cache: CommunityCache? = null,
) {
    val session = CommunitySession(endpoint, socketFactory, signer, verifier, clock, scope)
    val api = CommunityApi(endpoint, http, signer, verifier, clock)

    /** How far the user has read in each room, synced across their devices. */
    val readState = ReadStateSync(session, signer.pubkey, cipher, readStore, clock, scope)

    // Room id → unread message id → the message. Ids keep live counting idempotent (a
    // resubscription after a reconnect delivers stored events again), and each read mark
    // clears exactly the messages it covers.
    private val unreadIds = MutableStateFlow<Map<String, Map<String, UnreadRules.Item>>>(emptyMap())

    /** Threads the user started or answered: replies there count as unread ([UnreadRules]). */
    private val myThreads = MutableStateFlow<Set<String>>(emptySet())

    /**
     * Room id → unread messages by others (member rooms only), as the Buzz phone app counts
     * them: top-level messages and topics, mentions of the user, broadcasts, and replies in
     * the user's threads. At most [UNREAD_LIMIT] per room.
     */
    val unread: StateFlow<Map<String, Long>> = unreadIds
        .map { rooms -> rooms.mapValues { (_, items) -> items.size.toLong() } }
        .stateIn(scope, SharingStarted.Eagerly, emptyMap())

    /** Rooms on screen: new messages there are read, not counted. */
    private val openRooms = MutableStateFlow<Set<String>>(emptySet())

    val me: String get() = signer.pubkey

    private val _rooms = MutableStateFlow(RoomsState())
    val rooms: StateFlow<RoomsState> = _rooms.asStateFlow()

    private val _profiles = MutableStateFlow<Map<String, CommunityProfile>>(emptyMap())

    /** Profiles fetched so far, by pubkey. Ask for more with [requestProfiles]. */
    val profiles: StateFlow<Map<String, CommunityProfile>> = _profiles.asStateFlow()

    /** The relay's own key once NIP-11 named it. Nothing relay-signed is trusted before. */
    private val relayKey = MutableStateFlow<String?>(null)

    private var watcher: Job? = null
    private var startedAt: Long = 0
    private val roomsLock = Mutex()
    private val profilesLock = Mutex()
    private val requestedProfiles = HashSet<String>()

    fun start() {
        if (watcher != null) return
        startedAt = elapsedMillis()
        watcher = scope.launch {
            // The last known rooms and names first, before anything is on the network.
            launch { loadCached() }
            // The relay key comes over HTTP while the socket signs in; relay-only kinds wait for it.
            launch {
                val key = fetchRelayKey()
                session.relayPubkey = key
                relayKey.value = key
                timing("relay_info")
            }
            session.start()
            launch {
                combine(session.state, relayKey) { state, key -> state == SessionState.Ready && key != null }
                    .distinctUntilChanged()
                    .collect { ready ->
                        if (ready) {
                            timing("signed_in")
                            refreshRooms()
                        }
                    }
            }
            launch {
                awaitReady()
                session.subscribe(listOf(CommunityFilters.myMembershipNotices(me, clock.nowSeconds())))
                    .collect { item -> if (item is SubscriptionItem.Event) refreshRooms() }
            }
            launch {
                awaitReady()
                readState.follow()
            }
            // Marks from here or another device clear the messages they read.
            launch {
                readState.readAt.collect { marks -> unreadIds.update { all -> all.readAgainst(marks) } }
            }
            // New messages in my rooms count up live, wherever the user is in the app.
            launch {
                awaitReady()
                _rooms.map { s -> s.rooms.filter { it.isMember }.map { it.channel.id }.toSet() }
                    .distinctUntilChanged()
                    .collectLatest { mine ->
                        if (mine.isEmpty()) return@collectLatest
                        val kinds = UnreadRules.KINDS + CommunityKinds.DELETION + CommunityKinds.GROUP_DELETE_EVENT
                        val filter = RelayFilter(kinds = kinds, tags = mapOf('h' to mine.toList()), since = clock.nowSeconds())
                        session.subscribe(listOf(filter)).collect { item ->
                            val event = (item as? SubscriptionItem.Event)?.event ?: return@collect
                            if (event.kind == CommunityKinds.DELETION || event.kind == CommunityKinds.GROUP_DELETE_EVENT) {
                                // A deleted message no longer counts (the relay checked who may delete it).
                                val gone = event.tagValues("e").toSet()
                                unreadIds.update { all -> all.mapValues { (_, items) -> items - gone } }
                                return@collect
                            }
                            val room = event.channelId?.takeIf { it in mine } ?: return@collect
                            myThreads.update { it + UnreadRules.threadsOf(listOf(event), me) }
                            val unreadItem = UnreadRules.item(event, me, myThreads.value) ?: return@collect
                            // An open room shows its timeline, not the replies inside its threads.
                            if (room in openRooms.value && !unreadItem.inThread) return@collect
                            unreadIds.update { all ->
                                if (!UnreadRules.isUnread(unreadItem, room, readState.readAt.value)) all
                                else all + (room to (all[room].orEmpty() + (unreadItem.id to unreadItem)).newest())
                            }
                        }
                    }
            }
        }
    }

    /** A room screen opened: its new messages are read as they come. */
    fun roomOpened(channelId: String) {
        openRooms.value = openRooms.value + channelId
    }

    fun roomClosed(channelId: String) {
        openRooms.value = openRooms.value - channelId
    }

    /**
     * The user read [channelId]'s timeline up to [at], the newest top-level message (in a forum:
     * topic or comment) on screen. Written as the Buzz phone app's catch-up mark and as the room
     * mark, which the Buzz desktop app reads. The room mark reads everything older, so it stops
     * before the first unread reply that shows only inside a thread: opening the room does not
     * read those. Never past now: one message dated ahead must not mark the next minutes read
     * on every device.
     */
    fun markRead(channelId: String, at: Long) {
        val upTo = minOf(at, clock.nowSeconds())
        val firstInThread = unreadIds.value[channelId]?.values?.filter { it.inThread }?.minOfOrNull { it.createdAt }
        mark(mapOf(ReadStateBlob.activityKey(channelId) to upTo, channelId to minOf(upTo, (firstInThread ?: Long.MAX_VALUE) - 1)))
    }

    /** The user read a thread's replies up to [at]: `thread:<root>` (NIP-RS) and the phone app's `thread-activity:<root>`. */
    fun markThreadRead(rootId: String, at: Long) {
        val upTo = minOf(at, clock.nowSeconds())
        mark(mapOf(ReadStateBlob.threadKey(rootId) to upTo, ReadStateBlob.threadActivityKey(rootId) to upTo))
    }

    private fun mark(marks: Map<String, Long>) {
        marks.forEach { (key, at) -> readState.markRead(listOf(key), at) }
        // Cleared at once, not only when the stored marks catch up.
        val now = readState.readAt.value
        val next = now + marks.mapValues { (key, at) -> maxOf(at, now[key] ?: -1) }
        unreadIds.update { all -> all.readAgainst(next) }
    }

    /**
     * Loads, per member room, the messages by others since its room mark (a room never read
     * looks back 7 days, as NIP-RS treats unknown as unread), and which threads the user is in.
     * Over HTTP like the Buzz phone app's batch: a large answer on the socket could fill the
     * relay's send buffer for this connection and hold up the live events.
     */
    suspend fun refreshUnread() {
        val rooms = _rooms.value.rooms.filter { it.isMember }
        if (rooms.isEmpty()) return
        val startedAt = clock.nowSeconds()
        val floor = startedAt - ReadStateBlob.HORIZON_SECONDS
        val roomIds = rooms.map { it.channel.id }
        suspend fun query(filters: List<RelayFilter>): List<CommunityEvent>? = try {
            api.query(filters)
        } catch (e: kotlinx.coroutines.CancellationException) {
            throw e
        } catch (e: Exception) {
            null
        }
        val found = coroutineScope {
            val mine = async {
                query(listOf(RelayFilter(kinds = UnreadRules.KINDS, authors = listOf(me), tags = mapOf('h' to roomIds), since = floor, limit = UNREAD_QUERY_LIMIT))).orEmpty()
            }
            val perRoom = rooms.chunked(UNREAD_ROOMS_PER_QUERY).map { chunk ->
                async {
                    val filters = chunk.map { room ->
                        // Only the room mark reads everything older; the narrower marks are checked per message.
                        val since = maxOf(floor, (readState.readAt.value[room.channel.id] ?: floor) + 1)
                        RelayFilter(kinds = unreadKinds(room.channel.type), tags = mapOf('h' to listOf(room.channel.id)), since = since, limit = UNREAD_QUERY_LIMIT)
                    }
                    val events = query(filters) ?: return@async emptyList()
                    val byRoom = events.groupBy { it.channelId }
                    chunk.map { room -> room.channel.id to byRoom[room.channel.id].orEmpty() }
                }
            }.awaitAll().flatten()
            myThreads.update { it + UnreadRules.threadsOf(mine.await() + perRoom.flatMap { (_, events) -> events }, me) }
            perRoom.map { (id, events) -> id to events.mapNotNull { UnreadRules.item(it, me, myThreads.value) } }
        }
        // Each answered room gets what the relay has now (a message deleted since is gone), plus what
        // was counted live while the queries ran; then all is checked against the marks of now, not
        // of when the queries left, so a room read meanwhile stays read.
        unreadIds.update { all ->
            val counted = all + found.associate { (id, items) ->
                val fresh = items.filter { id !in openRooms.value || it.inThread }.associateBy { it.id }
                val liveSince = all[id].orEmpty().filterValues { it.createdAt >= startedAt - LIVE_OVERLAP_SECONDS }
                id to (fresh + liveSince).newest()
            }
            counted.readAgainst(readState.readAt.value)
        }
    }

    private fun Map<String, Map<String, UnreadRules.Item>>.readAgainst(marks: Map<String, Long>): Map<String, Map<String, UnreadRules.Item>> =
        mapValues { (room, items) -> items.filterValues { UnreadRules.isUnread(it, room, marks) } }

    private fun Map<String, UnreadRules.Item>.newest(): Map<String, UnreadRules.Item> =
        if (size <= UNREAD_LIMIT) this else values.sortedByDescending { it.createdAt }.take(UNREAD_LIMIT).associateBy { it.id }

    /** Shows what the cache holds while the network catches up. */
    private suspend fun loadCached() {
        val c = cache ?: return
        val cachedProfiles = cacheRead { c.events(CommunityCache.PROFILES, null, MAX_CACHED_PROFILES) }.mapNotNull { CommunityProfile.fromEvent(it) }
        if (cachedProfiles.isNotEmpty()) {
            profilesLock.withLock {
                val next = _profiles.value.toMutableMap()
                cachedProfiles.forEach { p -> if ((next[p.pubkey]?.updatedAt ?: -1) < p.updatedAt) next[p.pubkey] = p }
                _profiles.value = next
            }
        }
        val events = cacheRead { c.events(CommunityCache.ROOMS, null, MAX_CACHED_ROOM_EVENTS) }
        if (events.isEmpty()) return
        val list = roomListFrom(events)
        roomsLock.withLock {
            // The network list wins if it was faster, even an empty one.
            if (_rooms.value.firstListAfterSeconds == null) _rooms.value = _rooms.value.copy(rooms = list)
        }
    }

    /** A cache that cannot be read is an empty cache. */
    private suspend fun cacheRead(block: suspend () -> List<CommunityEvent>): List<CommunityEvent> = try {
        block()
    } catch (e: kotlinx.coroutines.CancellationException) {
        throw e
    } catch (e: Exception) {
        emptyList()
    }

    private suspend fun cacheWrite(block: suspend (CommunityCache) -> Unit) {
        val c = cache ?: return
        try {
            block(c)
        } catch (e: kotlinx.coroutines.CancellationException) {
            throw e
        } catch (e: Exception) {
            // The cache only saves time; the relay stays the source.
        }
    }

    /** Ends the client for good: the socket closes and the session's coroutine ends. */
    fun stop() {
        watcher?.cancel()
        watcher = null
        session.close()
    }

    /** Disconnects while nobody looks (the app is in the background); [resume] reconnects. */
    fun pause() = session.stop()

    fun resume() = session.start()

    /** Signed in, and the relay's key is known. */
    private suspend fun awaitReady() {
        relayKey.first { it != null }
        session.state.first { it == SessionState.Ready }
    }

    /**
     * NIP-11 `self`, retried until the relay names it. A relay without a stable key
     * cannot prove which room lists and notices are its own, so the client waits.
     */
    private suspend fun fetchRelayKey(): String {
        var attempt = 0
        while (true) {
            val key = try {
                api.relayInfo().relayPubkey
            } catch (e: kotlinx.coroutines.CancellationException) {
                throw e
            } catch (e: Exception) {
                null
            }
            if (key != null) return key
            _rooms.value = _rooms.value.copy(loading = false, error = "the community server did not name its key; trying again")
            delay(RELAY_KEY_RETRY_SECONDS[minOf(attempt++, RELAY_KEY_RETRY_SECONDS.lastIndex)] * 1000)
        }
    }

    /** Loads my memberships, their metadata and the open-room directory. */
    suspend fun refreshRooms() {
        awaitReady()
        roomsLock.withLock { loadRooms() }
        refreshUnread()
    }

    private suspend fun loadRooms() {
        _rooms.value = _rooms.value.copy(loading = true, error = null)
        try {
            // The open-room directory does not depend on my memberships: fetch both at once.
            val events = coroutineScope {
                val directory = async {
                    session.query(listOf(CommunityFilters.channelDirectory())).also { timing("directory") }
                }
                val memberEvents = session.query(listOf(CommunityFilters.myMemberships(me)))
                timing("memberships")
                val mine = myRoomIds(memberEvents)
                val myMetas = if (mine.isEmpty()) emptyList() else session.query(listOf(CommunityFilters.channelMetadata(mine.toList())))
                timing("my_rooms")
                memberEvents + myMetas + directory.await()
            }
            val list = roomListFrom(events)
            val first = _rooms.value.firstListAfterSeconds ?: ((elapsedMillis() - startedAt) / 1000.0)
            _rooms.value = RoomsState(list, loading = false, error = null, firstListAfterSeconds = first)
            cacheWrite { it.replace(CommunityCache.ROOMS, events.distinctBy { e -> e.id }) }
        } catch (e: Exception) {
            if (e is kotlinx.coroutines.CancellationException && e !is kotlinx.coroutines.TimeoutCancellationException) {
                // The caller left (e.g. a screen's scope); the list must not keep spinning.
                _rooms.value = _rooms.value.copy(loading = false)
                throw e
            }
            _rooms.value = _rooms.value.copy(loading = false, error = e.message ?: "loading rooms failed")
        }
    }

    /** The newest member list per room, from relay-signed kind:39002 events. */
    private fun memberLists(events: List<CommunityEvent>): Map<String, ChannelMembers> =
        events.mapNotNull { ChannelMembers.fromEvent(it) }
            .groupBy { it.channelId }
            .mapValues { (_, lists) -> lists.maxBy { it.revisionAt } }

    private fun myRoomIds(events: List<CommunityEvent>): Set<String> =
        memberLists(events).values.filter { it.contains(me) }.map { it.channelId }.toSet()

    /** The room list from member lists and room metadata, fresh or cached. */
    private fun roomListFrom(events: List<CommunityEvent>): List<RoomListItem> {
        val lists = memberLists(events)
        val mine = lists.values.filter { it.contains(me) }.map { it.channelId }.toSet()
        val roles = lists.filterKeys { it in mine }.mapValues { (_, list) -> list.roleOf(me) }
        val metas = events.mapNotNull { CommunityChannel.fromEvent(it) }
        return buildRoomList(metas, mine, emptyMap(), roles)
    }

    /** Reports a step until the first room list is there. */
    private fun timing(step: String) {
        if (_rooms.value.firstListAfterSeconds == null) onTiming(step, elapsedMillis() - startedAt)
    }

    /** Fetches the kind:0 profiles of [pubkeys] not asked for yet. */
    fun requestProfiles(pubkeys: Collection<String>) {
        scope.launch {
            val missing = profilesLock.withLock {
                pubkeys.filter { it.isHex(64) && requestedProfiles.add(it) }
            }
            if (missing.isEmpty()) return@launch
            session.state.first { it == SessionState.Ready }
            missing.chunked(100).forEach { chunk ->
                val events = runCatching { session.query(listOf(CommunityFilters.profiles(chunk))) }
                    .getOrElse {
                        profilesLock.withLock { requestedProfiles.removeAll(chunk.toSet()) }
                        return@forEach
                    }
                cacheWrite { c -> c.add(CommunityCache.PROFILES, "", events, MAX_CACHED_PROFILES) }
                val found = events.mapNotNull { CommunityProfile.fromEvent(it) }
                profilesLock.withLock {
                    val next = _profiles.value.toMutableMap()
                    found.forEach { p -> if ((next[p.pubkey]?.updatedAt ?: -1) < p.updatedAt) next[p.pubkey] = p }
                    _profiles.value = next
                }
            }
        }
    }

    /**
     * Sets the user's display name on this community. Reads the newest profile first, so
     * the other fields (picture, about, set in a Buzz app) and tags stay as they are. With
     * no profile here yet, it starts from [firstBase], the user's public profile.
     */
    suspend fun setDisplayName(displayName: String, firstBase: suspend () -> ProfileBase? = { null }): PublishResult {
        withTimeoutOrNull(READY_TIMEOUT_MS) { awaitReady() } ?: throw CommunityException("offline: not connected to the community")
        val current = ownNewestProfile(session.query(listOf(CommunityFilters.profiles(listOf(me)))), me)
        val base = current?.let(ProfileBase::of) ?: firstBase()
        val result = session.publish(CommunityEventTemplates.displayName(base, displayName, clock.nowSeconds()))
        if (result.accepted) {
            CommunityProfile.fromEvent(result.event)?.let { profile ->
                profilesLock.withLock { _profiles.value = _profiles.value + (me to profile) }
            }
        }
        return result
    }

    /** Joins an open room (kind:9021). */
    suspend fun joinRoom(channelId: String): PublishResult {
        val result = session.publish(CommunityEventTemplates.joinRequest(channelId, clock.nowSeconds()))
        if (result.accepted) refreshRooms()
        return result
    }

    /** Sends a message, or a thread reply when [replyTo] is set. */
    suspend fun sendMessage(channelId: String, text: String, replyTo: ThreadRef? = null, mentions: List<String> = emptyList()): PublishResult =
        session.publish(CommunityEventTemplates.streamMessage(channelId, text, clock.nowSeconds(), replyTo, mentions))

    /**
     * Reacts to a message with [emoji]. A second tap before the first reaction came back
     * finds it already there; the relay answers `duplicate:`, which is no failure.
     */
    suspend fun react(messageId: String, emoji: String): PublishResult =
        session.publish(CommunityEventTemplates.reaction(messageId, emoji, clock.nowSeconds()))
            .let { if (!it.accepted && it.message.startsWith("duplicate:")) it.copy(accepted = true) else it }

    /** Takes back your reaction [reactionId]. */
    suspend fun removeReaction(reactionId: String): PublishResult =
        session.publish(CommunityEventTemplates.removeReaction(reactionId, clock.nowSeconds()))

    /** Replaces the text of your own message; [mentions] are the members it names, [broadcast] keeps that mark. */
    suspend fun editMessage(channelId: String, messageId: String, text: String, mentions: List<String> = emptyList(), broadcast: Boolean = false): PublishResult =
        session.publish(CommunityEventTemplates.edit(channelId, messageId, text, clock.nowSeconds(), mentions, broadcast))

    /** Deletes your own message. */
    suspend fun deleteMessage(channelId: String, messageId: String): PublishResult =
        session.publish(CommunityEventTemplates.deleteOwn(channelId, messageId, clock.nowSeconds()))

    /** Removes someone's message as a room admin. */
    suspend fun removeMessage(channelId: String, messageId: String): PublishResult =
        session.publish(CommunityEventTemplates.removeMessage(channelId, messageId, clock.nowSeconds()))

    /** Reports a message or a person to the community's moderators. */
    suspend fun report(target: ReportTarget, type: ReportType, reason: String): PublishResult =
        session.publish(CommunityEventTemplates.report(target, type, reason, clock.nowSeconds()))

    suspend fun createForumPost(channelId: String, text: String, mentions: List<String> = emptyList()): PublishResult =
        session.publish(CommunityEventTemplates.forumPost(channelId, text, clock.nowSeconds(), mentions))

    suspend fun commentOnForumPost(channelId: String, postId: String, text: String, mentions: List<String> = emptyList()): PublishResult =
        session.publish(CommunityEventTemplates.forumComment(channelId, postId, text, clock.nowSeconds(), mentions))

    /** The members of one room, from the relay's newest member list; empty if it has none. */
    suspend fun roomMembers(channelId: String): List<ChannelMember> {
        awaitReady()
        return session.query(listOf(CommunityFilters.channelMembers(channelId)))
            .mapNotNull { ChannelMembers.fromEvent(it) }
            .filter { it.channelId == channelId }
            .maxByOrNull { it.revisionAt }
            ?.members
            .orEmpty()
    }

    /**
     * A stream room: the newest top-level rows from the NIP-CW window, then live
     * events. Falls back to a plain REQ if the window query fails.
     */
    fun streamTimeline(channelId: String, pageSize: Int = 50, older: Flow<Unit> = emptyFlow()): Flow<StreamTimeline> = callbackFlow {
        var buffer = TimelineBuffer(channelId, topLevelOnly = true)
        var state = StreamTimeline()
        fun emit(update: StreamTimeline.() -> StreamTimeline = { this }) {
            state = state.update().copy(messages = buffer.messages(), threads = buffer.threadSummaries())
            trySend(state)
        }
        // The last messages seen show at once; the first network page then fills in.
        val cached = cache?.let { c -> cacheRead { c.events(CommunityCache.TIMELINE, channelId, MAX_CACHED_TIMELINE) } }.orEmpty()
        if (cached.isNotEmpty() && buffer.add(cached)) emit()
        awaitReady()
        val firstPage = CompletableDeferred<Unit>()
        // Live events until the first page, to lay over it: the page replaces what the cache showed.
        val liveBeforePage = ArrayList<CommunityEvent>()
        // From the first page on the relay decides what is there. The cache only bridged the wait,
        // and what it had may have been deleted, edited or taken back meanwhile.
        fun replaceWith(events: List<CommunityEvent>, summaries: List<ThreadSummary>) {
            buffer = TimelineBuffer(channelId, topLevelOnly = true).apply {
                add(events)
                addSummaries(summaries)
                add(liveBeforePage)
            }
            liveBeforePage.clear()
        }
        // Live first, so nothing falls between the stored page and the live stream.
        val live = launch {
            session.subscribe(listOf(CommunityFilters.streamLive(channelId, clock.nowSeconds() - 2))).collect { item ->
                if (item is SubscriptionItem.Event) {
                    if (!firstPage.isCompleted) liveBeforePage += item.event
                    if (buffer.add(listOf(item.event))) {
                        emit()
                        cacheWrite { it.add(CommunityCache.TIMELINE, channelId, listOf(item.event), MAX_CACHED_TIMELINE) }
                    }
                }
                if (item is SubscriptionItem.Closed) emit { copy(error = item.reason) }
            }
        }
        // Where the next older window page starts; null without window paging.
        var next: WindowCursor? = null
        // Older pages one at a time, each from where the last one ended (NIP-CW cursor). Subscribed
        // before the first page, so a request right after it is not lost.
        val olderPages = launch(start = CoroutineStart.UNDISPATCHED) {
            older.collect {
                firstPage.await()
                val cursor = next
                if (cursor == null || !state.hasMore || state.loadingOlder) return@collect
                emit { copy(loadingOlder = true) }
                try {
                    val page = api.channelWindow(channelId, cursor, pageSize, session.relayPubkey)
                    buffer.add(page.rows + page.aux)
                    buffer.addSummaries(page.summaries)
                    next = page.bounds.nextCursor
                    emit { copy(loadingOlder = false, hasMore = page.bounds.hasMore && next != null, error = null) }
                } catch (e: Exception) {
                    if (e is kotlinx.coroutines.CancellationException && e !is kotlinx.coroutines.TimeoutCancellationException) throw e
                    emit { copy(loadingOlder = false, error = e.message ?: "loading older messages failed") }
                }
            }
        }
        try {
            val page = api.channelWindow(channelId, null, pageSize, session.relayPubkey)
            replaceWith(page.rows + page.aux, page.summaries)
            next = page.bounds.nextCursor
            cacheWrite { it.replaceRoom(CommunityCache.TIMELINE, channelId, page.rows + page.aux) }
            emit { copy(loading = false, hasMore = page.bounds.hasMore && next != null, error = null) }
        } catch (e: Exception) {
            if (e is kotlinx.coroutines.CancellationException && e !is kotlinx.coroutines.TimeoutCancellationException) throw e
            runCatching { session.query(listOf(CommunityFilters.streamHistory(channelId))) }
                .onSuccess { history ->
                    replaceWith(history, emptyList())
                    cacheWrite { it.replaceRoom(CommunityCache.TIMELINE, channelId, history) }
                    emit { copy(loading = false, hasMore = false) }
                }
                // Offline: what the cache showed stays, with the error.
                .onFailure { emit { copy(loading = false, error = it.message ?: "loading messages failed") } }
        }
        firstPage.complete(Unit)
        awaitClose { live.cancel(); olderPages.cancel() }
    }.conflate()

    /**
     * A thread: its head and every reply, live. The head lookup runs next to the
     * reply subscription and gives up after a few seconds, so a lost head never holds
     * the replies back for long. Edits,
     * deletions and reactions of a reply tag only that reply, so they come from a
     * lookup by the replies' ids and from the room's live aux stream.
     */
    fun thread(channelId: String, rootId: String, older: Flow<Unit> = emptyFlow()): Flow<ThreadView> = channelFlow {
        awaitReady()
        val buffer = TimelineBuffer(channelId, topLevelOnly = false)
        val lock = Mutex()
        var repliesLoaded = false
        var rootLoaded = false
        var error: String? = null
        var firstPageEvents = 0
        var firstPageDone = false
        var hasOlder = false
        // The last event the thread filter returned in the relay's order (oldest first by time, then
        // the highest id in that second), replies and the root's aux alike: the paging cursor.
        var cursor: WindowCursor? = null
        fun moveCursor(event: CommunityEvent) {
            val c = cursor
            if (c == null || event.createdAt < c.createdAt || (event.createdAt == c.createdAt && event.id > c.id)) {
                cursor = WindowCursor(event.createdAt, event.id)
            }
        }
        var loadingOlder = false
        suspend fun update(change: () -> Boolean) = lock.withLock {
            // First show waits for the head too (it has its own short timeout), so it does not jump in later.
            if (change() && repliesLoaded && rootLoaded) {
                // Every row of the reply subscription tags the root, so all but the root are replies.
                val all = buffer.messages()
                send(
                    ThreadView(
                        all.firstOrNull { it.id == rootId }, all.filter { it.id != rootId },
                        loading = !rootLoaded, hasOlder = hasOlder, loadingOlder = loadingOlder, error = error,
                    ),
                )
            }
        }
        // Edits, deletions and reactions of replies tag only the reply, so they come by the replies' ids.
        suspend fun auxOf(messageIds: Collection<String>) {
            val ids = messageIds.filter { it != rootId }
            if (ids.isEmpty()) return
            val aux = try {
                ids.chunked(AUX_LOOKUP_CHUNK).flatMap { session.query(listOf(CommunityFilters.auxFor(channelId, it))) }
            } catch (e: CommunityException) {
                emptyList()
            }
            update { buffer.add(aux) }
        }
        launch(start = CoroutineStart.UNDISPATCHED) {
            older.collect {
                val before = lock.withLock { if (!hasOlder || loadingOlder) null else cursor } ?: return@collect
                update { loadingOlder = true; true }
                val page = try {
                    session.query(listOf(CommunityFilters.thread(channelId, rootId, CommunityFilters.THREAD_OLDER_PAGE, before = before)))
                } catch (e: CommunityException) {
                    update { loadingOlder = false; error = e.message; true }
                    return@collect
                }
                // The composite cursor always moves, so a full page may have more behind it.
                update {
                    buffer.add(page)
                    page.forEach(::moveCursor)
                    hasOlder = page.size >= CommunityFilters.THREAD_OLDER_PAGE
                    loadingOlder = false
                    true
                }
                auxOf(page.filter { it.kind !in CommunityKinds.AUX }.map { it.id })
            }
        }
        launch {
            val event = try {
                session.query(listOf(CommunityFilters.threadRoot(channelId, rootId)), ROOT_LOOKUP_TIMEOUT_MS).firstOrNull()
            } catch (e: CommunityException) {
                null
            }
            update { rootLoaded = true; event?.let { buffer.add(listOf(it)) }; true }
        }
        launch {
            session.subscribe(listOf(CommunityFilters.roomAuxLive(channelId, clock.nowSeconds() - 2))).collect { item ->
                if (item is SubscriptionItem.Event) update { buffer.add(listOf(item.event)) }
            }
        }
        session.subscribe(listOf(CommunityFilters.thread(channelId, rootId))).collect { item ->
            when (item) {
                is SubscriptionItem.Event -> update {
                    if (!firstPageDone) {
                        firstPageEvents++
                        moveCursor(item.event)
                    }
                    buffer.add(listOf(item.event))
                }
                SubscriptionItem.Eose -> {
                    // Only the first page decides about older replies: after a reconnect the relay sends
                    // the newest page again, which says nothing about how far paging already got.
                    update {
                        repliesLoaded = true
                        if (!firstPageDone) hasOlder = firstPageEvents >= CommunityFilters.THREAD_PAGE
                        firstPageDone = true
                        true
                    }
                    val ids = lock.withLock { buffer.messages().map { it.id } }
                    launch { auxOf(ids) }
                }
                is SubscriptionItem.Closed -> update { repliesLoaded = true; error = item.reason; true }
            }
        }
    }.conflate()

    /** A forum room: topics, newest first, and how many comments each has. */
    fun forum(channelId: String): Flow<ForumView> = callbackFlow {
        awaitReady()
        val posts = LinkedHashMap<String, CommunityEvent>()
        val comments = HashMap<String, MutableSet<String>>()
        var newestCommentAt: Long? = null
        var loaded = false
        fun emit(error: String? = null) {
            val list = posts.values.mapNotNull { CommunityMessage.fromEvent(it) }.sortedByDescending { it.createdAt }
            val newest = listOfNotNull(list.firstOrNull()?.createdAt, newestCommentAt).maxOrNull()
            trySend(ForumView(list, comments.mapValues { it.value.size }, newestAt = newest, loading = !loaded, error = error))
        }
        val postsJob = launch {
            session.subscribe(listOf(CommunityFilters.forumPosts(channelId))).collect { item ->
                when (item) {
                    is SubscriptionItem.Event -> if (posts.put(item.event.id, item.event) == null && loaded) emit()
                    SubscriptionItem.Eose -> { loaded = true; emit() }
                    is SubscriptionItem.Closed -> { loaded = true; emit(item.reason) }
                }
            }
        }
        val commentsJob = launch {
            val filter = RelayFilter(kinds = listOf(CommunityKinds.FORUM_COMMENT), tags = mapOf('h' to listOf(channelId)), limit = 500)
            session.subscribe(listOf(filter)).filter { it is SubscriptionItem.Event }.collect { item ->
                val event = (item as SubscriptionItem.Event).event
                val parent = ThreadRef.of(event)?.rootId ?: return@collect
                newestCommentAt = maxOf(newestCommentAt ?: 0, event.createdAt)
                if (comments.getOrPut(parent) { HashSet() }.add(event.id) && loaded) emit()
            }
        }
        awaitClose { postsJob.cancel(); commentsJob.cancel() }
    }.conflate()
}

/** How long a thread waits for its head before showing the replies without it. */
private const val ROOT_LOOKUP_TIMEOUT_MS = 8_000L

/** The message kinds of a stream room and of a forum. */
private fun unreadKinds(type: ChannelType): List<Int> =
    if (type == ChannelType.FORUM) listOf(CommunityKinds.FORUM_POST, CommunityKinds.FORUM_COMMENT)
    else listOf(CommunityKinds.STREAM_MESSAGE, CommunityKinds.STREAM_MESSAGE_V2)

/** The most unread messages kept per room; the badge shows "99+" above 99. */
private const val UNREAD_LIMIT = 100

/** Messages fetched per room for counting: replies come too and only some count (the Buzz phone app's limit). */
private const val UNREAD_QUERY_LIMIT = 1_000

/** Rooms per unread query, to keep one HTTP answer small. */
private const val UNREAD_ROOMS_PER_QUERY = 5

/** Live counts from this long before a recount began are kept: the recount may have missed them. */
private const val LIVE_OVERLAP_SECONDS = 2L

/** What the cache keeps: room list events, profiles, and per room the newest timeline events. */
private const val MAX_CACHED_ROOM_EVENTS = 2_000
private const val MAX_CACHED_PROFILES = 2_000
private const val MAX_CACHED_TIMELINE = 300

/** How long a user action waits for the connection before it gives up. */
private const val READY_TIMEOUT_MS = 15_000L

/** Reply ids per aux lookup, to keep one filter small. */
private const val AUX_LOOKUP_CHUNK = 200

/** Waits between tries to learn the relay's key. */
private val RELAY_KEY_RETRY_SECONDS = listOf(1L, 2, 5, 10, 30, 60)

/**
 * The join steps before the first connection: read the rules, accept them, claim
 * the invite. Kept apart from [CommunityClient] because a not-yet-member cannot
 * authenticate on the relay.
 */
class CommunityJoin(private val api: CommunityApi) {
    suspend fun rules(): JoinPolicy? = api.joinPolicy()

    /** Accepts [policy] (if any) and claims [invite]. */
    suspend fun join(invite: CommunityInvite, policy: JoinPolicy?, ageConfirmed: Boolean): ClaimResult {
        val receipt = policy?.let { api.acceptPolicy(invite.code, it, ageConfirmed) }
        return api.claimInvite(invite.code, receipt)
    }
}
