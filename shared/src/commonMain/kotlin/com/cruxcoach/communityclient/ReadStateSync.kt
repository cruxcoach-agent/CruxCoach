package com.cruxcoach.communityclient

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock

/**
 * How far the user has read in each room and thread, on this device and, with a [cipher],
 * across the user's devices (NIP-RS, see [ReadStateBlob]). Times only move forward. Contexts
 * from other devices that this client does not read are not kept, and old thread and message
 * marks are pruned as the Buzz phone app prunes them, so the local store stays small.
 */
class ReadStateSync(
    private val session: CommunitySession,
    private val me: String,
    private val cipher: SelfCipher?,
    private val store: ReadStateStore,
    private val clock: UnixClock,
    private val scope: CoroutineScope,
    private val debounceMs: Long = 8_000,
) {
    private val lock = Mutex()
    private var stored = store.load()
    private val _readAt = MutableStateFlow(stored.contexts)

    /** Room id → read up to (unix seconds). */
    val readAt: StateFlow<Map<String, Long>> = _readAt.asStateFlow()

    private var publishJob: Job? = null

    /** Marks the contexts [keys] read up to [at], a message's `created_at`. Never moves back. */
    fun markRead(keys: Collection<String>, at: Long) {
        scope.launch {
            val changed = lock.withLock {
                val moved = keys.filter { (stored.contexts[it] ?: -1) < at }
                if (moved.isEmpty()) return@withLock false
                update(stored.copy(contexts = stored.contexts + moved.associateWith { at }))
                true
            }
            if (changed) schedulePublish()
        }
    }

    /**
     * Takes in the blobs of the user's other devices from the last 7 days, then follows them
     * live. Suspends until cancelled; call it once the session is ready.
     */
    suspend fun follow() {
        if (cipher == null) return
        session.subscribe(listOf(CommunityFilters.readStates(me, clock.nowSeconds()))).collect { item ->
            if (item is SubscriptionItem.Event) absorb(item.event)
        }
    }

    private suspend fun absorb(event: CommunityEvent) {
        val blob = decrypt(event) ?: return
        val slot = ReadStateBlob.slotOf(event) ?: return
        lock.withLock {
            // Another installation at our coordinate: never write there again (NIP-RS conflict rule).
            val conflicted = slot == stored.slotId && blob.clientId != stored.clientId
            val merged = mergeReadState(stored.contexts, blob.contexts.filterKeys { ReadStateBlob.isUsedContext(it) })
            if (conflicted || merged != stored.contexts) {
                update(stored.copy(slotId = if (conflicted) ReadStateBlob.newSlotId() else stored.slotId, contexts = merged))
            }
        }
    }

    private fun schedulePublish() {
        if (cipher == null) return
        publishJob?.cancel()
        publishJob = scope.launch {
            delay(debounceMs)
            try {
                publish()
            } catch (e: kotlinx.coroutines.CancellationException) {
                throw e
            } catch (e: Exception) {
                // Best effort: the next change publishes again, and the merge loses nothing.
            }
        }
    }

    /** Read-before-write at this installation's slot, then the merged blob there. */
    suspend fun publish(): PublishResult? {
        val c = cipher ?: return null
        val slot = lock.withLock { stored.slotId }
        val own = try {
            session.query(listOf(CommunityFilters.ownReadState(me, slot)))
        } catch (e: CommunityException) {
            return null
        }
        var newestAt = 0L
        for (event in own) {
            if (ReadStateBlob.slotOf(event) != slot) continue
            newestAt = maxOf(newestAt, event.createdAt)
            absorb(event)
        }
        val (slotNow, blob) = lock.withLock {
            stored.slotId to ReadStateBlob(stored.clientId, ReadStateBlob.forPublishing(stored.contexts, MAX_PUBLISHED))
        }
        // A conflict found just now moved us to a fresh slot, which has nothing to be newer than.
        val createdAt = if (slotNow == slot) maxOf(clock.nowSeconds(), newestAt + 1) else clock.nowSeconds()
        return session.publish(CommunityEventTemplates.readState(slotNow, c.encryptToSelf(blob.toJson()), createdAt))
    }

    private suspend fun decrypt(event: CommunityEvent): ReadStateBlob? {
        if (event.pubkey != me || ReadStateBlob.slotOf(event) == null) return null
        val c = cipher ?: return null
        val plaintext = try {
            c.decryptFromSelf(event.content)
        } catch (e: kotlinx.coroutines.CancellationException) {
            throw e
        } catch (e: Exception) {
            return null
        }
        return ReadStateBlob.parse(plaintext)
    }

    private fun update(next: StoredReadState) {
        val kept = next.copy(contexts = ReadStateBlob.prune(next.contexts, clock.nowSeconds()))
        stored = kept
        store.save(kept)
        _readAt.value = kept.contexts
    }

    private companion object {
        /** Marks in one published blob, well inside the size NIP-44 can encrypt. */
        const val MAX_PUBLISHED = 500
    }
}
