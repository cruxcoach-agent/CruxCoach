package com.cruxcoach.android.communityclient

import android.content.Context
import android.content.SharedPreferences
import android.os.SystemClock
import android.util.Log
import androidx.lifecycle.DefaultLifecycleObserver
import androidx.lifecycle.LifecycleOwner
import androidx.lifecycle.ProcessLifecycleOwner
import com.cruxcoach.android.BuildConfig
import com.cruxcoach.android.nostr.NostrSigner
import com.cruxcoach.android.payment.NostrProfileManager
import com.cruxcoach.android.payment.PublishedProfileLookup
import com.cruxcoach.communityclient.CommunityApi
import com.cruxcoach.communityclient.CommunityClient
import com.cruxcoach.communityclient.CommunityEndpoint
import com.cruxcoach.communityclient.CommunityEvent
import com.cruxcoach.communityclient.CommunityJoin
import com.cruxcoach.communityclient.CommunityProfile
import com.cruxcoach.communityclient.ProfileBase
import com.cruxcoach.communityclient.ReadStateBlob
import com.cruxcoach.communityclient.ReadStateStore
import com.cruxcoach.communityclient.StoredReadState
import com.cruxcoach.communityclient.UnixClock
import com.vitorpamplona.quartz.nip01Core.core.hexToByteArray
import com.vitorpamplona.quartz.nip19Bech32.toNpub
import dagger.hilt.android.qualifiers.ApplicationContext
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.contentOrNull
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.longOrNull
import kotlinx.serialization.json.put
import kotlinx.serialization.json.putJsonObject
import okhttp3.OkHttpClient
import javax.inject.Inject
import javax.inject.Named
import javax.inject.Singleton

/** What the community screens need to know before a client exists. */
data class CommunitySetup(
    /** The user turned the feature on. Nothing reaches the server before that. */
    val enabled: Boolean,
    /** The community this key joined, if any. */
    val joined: CommunityEndpoint?,
    /** Just joined with an invite: ask for the name people see, once. */
    val askName: Boolean = false,
)

/** The user's public profile, as the app's relays hold it. */
sealed interface PublicProfile {
    /** Its newest signed kind:0, and the name it shows (empty if none). */
    data class Found(val base: ProfileBase, val name: String) : PublicProfile

    /** The relays answered without one: a first write starts empty. */
    data object None : PublicProfile

    /** No relay answered: a first write must wait, it might wipe a profile that exists. */
    data object Unavailable : PublicProfile
}

/** The first community profile would start without the public one. */
class PublicProfileUnavailableException : Exception("public profile unavailable")

/**
 * Owns the one [CommunityClient] of the app and the feature's own small settings
 * file. The client exists only while the feature is on and a community is joined,
 * and it is rebuilt when the user switches keys.
 */
@Singleton
class CommunityClientHolder @Inject constructor(
    @param:ApplicationContext private val context: Context,
    private val nostrSigner: NostrSigner,
    private val profileManager: NostrProfileManager,
    @param:Named("nostr") private val okHttpClient: OkHttpClient,
) {
    private val prefs = context.getSharedPreferences(PREFS, Context.MODE_PRIVATE)
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
    private val clock = UnixClock { System.currentTimeMillis() / 1000 }
    private val http by lazy { OkHttpCommunityHttp(okHttpClient) }
    private val sockets by lazy { OkHttpCommunitySocketFactory(okHttpClient) }
    private val signer = QuartzCommunitySigner(signer = { nostrSigner.signer }, publicKey = { nostrSigner.getPublicKeyHex() })

    private val _setup = MutableStateFlow(readSetup())
    val setup: StateFlow<CommunitySetup> = _setup.asStateFlow()

    private var client: CommunityClient? = null
    private var clientScope: CoroutineScope? = null
    private var clientKey: Pair<CommunityEndpoint, String>? = null

    /**
     * The app is on screen. A minute after it left the screen the session disconnects
     * (no notifications in the preview). The minute covers a trip to Amber for a signature.
     */
    private var foreground = true
    private var pauseJob: Job? = null

    init {
        scope.launch(Dispatchers.Main) {
            ProcessLifecycleOwner.get().lifecycle.addObserver(object : DefaultLifecycleObserver {
                override fun onStart(owner: LifecycleOwner) = setForeground(true)
                override fun onStop(owner: LifecycleOwner) = setForeground(false)
            })
        }
    }

    @Synchronized
    private fun setForeground(value: Boolean) {
        foreground = value
        pauseJob?.cancel()
        pauseJob = null
        val current = client ?: return
        if (value) current.resume() else schedulePause(current)
    }

    private fun schedulePause(target: CommunityClient) {
        pauseJob = scope.launch {
            delay(BACKGROUND_GRACE_MS)
            synchronized(this@CommunityClientHolder) { if (!foreground && client === target) target.pause() }
        }
    }

    /** The identity that signs, for display. */
    val pubkey: String get() = signer.pubkey

    /** The same identity as an npub, which people can give to a community admin. */
    val npub: String get() = runCatching { pubkey.hexToByteArray().toNpub() }.getOrDefault(pubkey)
    val usesAmber: Boolean get() = nostrSigner.getStoredSignerMode().name == "AMBER"

    fun setEnabled(enabled: Boolean) {
        prefs.edit().putBoolean(KEY_ENABLED, enabled).apply()
        if (!enabled) closeClient()
        _setup.value = readSetup()
    }

    /** Remembers that this key is a member of [endpoint]; [askName] after a fresh invite. */
    fun markJoined(endpoint: CommunityEndpoint, askName: Boolean = false) {
        val edit = prefs.edit()
            .putString(KEY_HOST, endpoint.host)
            .putBoolean(KEY_SECURE, endpoint.secure)
            .putString(KEY_JOINED_PUBKEY, signer.pubkey)
        if (askName) edit.putString(KEY_ASK_NAME_PUBKEY, signer.pubkey) else edit.remove(KEY_ASK_NAME_PUBKEY)
        edit.apply()
        _setup.value = readSetup()
    }

    /** The name step is done (saved or skipped). */
    fun nameAsked() {
        prefs.edit().remove(KEY_ASK_NAME_PUBKEY).apply()
        _setup.value = readSetup()
    }

    /**
     * The user's public profile from the app's relays, as signed. Fields saved only on
     * this device never come from here, so nothing private reaches the community.
     */
    suspend fun publicProfile(): PublicProfile = withContext(Dispatchers.IO) {
        when (val lookup = profileManager.fetchPublishedProfile(signer.pubkey)) {
            is PublishedProfileLookup.Found -> {
                val event = lookup.event
                val tags = event.tags.map { it.toList() }
                val asCommunityEvent = CommunityEvent(event.id, event.pubKey, event.createdAt, event.kind, tags, event.content, event.sig)
                PublicProfile.Found(
                    base = ProfileBase(event.content, tags, event.createdAt),
                    name = CommunityProfile.fromEvent(asCommunityEvent)?.label.orEmpty(),
                )
            }
            PublishedProfileLookup.None -> PublicProfile.None
            PublishedProfileLookup.Unreachable -> PublicProfile.Unavailable
        }
    }

    /** Forgets the joined community on this device (membership on the server stays). */
    fun forgetCommunity() {
        prefs.edit().remove(KEY_HOST).remove(KEY_SECURE).remove(KEY_JOINED_PUBKEY).remove(KEY_ASK_NAME_PUBKEY).apply()
        closeClient()
        _setup.value = readSetup()
    }

    /** The running client for the joined community, created and started on first use. */
    @Synchronized
    fun client(): CommunityClient? {
        val setup = readSetup()
        val endpoint = setup.joined?.takeIf { setup.enabled } ?: return null.also { closeClient() }
        val key = endpoint to signer.pubkey
        if (clientKey != key) {
            closeClient()
            // Signing and decrypting stay with the key the client is for: after a switch it refuses
            // until client() builds the next one, so nothing this device keeps for one key
            // reaches another key's events or files.
            val pubkey = key.second
            val boundSigner = {
                nostrSigner.signer.also { check(it.pubKey == pubkey) { "the signing key changed" } }
            }
            // Its own scope, so closing the client ends everything it started.
            val ownScope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
            clientScope = ownScope
            client = CommunityClient(
                endpoint = endpoint,
                socketFactory = sockets,
                http = http,
                signer = QuartzCommunitySigner(signer = boundSigner, publicKey = { pubkey }),
                verifier = QuartzEventVerifier,
                clock = clock,
                scope = ownScope,
                elapsedMillis = { SystemClock.elapsedRealtime() },
                onTiming = { step, millis -> Log.i(TAG, "event=timing step=$step ms=$millis") },
                cipher = QuartzSelfCipher(signer = boundSigner, publicKey = { pubkey }),
                readStore = PrefsReadStateStore(prefs, "$KEY_READ_STATE_PREFIX${key.second}@${endpoint.host}"),
            ).also { started ->
                started.start()
                if (!foreground) schedulePause(started)
                // Time to the first room list, for the spike report (logcat only, no content).
                ownScope.launch {
                    val seconds = started.rooms.first { it.firstListAfterSeconds != null }.firstListAfterSeconds
                    Log.i(TAG, "event=first_room_list seconds=$seconds")
                }
            }
            clientKey = key
        }
        return client
    }

    /** The pre-membership API for the join steps. */
    fun joinSteps(endpoint: CommunityEndpoint): CommunityJoin =
        CommunityJoin(CommunityApi(endpoint, http, signer, QuartzEventVerifier, clock))

    @Synchronized
    private fun closeClient() {
        client?.stop()
        // Give the session a moment to close its socket, then end whatever the client still runs.
        val closing = clientScope
        closing?.launch { delay(CLOSE_GRACE_MS); closing.cancel() }
        client = null
        clientScope = null
        clientKey = null
    }

    private fun readSetup(): CommunitySetup {
        val host = prefs.getString(KEY_HOST, null)
        val joinedBy = prefs.getString(KEY_JOINED_PUBKEY, null)
        // A membership belongs to one key; after a key switch the user joins again.
        val joined = if (host != null && joinedBy == runCatching { signer.pubkey }.getOrNull()) {
            CommunityEndpoint(host, prefs.getBoolean(KEY_SECURE, true))
        } else {
            null
        }
        return CommunitySetup(
            enabled = prefs.getBoolean(KEY_ENABLED, false),
            joined = joined,
            askName = joined != null && prefs.getString(KEY_ASK_NAME_PUBKEY, null) == joinedBy,
        )
    }

    companion object {
        /** The CruxCoach community. Invites name their own host; this is the default for members. */
        val DEFAULT_COMMUNITY = CommunityEndpoint("community.cruxcoach.org")

        /** The preview is in feature and development builds only. */
        val isAvailable: Boolean get() = BuildConfig.DEBUG

        private const val TAG = "CommunityClient"
        private const val CLOSE_GRACE_MS = 2_000L
        private const val BACKGROUND_GRACE_MS = 60_000L
        private const val PREFS = "community_client"
        private const val KEY_ENABLED = "enabled"
        private const val KEY_HOST = "host"
        private const val KEY_SECURE = "secure"
        private const val KEY_JOINED_PUBKEY = "joined_pubkey"
        private const val KEY_ASK_NAME_PUBKEY = "ask_name_pubkey"
        private const val KEY_READ_STATE_PREFIX = "read_state:"
    }
}

/**
 * Read state in the feature's own preferences file, one entry per key and community: the
 * NIP-RS installation ids and the read time per room. No database migration.
 */
internal class PrefsReadStateStore(private val prefs: SharedPreferences, private val key: String) : ReadStateStore {
    override fun load(): StoredReadState {
        val obj = prefs.getString(key, null)?.let { runCatching { Json.parseToJsonElement(it).jsonObject }.getOrNull() }
        val clientId = obj?.get("client_id")?.jsonPrimitive?.contentOrNull?.takeIf { it.isNotEmpty() }
        val slotId = obj?.get("slot_id")?.jsonPrimitive?.contentOrNull?.takeIf { ReadStateBlob.isSlotId(it) }
        val contexts = (obj?.get("contexts") as? JsonObject)?.mapNotNull { (k, v) -> v.jsonPrimitive.longOrNull?.let { k to it } }?.toMap().orEmpty()
        val state = StoredReadState(clientId ?: ReadStateBlob.newSlotId(), slotId ?: ReadStateBlob.newSlotId(), contexts)
        if (clientId == null || slotId == null) save(state)
        return state
    }

    override fun save(state: StoredReadState) {
        val json = buildJsonObject {
            put("client_id", state.clientId)
            put("slot_id", state.slotId)
            putJsonObject("contexts") { state.contexts.forEach { (k, v) -> put(k, v) } }
        }
        prefs.edit().putString(key, json.toString()).apply()
    }
}
