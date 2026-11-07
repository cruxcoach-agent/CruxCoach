package com.cruxcoach.communityclient

/** The most people one message may mention (the Buzz apps' limit). */
const val MAX_MENTIONS = 32

/** Someone the composer can mention: a room member and the name the user picked them by. */
data class MentionCandidate(val pubkey: String, val label: String)

/**
 * The members of one room (relay-signed kind:39002), newest list only.
 */
fun CommunityFilters.channelMembers(channelId: String): RelayFilter =
    RelayFilter(kinds = listOf(CommunityKinds.GROUP_MEMBERS), tags = mapOf('d' to listOf(channelId)), limit = 1)

object Mentions {
    /**
     * The `@query` the user is typing at [cursor] in [text], or null. The query starts
     * after an `@` at the start of the text or after whitespace, and runs to the cursor
     * without whitespace.
     */
    fun queryAt(text: String, cursor: Int): MentionQuery? {
        if (cursor < 1 || cursor > text.length) return null
        var i = cursor - 1
        while (i >= 0 && !text[i].isWhitespace() && text[i] != '@') i--
        if (i < 0 || text[i] != '@') return null
        if (i > 0 && !text[i - 1].isWhitespace()) return null
        return MentionQuery(start = i, query = text.substring(i + 1, cursor))
    }

    /** Members matching [query], names starting with it first, at most [limit]. */
    fun candidates(members: List<MentionCandidate>, query: String, limit: Int = 6): List<MentionCandidate> {
        val q = query.trim().lowercase()
        val matching = members.filter { q.isEmpty() || it.label.lowercase().contains(q) }
        return matching.sortedWith(compareBy<MentionCandidate> { !it.label.lowercase().startsWith(q) }.thenBy { it.label.lowercase() })
            .take(limit)
    }

    /**
     * The pubkeys to tag for [text]: those of [chosen] whose `@label` still appears in
     * the text, that are room [members], in order, at most [MAX_MENTIONS].
     */
    fun recipients(text: String, chosen: List<MentionCandidate>, members: Set<String>): List<String> =
        chosen.filter { it.pubkey.isHex(64) && it.pubkey in members && names(text, it.label) }
            .map { it.pubkey }
            .distinct()
            .take(MAX_MENTIONS)

    /** Whether [text] has `@label` as a whole name ("@Al" is not in "@Alice"). */
    private fun names(text: String, label: String): Boolean {
        val needle = "@$label"
        var from = 0
        while (true) {
            val at = text.indexOf(needle, from)
            if (at < 0) return false
            val next = text.getOrNull(at + needle.length)
            if (next == null || !next.isLetterOrDigit()) return true
            from = at + 1
        }
    }
}

/** An `@` the user is typing: where it starts and what follows it so far. */
data class MentionQuery(val start: Int, val query: String)
