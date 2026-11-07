package com.cruxcoach.communityclient

/** Why a message or a person is reported: the NIP-56 types the relay accepts at the pinned version. */
enum class ReportType(val wire: String) {
    SPAM("spam"),
    PROFANITY("profanity"),
    IMPERSONATION("impersonation"),
    NUDITY("nudity"),
    ILLEGAL("illegal"),
    MALWARE("malware"),
    OTHER("other"),
}

/** What a report is about. */
sealed interface ReportTarget {
    data class Message(val eventId: String, val author: String) : ReportTarget
    data class Person(val pubkey: String) : ReportTarget
}

/** The longest emoji (or shortcode) the relay takes as a reaction. */
const val MAX_REACTION_LENGTH = 64

/** The longest reason the client sends with a report. */
const val MAX_REPORT_REASON_LENGTH = 500

/**
 * Templates for acting on messages, in the tag shapes the Buzz apps write at the pinned
 * version (mobile/lib/features/channels/channel_management_actions.dart).
 */

/** A reaction (kind:7). Only the target is tagged; the relay derives the room from it. */
fun CommunityEventTemplates.reaction(targetId: String, emoji: String, createdAt: Long): UnsignedEvent {
    requireEventId(targetId)
    val content = emoji.trim()
    require(content.isNotEmpty() && content.length <= MAX_REACTION_LENGTH) { "not a reaction: $emoji" }
    return UnsignedEvent(createdAt, CommunityKinds.REACTION, listOf(listOf("e", targetId)), content)
}

/** Takes back one of your reactions (NIP-09 kind:5 on the reaction event). */
fun CommunityEventTemplates.removeReaction(reactionId: String, createdAt: Long): UnsignedEvent {
    requireEventId(reactionId)
    return UnsignedEvent(createdAt, CommunityKinds.DELETION, listOf(listOf("e", reactionId)), "")
}

/**
 * New text for your own message (kind:40003). Like the Buzz desktop app, the edit tags the
 * room members it mentions ([mentions]), so a name added in an edit notifies them. The Buzz
 * phone app reads an edit's tags as the message's, so the edit also keeps a [broadcast] mark.
 */
fun CommunityEventTemplates.edit(
    channelId: String,
    messageId: String,
    content: String,
    createdAt: Long,
    mentions: List<String> = emptyList(),
    broadcast: Boolean = false,
): UnsignedEvent {
    requireEventId(messageId)
    require(content.isNotBlank()) { "an edit needs text" }
    val tags = listOf(listOf("h", channelId), listOf("e", messageId)) +
        mentions.distinct().map { listOf("p", it) } +
        listOfNotNull(listOf("broadcast", "1").takeIf { broadcast })
    return UnsignedEvent(createdAt, CommunityKinds.STREAM_MESSAGE_EDIT, tags, content)
}

/** Deletes your own message (NIP-09 kind:5 with the room). */
fun CommunityEventTemplates.deleteOwn(channelId: String, messageId: String, createdAt: Long): UnsignedEvent {
    requireEventId(messageId)
    return UnsignedEvent(createdAt, CommunityKinds.DELETION, listOf(listOf("h", channelId), listOf("e", messageId)), "")
}

/** An admin removes someone's message (NIP-29 kind:9005). The relay checks the role. */
fun CommunityEventTemplates.removeMessage(channelId: String, messageId: String, createdAt: Long): UnsignedEvent {
    requireEventId(messageId)
    return UnsignedEvent(createdAt, CommunityKinds.GROUP_DELETE_EVENT, listOf(listOf("h", channelId), listOf("e", messageId)), "")
}

/**
 * A NIP-56 report (kind:1984). The relay wants exactly one `p` tag, the type on the
 * target tag, and keeps reports for the moderators instead of showing them to anyone.
 */
fun CommunityEventTemplates.report(target: ReportTarget, type: ReportType, reason: String, createdAt: Long): UnsignedEvent {
    val tags = when (target) {
        is ReportTarget.Message -> {
            requireEventId(target.eventId)
            require(target.author.isHex(64)) { "not a pubkey: ${target.author}" }
            listOf(listOf("e", target.eventId, type.wire), listOf("p", target.author))
        }
        is ReportTarget.Person -> {
            require(target.pubkey.isHex(64)) { "not a pubkey: ${target.pubkey}" }
            listOf(listOf("p", target.pubkey, type.wire))
        }
    }
    return UnsignedEvent(createdAt, CommunityKinds.REPORT, tags, reason.trim().take(MAX_REPORT_REASON_LENGTH))
}

private fun requireEventId(id: String) = require(id.isHex(64)) { "not an event id: $id" }
