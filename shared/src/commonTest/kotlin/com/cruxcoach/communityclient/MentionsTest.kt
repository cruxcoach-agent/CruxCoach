package com.cruxcoach.communityclient

import com.cruxcoach.communityclient.CommunityTestEvents.ALICE
import com.cruxcoach.communityclient.CommunityTestEvents.BOB
import com.cruxcoach.communityclient.CommunityTestEvents.CHANNEL
import com.cruxcoach.communityclient.CommunityTestEvents.event
import com.cruxcoach.communityclient.CommunityTestEvents.id
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull

class MentionsTest {

    @Test
    fun queryAt_findsTheAtTheUserIsTyping() {
        assertEquals(MentionQuery(0, ""), Mentions.queryAt("@", 1))
        assertEquals(MentionQuery(6, "Al"), Mentions.queryAt("hello @Al", 9))
        assertEquals(MentionQuery(6, "A"), Mentions.queryAt("hello @Al", 8))
        // Not after a word (an e-mail address), not across a space, not after the cursor.
        assertNull(Mentions.queryAt("mail me@host", 12))
        assertNull(Mentions.queryAt("@Al ice", 7))
        assertNull(Mentions.queryAt("hello", 5))
        assertNull(Mentions.queryAt("@Al", 0))
    }

    @Test
    fun candidates_putNamesThatStartWithTheQueryFirst() {
        val members = listOf(
            MentionCandidate("c".repeat(64), "Mia"),
            MentionCandidate(ALICE, "Alice"),
            MentionCandidate(BOB, "Kalia"),
        )
        assertEquals(listOf("Alice", "Kalia"), Mentions.candidates(members, "al").map { it.label })
        assertEquals(listOf("Alice", "Kalia", "Mia"), Mentions.candidates(members, "").map { it.label })
        assertEquals(1, Mentions.candidates(members, "", limit = 1).size)
    }

    @Test
    fun recipients_keepOnlyMembersStillNamedInTheText() {
        val alice = MentionCandidate(ALICE, "Alice")
        val bob = MentionCandidate(BOB, "Bob")
        val stranger = MentionCandidate("d".repeat(64), "Stranger")
        val members = setOf(ALICE, BOB)
        assertEquals(listOf(ALICE), Mentions.recipients("hi @Alice", listOf(alice, bob), members))
        assertEquals(listOf(ALICE, BOB), Mentions.recipients("@Alice and @Bob", listOf(alice, bob, alice), members))
        // A picked person who is not in the room is not tagged: the relay would refuse the message.
        assertEquals(emptyList(), Mentions.recipients("@Stranger", listOf(stranger), members))
        // A shorter name inside a longer one does not count.
        assertEquals(listOf(BOB), Mentions.recipients("@Bobby and @Bob.", listOf(MentionCandidate(ALICE, "Bobb"), bob), members))
        val many = (1..40).map { MentionCandidate(it.toString(16).padStart(64, '0'), "p$it") }
        val text = many.joinToString(" ") { "@" + it.label }
        assertEquals(MAX_MENTIONS, Mentions.recipients(text, many, many.map { it.pubkey }.toSet()).size)
    }

    @Test
    fun edits_tagWhomTheirTextMentions() {
        val target = id(10)
        val edit = CommunityEventTemplates.edit(CHANNEL, target, "now with @Bob", 1_000, mentions = listOf(BOB, BOB))
        assertEquals(listOf(listOf("h", CHANNEL), listOf("e", target), listOf("p", BOB)), edit.tags)
        // A broadcast reply stays one: the Buzz phone app reads an edit's tags as the message's.
        val broadcastEdit = CommunityEventTemplates.edit(CHANNEL, target, "x", 1_000, broadcast = true)
        assertEquals(listOf("broadcast", "1"), broadcastEdit.tags.last())

        // A name added in an edit counts as a mention of the message; one in the original stays.
        val message = CommunityMessage.fromEvent(event(9, listOf(listOf("h", CHANNEL), listOf("p", ALICE)), "hi @Alice", id = target))!!
        val applied = applyAux(
            listOf(message),
            listOf(event(40003, listOf(listOf("h", CHANNEL), listOf("e", target), listOf("p", BOB)), "hi @Alice and @Bob", id = id(11), createdAt = message.createdAt + 1)),
        ).single()
        assertEquals(listOf(ALICE, BOB), applied.mentions)
        assertEquals("hi @Alice and @Bob", applied.displayContent)
    }
}
