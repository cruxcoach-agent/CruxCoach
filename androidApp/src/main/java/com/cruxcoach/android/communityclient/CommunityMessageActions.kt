package com.cruxcoach.android.communityclient

import androidx.compose.foundation.clickable
import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.ExperimentalLayoutApi
import androidx.compose.foundation.layout.FlowRow
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.rememberScrollState
import androidx.compose.foundation.selection.selectable
import androidx.compose.foundation.verticalScroll
import androidx.compose.material.icons.Icons
import androidx.compose.material.icons.outlined.ContentCopy
import androidx.compose.material.icons.outlined.Delete
import androidx.compose.material.icons.outlined.Edit
import androidx.compose.material.icons.outlined.Flag
import androidx.compose.material.icons.outlined.RemoveCircleOutline
import androidx.compose.material3.AlertDialog
import androidx.compose.material3.ExperimentalMaterial3Api
import androidx.compose.material3.FilterChip
import androidx.compose.material3.Icon
import androidx.compose.material3.ListItem
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.ModalBottomSheet
import androidx.compose.material3.OutlinedTextField
import androidx.compose.material3.RadioButton
import androidx.compose.material3.Text
import androidx.compose.material3.TextButton
import androidx.compose.runtime.Composable
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.saveable.rememberSaveable
import androidx.compose.runtime.setValue
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.platform.LocalClipboardManager
import androidx.compose.ui.platform.testTag
import androidx.compose.ui.res.stringResource
import androidx.compose.ui.semantics.Role
import androidx.compose.ui.text.AnnotatedString
import androidx.compose.ui.unit.dp
import com.cruxcoach.android.R
import com.cruxcoach.communityclient.CommunityClient
import com.cruxcoach.communityclient.CommunityMessage
import com.cruxcoach.communityclient.PublishResult
import com.cruxcoach.communityclient.ReportTarget
import com.cruxcoach.communityclient.ReportType
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.launch

/** What happened to the last message action, for a short note on screen. */
sealed interface ActionFeedback {
    data class Failed(val reason: PublishError) : ActionFeedback
    data object Reported : ActionFeedback
}

/** Room roles that may remove other people's messages (the relay checks this too). */
private val MODERATOR_ROLES = setOf("owner", "admin")

fun canModerate(role: String?): Boolean = role in MODERATOR_ROLES

/** Reactions, edits, deletions and reports, shared by the room and the thread screen. */
class CommunityMessageActions(
    private val client: CommunityClient?,
    private val channelId: String,
    private val scope: CoroutineScope,
) {
    private val _feedback = MutableStateFlow<ActionFeedback?>(null)
    val feedback: StateFlow<ActionFeedback?> = _feedback.asStateFlow()

    /** Adds your reaction with [emoji], or takes it back if you already reacted so. */
    fun toggleReaction(message: CommunityMessage, emoji: String) {
        val c = client ?: return
        val mine = message.reactionId(emoji, c.me)
        run { if (mine != null) c.removeReaction(mine) else c.react(message.id, emoji) }
    }

    fun edit(message: CommunityMessage, text: String) {
        val c = client ?: return
        if (text.isBlank() || text.trim() == message.displayContent.trim()) return
        run { c.editMessage(channelId, message.id, text.trim()) }
    }

    fun delete(message: CommunityMessage) {
        val c = client ?: return
        run { c.deleteMessage(channelId, message.id) }
    }

    fun remove(message: CommunityMessage) {
        val c = client ?: return
        run { c.removeMessage(channelId, message.id) }
    }

    fun report(target: ReportTarget, type: ReportType, reason: String) {
        val c = client ?: return
        run(onDone = { _feedback.value = ActionFeedback.Reported }) { c.report(target, type, reason) }
    }

    fun clearFeedback() {
        _feedback.value = null
    }

    private fun run(onDone: () -> Unit = {}, block: suspend () -> PublishResult) {
        scope.launch {
            val error = publishError(block)
            if (error != null) _feedback.value = ActionFeedback.Failed(error) else onDone()
        }
    }
}

/** The quick reactions offered in the message menu. */
private val QUICK_REACTIONS = listOf("👍", "❤️", "😂", "🎉", "🙏", "👀")

/** Which dialog the message menu opened. */
private sealed interface MenuDialog {
    data object Edit : MenuDialog
    data object Delete : MenuDialog
    data object Remove : MenuDialog
    data class Report(val person: Boolean) : MenuDialog
}

/**
 * Long-press menu for one message: quick reactions, copy, and what this user may do
 * with it (edit and delete their own; remove as an admin; report someone else's).
 */
@OptIn(ExperimentalMaterial3Api::class, ExperimentalLayoutApi::class)
@Composable
fun MessageMenu(
    message: CommunityMessage,
    me: String,
    canModerate: Boolean,
    actions: CommunityMessageActions,
    onDismiss: () -> Unit,
) {
    val clipboard = LocalClipboardManager.current
    var dialog by rememberSaveable(stateSaver = menuDialogSaver) { mutableStateOf<MenuDialog?>(null) }
    val own = message.author == me
    if (dialog == null) {
        ModalBottomSheet(onDismissRequest = onDismiss) {
            Column(Modifier.padding(bottom = 24.dp).testTag("community_message_menu")) {
                FlowRow(
                    Modifier.fillMaxWidth().padding(horizontal = 16.dp, vertical = 8.dp),
                    horizontalArrangement = Arrangement.spacedBy(8.dp),
                ) {
                    QUICK_REACTIONS.forEach { emoji ->
                        FilterChip(
                            selected = message.reactionId(emoji, me) != null,
                            onClick = { actions.toggleReaction(message, emoji); onDismiss() },
                            label = { Text(emoji, style = MaterialTheme.typography.titleMedium) },
                        )
                    }
                }
                MenuItem(Icons.Outlined.ContentCopy, stringResource(R.string.community_client_action_copy)) {
                    clipboard.setText(AnnotatedString(message.displayContent))
                    onDismiss()
                }
                if (own && !message.isDeleted) {
                    MenuItem(Icons.Outlined.Edit, stringResource(R.string.community_client_action_edit)) { dialog = MenuDialog.Edit }
                    MenuItem(Icons.Outlined.Delete, stringResource(R.string.community_client_action_delete)) { dialog = MenuDialog.Delete }
                }
                if (!own && canModerate && !message.isDeleted) {
                    MenuItem(Icons.Outlined.RemoveCircleOutline, stringResource(R.string.community_client_action_remove)) { dialog = MenuDialog.Remove }
                }
                if (!own) {
                    MenuItem(Icons.Outlined.Flag, stringResource(R.string.community_client_action_report_message)) { dialog = MenuDialog.Report(person = false) }
                    MenuItem(Icons.Outlined.Flag, stringResource(R.string.community_client_action_report_person)) { dialog = MenuDialog.Report(person = true) }
                }
            }
        }
    }
    when (val d = dialog) {
        null -> Unit
        MenuDialog.Edit -> EditDialog(message.displayContent, onSave = { actions.edit(message, it); onDismiss() }, onDismiss = onDismiss)
        MenuDialog.Delete -> ConfirmDialog(
            title = stringResource(R.string.community_client_delete_title),
            body = stringResource(R.string.community_client_delete_body),
            confirm = stringResource(R.string.community_client_action_delete),
            onConfirm = { actions.delete(message); onDismiss() },
            onDismiss = onDismiss,
        )
        MenuDialog.Remove -> ConfirmDialog(
            title = stringResource(R.string.community_client_remove_title),
            body = stringResource(R.string.community_client_remove_body),
            confirm = stringResource(R.string.community_client_action_remove),
            onConfirm = { actions.remove(message); onDismiss() },
            onDismiss = onDismiss,
        )
        is MenuDialog.Report -> ReportDialog(
            person = d.person,
            onSend = { type, reason ->
                val target = if (d.person) ReportTarget.Person(message.author) else ReportTarget.Message(message.id, message.author)
                actions.report(target, type, reason)
                onDismiss()
            },
            onDismiss = onDismiss,
        )
    }
}

@Composable
private fun MenuItem(icon: androidx.compose.ui.graphics.vector.ImageVector, label: String, onClick: () -> Unit) {
    ListItem(
        headlineContent = { Text(label) },
        leadingContent = { Icon(icon, contentDescription = null) },
        modifier = Modifier.clickable(onClick = onClick),
    )
}

@Composable
private fun EditDialog(initial: String, onSave: (String) -> Unit, onDismiss: () -> Unit) {
    var text by rememberSaveable { mutableStateOf(initial) }
    AlertDialog(
        onDismissRequest = onDismiss,
        title = { Text(stringResource(R.string.community_client_edit_title)) },
        text = {
            OutlinedTextField(
                value = text,
                onValueChange = { text = it },
                maxLines = 8,
                modifier = Modifier.fillMaxWidth().testTag("community_edit_field"),
            )
        },
        confirmButton = {
            TextButton(onClick = { onSave(text) }, enabled = text.isNotBlank() && text.trim() != initial.trim()) {
                Text(stringResource(R.string.action_save))
            }
        },
        dismissButton = { TextButton(onClick = onDismiss) { Text(stringResource(R.string.action_cancel)) } },
    )
}

@Composable
private fun ConfirmDialog(title: String, body: String, confirm: String, onConfirm: () -> Unit, onDismiss: () -> Unit) {
    AlertDialog(
        onDismissRequest = onDismiss,
        title = { Text(title) },
        text = { Text(body) },
        confirmButton = {
            TextButton(onClick = onConfirm) { Text(confirm, color = MaterialTheme.colorScheme.error) }
        },
        dismissButton = { TextButton(onClick = onDismiss) { Text(stringResource(R.string.action_cancel)) } },
    )
}

@Composable
private fun ReportDialog(person: Boolean, onSend: (ReportType, String) -> Unit, onDismiss: () -> Unit) {
    var type by rememberSaveable { mutableStateOf<ReportType?>(null) }
    var reason by rememberSaveable { mutableStateOf("") }
    AlertDialog(
        onDismissRequest = onDismiss,
        title = {
            Text(stringResource(if (person) R.string.community_client_report_person_title else R.string.community_client_report_message_title))
        },
        text = {
            Column(Modifier.verticalScroll(rememberScrollState()), verticalArrangement = Arrangement.spacedBy(4.dp)) {
                Text(stringResource(R.string.community_client_report_body), style = MaterialTheme.typography.bodyMedium)
                ReportType.entries.forEach { option ->
                    Row(
                        Modifier.fillMaxWidth()
                            .selectable(selected = type == option, onClick = { type = option }, role = Role.RadioButton)
                            .padding(vertical = 2.dp),
                        verticalAlignment = Alignment.CenterVertically,
                    ) {
                        RadioButton(selected = type == option, onClick = null)
                        Text(reportTypeLabel(option), style = MaterialTheme.typography.bodyMedium, modifier = Modifier.padding(start = 8.dp))
                    }
                }
                OutlinedTextField(
                    value = reason,
                    onValueChange = { if (it.length <= com.cruxcoach.communityclient.MAX_REPORT_REASON_LENGTH) reason = it },
                    placeholder = { Text(stringResource(R.string.community_client_report_reason_hint)) },
                    maxLines = 4,
                    modifier = Modifier.fillMaxWidth().testTag("community_report_reason"),
                )
            }
        },
        confirmButton = {
            TextButton(onClick = { type?.let { onSend(it, reason) } }, enabled = type != null) {
                Text(stringResource(R.string.community_client_report_send))
            }
        },
        dismissButton = { TextButton(onClick = onDismiss) { Text(stringResource(R.string.action_cancel)) } },
    )
}

@Composable
private fun reportTypeLabel(type: ReportType): String = stringResource(
    when (type) {
        ReportType.SPAM -> R.string.community_client_report_spam
        ReportType.PROFANITY -> R.string.community_client_report_profanity
        ReportType.IMPERSONATION -> R.string.community_client_report_impersonation
        ReportType.NUDITY -> R.string.community_client_report_nudity
        ReportType.ILLEGAL -> R.string.community_client_report_illegal
        ReportType.MALWARE -> R.string.community_client_report_malware
        ReportType.OTHER -> R.string.community_client_report_other
    },
)

/** The reactions under a message; tapping one adds or takes back your own. */
@OptIn(ExperimentalLayoutApi::class)
@Composable
fun ReactionChips(message: CommunityMessage, me: String, onToggle: ((String) -> Unit)?) {
    if (message.reactions.isEmpty()) return
    FlowRow(horizontalArrangement = Arrangement.spacedBy(6.dp), modifier = Modifier.padding(top = 2.dp)) {
        message.reactions.entries.sortedByDescending { it.value.size }.forEach { (emoji, authors) ->
            FilterChip(
                selected = me in authors,
                onClick = { onToggle?.invoke(emoji) },
                enabled = onToggle != null,
                label = { Text("$emoji ${authors.size}", style = MaterialTheme.typography.labelMedium) },
            )
        }
    }
}

/** Keeps the open dialog across rotation. */
private val menuDialogSaver = androidx.compose.runtime.saveable.Saver<MenuDialog?, String>(
    save = {
        when (it) {
            null -> ""
            MenuDialog.Edit -> "edit"
            MenuDialog.Delete -> "delete"
            MenuDialog.Remove -> "remove"
            is MenuDialog.Report -> if (it.person) "report_person" else "report_message"
        }
    },
    restore = {
        when (it) {
            "edit" -> MenuDialog.Edit
            "delete" -> MenuDialog.Delete
            "remove" -> MenuDialog.Remove
            "report_person" -> MenuDialog.Report(person = true)
            "report_message" -> MenuDialog.Report(person = false)
            else -> null
        }
    },
)
