package com.github.pyracantha.coccinea.replication

import com.github.pyracantha.coccinea.journal.Action
import com.github.pyracantha.coccinea.journal.Action.SAVE
import com.github.pyracantha.coccinea.journal.DocumentId
import com.github.pyracantha.coccinea.journal.documentId

fun replicationEvent(
    documentId: DocumentId = documentId(),
    action: Action = SAVE
) = ReplicationEvent(
    documentId = documentId,
    action = action
)