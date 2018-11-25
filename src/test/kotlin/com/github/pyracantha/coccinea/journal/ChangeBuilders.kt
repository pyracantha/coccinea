package com.github.pyracantha.coccinea.journal

import com.github.pyracantha.coccinea.journal.Action.SAVE

fun change(
    documentId: DocumentId = documentId(),
    version: Version = version(),
    action: Action = SAVE,
    sequence: Sequence = sequence()
) = Change(
    documentId = documentId,
    version = version,
    action = action,
    sequence = sequence
)