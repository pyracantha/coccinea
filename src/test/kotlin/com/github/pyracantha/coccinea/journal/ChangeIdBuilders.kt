package com.github.pyracantha.coccinea.journal

fun changeId(
    documentId: DocumentId = documentId(),
    version: Version = version()
) = ChangeId(
    documentId = documentId,
    version = version
)