package com.github.pyracantha.coccinea.journal

import java.util.UUID.randomUUID

fun documentId(
    value: String = "document id ${randomUUID()}"
) = DocumentId(value = value)