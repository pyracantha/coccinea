package com.github.pyracantha.coccinea.bucket

import java.util.UUID.randomUUID

fun document(
    value: String = "payload ${randomUUID()}"
) = Document(value)