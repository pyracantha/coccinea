package com.github.pyracantha.coccinea.journal

import java.util.UUID.randomUUID

fun changeId(
    value: String = "change id ${randomUUID()}"
) = ChangeId(
    value = value
)