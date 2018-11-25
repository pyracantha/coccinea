package com.github.pyracantha.coccinea.journal

import java.util.UUID.randomUUID

fun version(
    value: String = "version ${randomUUID()}"
) = Version(value)