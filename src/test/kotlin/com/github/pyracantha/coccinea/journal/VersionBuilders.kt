package com.github.pyracantha.coccinea.journal

import kotlin.math.absoluteValue
import kotlin.random.Random

fun version(
    sequence: Long = Random.nextLong().absoluteValue,
    timestamp: Long = Random.nextLong().absoluteValue
) = Version(
    sequence = sequence,
    timestamp = timestamp
)