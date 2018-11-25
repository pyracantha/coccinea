package com.github.pyracantha.coccinea.database

import java.util.UUID.randomUUID

fun databaseId(
    value: String = "db id ${randomUUID()}"
) = DatabaseId(value)