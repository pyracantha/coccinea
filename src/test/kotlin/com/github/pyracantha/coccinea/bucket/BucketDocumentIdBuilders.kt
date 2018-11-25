package com.github.pyracantha.coccinea.bucket

import java.util.UUID.randomUUID

fun bucketDocumentId(
    value: String = "bucket document id ${randomUUID()}"
) = BucketDocumentId(value)