package com.github.pyracantha.coccinea.bucket

import com.github.pyracantha.coccinea.journal.documentId
import com.github.pyracantha.coccinea.journal.version
import io.reactivex.schedulers.Schedulers
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

internal class BucketDocumentIdFactoryImplTest {

    private lateinit var bucketDocumentIdFactory: BucketDocumentIdFactory

    @BeforeEach
    fun setUp() {
        bucketDocumentIdFactory = BucketDocumentIdFactoryImpl(Schedulers.trampoline())
    }

    @Test
    fun test() {
        val documentId = documentId()
        val version = version()
        val expected = bucketDocumentId(value = "${documentId.value}-${version.value}")

        val bucketDocumentId = bucketDocumentIdFactory.create(documentId, version).blockingGet()

        assertThat(bucketDocumentId)
            .isEqualTo(expected)
    }
}