package com.github.pyracantha.coccinea.bucket

import io.reactivex.schedulers.Schedulers
import com.github.pyracantha.coccinea.journal.DocumentId
import com.github.pyracantha.coccinea.journal.Version
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
        val documentId = DocumentId(value = "doc")
        val version = Version(value = "version")
        val expected = bucketDocumentId(value = "${documentId.value}-${version.value}")

        val bucketDocumentId = bucketDocumentIdFactory.create(documentId, version).blockingGet()

        assertThat(bucketDocumentId)
                .isEqualTo(expected)
    }
}