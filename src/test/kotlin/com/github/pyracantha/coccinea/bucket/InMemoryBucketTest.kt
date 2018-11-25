package com.github.pyracantha.coccinea.bucket

import com.nhaarman.mockitokotlin2.doReturn
import com.nhaarman.mockitokotlin2.mock
import com.nhaarman.mockitokotlin2.verify
import com.nhaarman.mockitokotlin2.whenever
import io.reactivex.schedulers.Schedulers
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.concurrent.ConcurrentHashMap

internal class InMemoryBucketTest {

    private lateinit var storage: ConcurrentHashMap<BucketDocumentId, Document>
    private lateinit var inMemoryBucket: Bucket

    @BeforeEach
    fun setUp() {
        storage = mock()
        inMemoryBucket = InMemoryBucket(storage, Schedulers.trampoline())
    }

    @Test
    fun delegatesGet() {
        val bucketDocumentId = bucketDocumentId()
        val document = document()
        whenever(storage[bucketDocumentId]).doReturn(document)

        val result = inMemoryBucket.get(bucketDocumentId).blockingGet()

        assertThat(result)
                .isEqualTo(document)
    }

    @Test
    fun delegatesPut() {
        val bucketDocumentId = bucketDocumentId()
        val document = document()

        inMemoryBucket.put(bucketDocumentId, document).blockingAwait()

        verify(storage)[bucketDocumentId] = document
    }

    @Test
    fun delegatesRemove() {
        val bucketDocumentId = bucketDocumentId()

        inMemoryBucket.remove(bucketDocumentId).blockingAwait()

        verify(storage).remove(bucketDocumentId)
    }

    @Test
    fun delegatesList() {
        val bucketDocumentId1 = bucketDocumentId()
        val bucketDocumentId2 = bucketDocumentId()
        val keys = mock<ConcurrentHashMap.KeySetView<BucketDocumentId, Document>> {
            on { iterator() } doReturn mutableSetOf(bucketDocumentId1, bucketDocumentId2).iterator()
        }
        whenever(storage.keys).doReturn(keys)

        val observer = inMemoryBucket.list().test()
        observer.await()

        observer.assertValues(bucketDocumentId1, bucketDocumentId2)
    }
}