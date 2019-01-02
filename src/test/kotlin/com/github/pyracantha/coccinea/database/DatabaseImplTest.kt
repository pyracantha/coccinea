package com.github.pyracantha.coccinea.database

import com.github.pyracantha.coccinea.bucket.Bucket
import com.github.pyracantha.coccinea.bucket.BucketDocumentIdFactory
import com.github.pyracantha.coccinea.bucket.bucketDocumentId
import com.github.pyracantha.coccinea.bucket.document
import com.nhaarman.mockitokotlin2.any
import com.nhaarman.mockitokotlin2.doReturn
import com.nhaarman.mockitokotlin2.eq
import com.nhaarman.mockitokotlin2.mock
import com.nhaarman.mockitokotlin2.times
import com.nhaarman.mockitokotlin2.verify
import com.nhaarman.mockitokotlin2.verifyNoMoreInteractions
import com.nhaarman.mockitokotlin2.verifyZeroInteractions
import com.nhaarman.mockitokotlin2.whenever
import io.reactivex.Completable
import io.reactivex.Maybe
import io.reactivex.Observable
import io.reactivex.Single
import com.github.pyracantha.coccinea.journal.Action.DELETE
import com.github.pyracantha.coccinea.journal.Action.SAVE
import com.github.pyracantha.coccinea.journal.DocumentIdFactory
import com.github.pyracantha.coccinea.journal.Journal
import com.github.pyracantha.coccinea.journal.VersionFactory
import com.github.pyracantha.coccinea.journal.change
import com.github.pyracantha.coccinea.journal.changeId
import com.github.pyracantha.coccinea.journal.documentId
import com.github.pyracantha.coccinea.journal.version
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import com.github.pyracantha.coccinea.replication.ReplicationPeer
import com.github.pyracantha.coccinea.replication.Replicator
import com.github.pyracantha.coccinea.replication.replicationEvent

internal class DatabaseImplTest {

    private lateinit var databaseId: DatabaseId
    private lateinit var bucket: Bucket
    private lateinit var journal: Journal
    private lateinit var documentIdFactory: DocumentIdFactory
    private lateinit var versionFactory: VersionFactory
    private lateinit var bucketDocumentIdFactory: BucketDocumentIdFactory
    private lateinit var replicator: Replicator

    private lateinit var database: Database
    private lateinit var replicationPeer: ReplicationPeer

    @BeforeEach
    fun setUp() {
        databaseId = databaseId()
        bucket = mock()
        journal = mock()
        documentIdFactory = mock()
        versionFactory = mock()
        bucketDocumentIdFactory = mock()
        replicator = mock()

        database = DatabaseImpl(
                databaseId,
                bucket,
                journal,
                documentIdFactory,
                versionFactory,
                bucketDocumentIdFactory,
                replicator
        )

        replicationPeer = database as ReplicationPeer
    }

    @Test
    fun getsLatestVersionOfDocument() {
        val documentId = documentId()
        val version = version()
        val change = change(documentId = documentId, action = SAVE, version = version)
        val bucketDocumentId = bucketDocumentId()
        val document = document()
        whenever(journal.latestChangeOf(documentId)).doReturn(Maybe.just(change))
        whenever(bucketDocumentIdFactory.create(documentId, version)).doReturn(Single.just(bucketDocumentId))
        whenever(bucket.get(bucketDocumentId)).doReturn(Maybe.just(document))

        val observer = database.get(documentId).test()
        observer.await()

        observer.assertResult(document)
    }

    @Test
    fun getsNoResultForUnknownDocument() {
        val documentId = documentId()
        val version = version()
        val change = change(documentId = documentId, action = SAVE, version = version)
        val bucketDocumentId = bucketDocumentId()
        whenever(journal.latestChangeOf(documentId)).doReturn(Maybe.just(change))
        whenever(bucketDocumentIdFactory.create(documentId, version)).doReturn(Single.just(bucketDocumentId))
        whenever(bucket.get(bucketDocumentId)).doReturn(Maybe.empty())

        val observer = database.get(documentId).test()
        observer.await()

        observer.assertResult()
    }

    @Test
    fun ignoresDeletedDocument() {
        val documentId = documentId()
        val change = change(action = DELETE)
        whenever(journal.latestChangeOf(documentId)).doReturn(Maybe.just(change))

        val observer = database.get(documentId).test()
        observer.await()

        observer.assertResult()
    }

    @Test
    fun createsDocument() {
        val documentId = documentId()
        val version = version()
        val bucketDocumentId = bucketDocumentId()
        val document = document()
        whenever(documentIdFactory.create()).doReturn(Single.just(documentId))
        whenever(versionFactory.create()).doReturn(Single.just(version))
        whenever(bucketDocumentIdFactory.create(documentId, version)).doReturn(Single.just(bucketDocumentId))
        whenever(bucket.put(bucketDocumentId, document)).doReturn(Completable.complete())
        whenever(journal.insert(documentId, version, SAVE)).doReturn(Single.just(changeId()))

        val observer = database.put(document).test()
        observer.await()

        observer.assertResult(documentId)
        verify(bucket).put(bucketDocumentId, document)
        verify(journal).insert(documentId, version, SAVE)
    }

    @Test
    fun createDocumentFailsDuringBucketPutPreventsFromJournalModification() {
        val documentId = documentId()
        val version = version()
        val bucketDocumentId = bucketDocumentId()
        val document = document()
        val error = IllegalStateException()
        whenever(documentIdFactory.create()).doReturn(Single.just(documentId))
        whenever(versionFactory.create()).doReturn(Single.just(version))
        whenever(bucketDocumentIdFactory.create(documentId, version)).doReturn(Single.just(bucketDocumentId))
        whenever(bucket.put(bucketDocumentId, document)).doReturn(Completable.error(error))

        val observer = database.put(document).test()
        observer.await()

        observer.assertError(error)
        verify(bucket).put(bucketDocumentId, document)
        verifyZeroInteractions(journal)
    }

    @Test
    fun updatesDocument() {
        val documentId = documentId()
        val version = version(sequence = 2)
        val bucketDocumentId = bucketDocumentId()
        val document = document()
        val change = change(version = version(sequence = 1))
        whenever(journal.latestChangeOf(documentId)).doReturn(Maybe.just(change))
        whenever(versionFactory.create(change.version)).doReturn(Single.just(version))
        whenever(bucketDocumentIdFactory.create(documentId, version)).doReturn(Single.just(bucketDocumentId))
        whenever(bucket.put(bucketDocumentId, document)).doReturn(Completable.complete())
        whenever(journal.insert(documentId, version, SAVE)).doReturn(Single.just(changeId()))

        val observer = database.put(documentId, document).test()
        observer.await()

        observer.assertResult()
        verify(bucket).put(bucketDocumentId, document)
        verify(journal).insert(documentId, version, SAVE)
    }

    @Test
    fun updateDocumentFailsDuringBucketPutPreventsFromJournalModification() {
        val documentId = documentId()
        val version = version(sequence = 2)
        val bucketDocumentId = bucketDocumentId()
        val document = document()
        val error = IllegalStateException()
        val change = change(version = version(sequence = 1))
        whenever(journal.latestChangeOf(documentId)).doReturn(Maybe.just(change))
        whenever(versionFactory.create(change.version)).doReturn(Single.just(version))
        whenever(bucketDocumentIdFactory.create(documentId, version)).doReturn(Single.just(bucketDocumentId))
        whenever(bucket.put(bucketDocumentId, document)).doReturn(Completable.error(error))

        val observer = database.put(documentId, document).test()
        observer.await()

        observer.assertError(error)
        verify(bucket).put(bucketDocumentId, document)
        verify(journal, times(0)).insert(any(), any(), any())
    }

    @Test
    fun updateDocumentFailsDuringRetrievalOfLatestChange() {
        val documentId = documentId()
        val document = document()
        whenever(journal.latestChangeOf(documentId)).doReturn(Maybe.empty())

        val observer = database.put(documentId, document).test()
        observer.await()

        observer.assertError(java.lang.IllegalStateException::class.java)
    }

    @Test
    fun removesDocument() {
        val documentId = documentId()
        val version = version(sequence = 2)
        val change = change(version = version(sequence = 1))
        whenever(journal.latestChangeOf(documentId)).doReturn(Maybe.just(change))
        whenever(versionFactory.create(change.version)).doReturn(Single.just(version))
        whenever(journal.insert(documentId, version, DELETE)).doReturn(Single.just(changeId()))

        val observer = database.remove(documentId).test()
        observer.await()

        observer.assertResult()
        verify(journal).insert(documentId, version, DELETE)
        verifyNoMoreInteractions(bucket)
    }

    @Test
    fun removeDocumentFailsDuringRetrievalOfLatestChange() {
        val documentId = documentId()
        whenever(journal.latestChangeOf(documentId)).doReturn(Maybe.empty())

        val observer = database.remove(documentId).test()

        observer.assertError(java.lang.IllegalStateException::class.java)
    }

    @Test
    fun delegatesList() {
        val documentId = documentId()
        whenever(journal.list()).doReturn(Observable.just(documentId))

        val observer = database.list().test()
        observer.await()

        observer.assertResult(documentId)
        verify(journal).list()
    }

    @Test
    fun delegatesReplicate() {
        val event = replicationEvent()
        val replicationPeer: ReplicationPeer = mock()
        whenever(replicator.replicate(any(), eq(replicationPeer))).doReturn(Observable.just(event))

        val observer = database.replicate(replicationPeer).test().await()
        observer.await()

        observer.assertResult(event)
        verify(replicator).replicate(any(), eq(replicationPeer))
    }

    @Test
    fun replicationPeerDelegatesDatabaseId() {
        val databaseId = replicationPeer.databaseId().blockingGet()

        assertThat(databaseId)
                .isEqualTo(database.databaseId)
    }

    @Test
    fun replicationPeerDelegatesExists() {
        val documentId = documentId()
        val version = version()
        val exists = true
        whenever(journal.exists(documentId, version)).doReturn(Single.just(exists))

        val observer = replicationPeer.exists(documentId, version).test()
        observer.await()

        observer.assertResult(exists)
        verify(journal).exists(documentId, version)
    }

    @Test
    fun replicationPeerGetsDocument() {
        val documentId = documentId()
        val version = version()
        val bucketDocumentId = bucketDocumentId()
        val document = document()
        whenever(bucketDocumentIdFactory.create(documentId, version)).doReturn(Single.just(bucketDocumentId))
        whenever(bucket.get(bucketDocumentId)).doReturn(Maybe.just(document))

        val observer = replicationPeer.get(documentId, version).test()
        observer.await()

        observer.assertResult(document)
    }

    @Test
    fun replicationPeerGetsNoResultForUnknownDocument() {
        val documentId = documentId()
        val version = version()
        val bucketDocumentId = bucketDocumentId()
        whenever(bucketDocumentIdFactory.create(documentId, version)).doReturn(Single.just(bucketDocumentId))
        whenever(bucket.get(bucketDocumentId)).doReturn(Maybe.empty())

        val observer = replicationPeer.get(documentId, version).test()
        observer.await()

        observer.assertResult()
    }

    @Test
    fun replicationPeerPutsDocumentSave() {
        val documentId = documentId()
        val version = version()
        val bucketDocumentId = bucketDocumentId()
        val document = document()
        whenever(journal.exists(documentId, version)).doReturn(Single.just(false))
        whenever(bucketDocumentIdFactory.create(documentId, version)).doReturn(Single.just(bucketDocumentId))
        whenever(bucket.put(bucketDocumentId, document)).doReturn(Completable.complete())
        whenever(journal.insert(documentId, version, SAVE)).doReturn(Single.just(changeId()))

        val observer = replicationPeer.put(documentId, version, SAVE, document).test()
        observer.await()

        observer.assertResult()
        verify(bucket).put(bucketDocumentId, document)
        verify(journal).insert(documentId, version, SAVE)
    }

    @Test
    fun replicationPeerPutDocumentIgnoresWhenJournalEntryExists() {
        val documentId = documentId()
        val version = version()
        val document = document()
        whenever(journal.exists(documentId, version)).doReturn(Single.just(true))

        val observer = replicationPeer.put(documentId, version, SAVE, document).test()
        observer.await()

        observer.assertResult()
    }

    @Test
    fun replicationPeerPutDocumentFailsWhenDocumentIsMissing() {
        val documentId = documentId()
        val version = version()
        whenever(journal.exists(documentId, version)).doReturn(Single.just(false))

        val observer = replicationPeer.put(documentId, version, SAVE, null).test()
        observer.await()

        observer.assertError(java.lang.IllegalStateException::class.java)
    }

    @Test
    fun replicationPeerPutDocumentFailsWhenDocumentIsPresent() {
        val documentId = documentId()
        val version = version()
        whenever(journal.exists(documentId, version)).doReturn(Single.just(false))

        val observer = replicationPeer.put(documentId, version, DELETE, document()).test()
        observer.await()

        observer.assertError(java.lang.IllegalStateException::class.java)
    }

    @Test
    fun replicationPeerPutsDocumentDelete() {
        val documentId = documentId()
        val version = version()
        whenever(journal.exists(documentId, version)).doReturn(Single.just(false))
        whenever(journal.insert(documentId, version, DELETE)).doReturn(Single.just(changeId()))

        val observer = replicationPeer.put(documentId, version, DELETE).test()
        observer.await()

        observer.assertResult()
        verify(journal).insert(documentId, version, DELETE)
    }

    @Test
    fun replicationPeerPutDocumentFailsDuringBucketPutPreventsFromJournalModification() {
        val documentId = documentId()
        val version = version()
        val bucketDocumentId = bucketDocumentId()
        val document = document()
        val error = IllegalStateException()
        whenever(journal.exists(documentId, version)).doReturn(Single.just(false))
        whenever(bucketDocumentIdFactory.create(documentId, version)).doReturn(Single.just(bucketDocumentId))
        whenever(bucket.put(bucketDocumentId, document)).doReturn(Completable.error(error))

        val observer = replicationPeer.put(documentId, version, SAVE, document).test()
        observer.await()

        observer.assertError(error)
        verify(bucket).put(bucketDocumentId, document)
        verify(journal, times(0)).insert(documentId, version, SAVE)
    }

    @Test
    fun replicationPeerDelegatesChanges() {
        val changeId = changeId()
        whenever(journal.changes(changeId)).doReturn(Observable.empty())

        val observer = replicationPeer.changes(latestSeen = changeId).test()
        observer.await()

        observer.assertResult()
        verify(journal).changes(changeId)
    }
}