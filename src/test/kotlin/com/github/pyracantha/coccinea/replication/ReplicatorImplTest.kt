package com.github.pyracantha.coccinea.replication

import com.github.pyracantha.coccinea.bucket.Document
import com.github.pyracantha.coccinea.bucket.document
import com.github.pyracantha.coccinea.database.DatabaseId
import com.github.pyracantha.coccinea.database.databaseId
import com.github.pyracantha.coccinea.journal.Action
import com.github.pyracantha.coccinea.journal.Action.DELETE
import com.github.pyracantha.coccinea.journal.Action.SAVE
import com.github.pyracantha.coccinea.journal.Change
import com.github.pyracantha.coccinea.journal.ChangeId
import com.github.pyracantha.coccinea.journal.change
import com.github.pyracantha.coccinea.journal.changeId
import com.github.pyracantha.coccinea.replication.ReplicationDirection.INBOUND
import com.github.pyracantha.coccinea.replication.ReplicationDirection.OUTBOUND
import com.nhaarman.mockitokotlin2.any
import com.nhaarman.mockitokotlin2.anyOrNull
import com.nhaarman.mockitokotlin2.doReturn
import com.nhaarman.mockitokotlin2.mock
import com.nhaarman.mockitokotlin2.times
import com.nhaarman.mockitokotlin2.verify
import com.nhaarman.mockitokotlin2.whenever
import io.reactivex.Completable
import io.reactivex.Maybe
import io.reactivex.Observable
import io.reactivex.Single
import io.reactivex.schedulers.Schedulers.trampoline
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

internal class ReplicatorImplTest {

    companion object {
        private val databaseIdSource = databaseId("source")
        private val databaseIdDestination = databaseId("destination")
    }

    private lateinit var transferLog: TransferLog
    private lateinit var source: ReplicableDatabase
    private lateinit var destination: ReplicableDatabase

    private lateinit var replicator: Replicator

    @BeforeEach
    fun setUp() {
        transferLog = mock()
        source = mock {
            on { databaseId() } doReturn Single.just(databaseIdSource)
        }
        destination = mock {
            on { databaseId() } doReturn Single.just(databaseIdDestination)
        }
        replicator = ReplicatorImpl(transferLog, trampoline())
    }

    @Test
    fun delegatesReplicate() {
        whenever(source.changes()).doReturn(Observable.empty())
        whenever(destination.changes()).doReturn(Observable.empty())
        whenever(transferLog.get(any(), any())).doReturn(Maybe.empty())

        val observer = replicator.replicate(source, destination).test()
        observer.await()

        observer.assertResult()
        verify(source).changes()
        verify(destination).changes()
        verify(source).changes()
        verify(transferLog, times(2)).get(any(), any())
    }

    @Test
    fun transfersOutbound() {
        val change1 = change()
        val change2 = change()
        val change3 = change()
        val document1 = document()
        val document2 = document()
        val document3 = document()
        whenever(source.changes()).doReturn(Observable.fromArray(change1, change2, change3))
        whenever(source.get(change1.documentId, change1.version)).doReturn(Maybe.just(document1))
        whenever(source.get(change2.documentId, change2.version)).doReturn(Maybe.just(document2))
        whenever(source.get(change3.documentId, change3.version)).doReturn(Maybe.just(document3))
        whenever(destination.changes()).doReturn(Observable.empty())
        whenever(destination.exists(any(), any())).doReturn(Single.just(false))
        whenever(destination.put(any(), any(), any(), any())).doReturn(Completable.complete())
        whenever(transferLog.get(any(), any())).doReturn(Maybe.empty())
        whenever(transferLog.put(any(), any(), any())).doReturn(Completable.complete())

        val observer = replicator.replicate(source, destination).test()
        observer.await()

        observer.assertResult()

        verifyExists(destination, change1)
        verifyExists(destination, change2)
        verifyExists(destination, change3)
        verifyPut(destination, change1, SAVE, document1)
        verifyPut(destination, change2, SAVE, document2)
        verifyPut(destination, change3, SAVE, document3)
        verifyPutTransferLog(transferLog, databaseIdDestination, OUTBOUND, change1.changeId)
        verifyPutTransferLog(transferLog, databaseIdDestination, OUTBOUND, change2.changeId)
        verifyPutTransferLog(transferLog, databaseIdDestination, OUTBOUND, change3.changeId)
    }

    @Test
    fun transfersOutboundDelete() {
        val change1 = change(action = DELETE)
        val document1 = document()
        whenever(source.changes()).doReturn(Observable.just(change1))
        whenever(source.get(change1.documentId, change1.version)).doReturn(Maybe.just(document1))
        whenever(destination.changes()).doReturn(Observable.empty())
        whenever(destination.exists(any(), any())).doReturn(Single.just(false))
        whenever(destination.put(any(), any(), any(), anyOrNull())).doReturn(Completable.complete())
        whenever(transferLog.get(any(), any())).doReturn(Maybe.empty())
        whenever(transferLog.put(any(), any(), any())).doReturn(Completable.complete())

        val observer = replicator.replicate(source, destination).test()
        observer.await()

        observer.assertResult()

        verifyPut(destination, change1, DELETE)
        verifyPutTransferLog(transferLog, databaseIdDestination, OUTBOUND, change1.changeId)
    }

    @Test
    fun transfersInbound() {
        val change1 = change()
        val change2 = change()
        val change3 = change()
        val document1 = document()
        val document2 = document()
        val document3 = document()
        whenever(destination.changes()).doReturn(Observable.fromArray(change1, change2, change3))
        whenever(destination.get(change1.documentId, change1.version)).doReturn(Maybe.just(document1))
        whenever(destination.get(change2.documentId, change2.version)).doReturn(Maybe.just(document2))
        whenever(destination.get(change3.documentId, change3.version)).doReturn(Maybe.just(document3))
        whenever(source.changes()).doReturn(Observable.empty())
        whenever(source.exists(any(), any())).doReturn(Single.just(false))
        whenever(source.put(any(), any(), any(), any())).doReturn(Completable.complete())
        whenever(transferLog.get(any(), any())).doReturn(Maybe.empty())
        whenever(transferLog.put(any(), any(), any())).doReturn(Completable.complete())

        val observer = replicator.replicate(source, destination).test()
        observer.await()

        observer.assertResult(
            replicationEvent(change1.documentId, SAVE),
            replicationEvent(change2.documentId, SAVE),
            replicationEvent(change3.documentId, SAVE)
        )

        verifyExists(source, change1)
        verifyExists(source, change2)
        verifyExists(source, change3)
        verifyPut(source, change1, SAVE, document1)
        verifyPut(source, change2, SAVE, document2)
        verifyPut(source, change3, SAVE, document3)
        verifyPutTransferLog(transferLog, databaseIdDestination, INBOUND, change1.changeId)
        verifyPutTransferLog(transferLog, databaseIdDestination, INBOUND, change2.changeId)
        verifyPutTransferLog(transferLog, databaseIdDestination, INBOUND, change3.changeId)
    }

    @Test
    fun skipsExistingDocuments() {
        val change1 = change()
        val change2 = change()
        val document = document()
        whenever(source.changes()).doReturn(Observable.fromArray(change1, change2))
        whenever(destination.changes()).doReturn(Observable.empty())
        whenever(destination.exists(change1.documentId, change1.version)).doReturn(Single.just(true))
        whenever(destination.exists(change2.documentId, change2.version)).doReturn(Single.just(false))
        whenever(source.get(change2.documentId, change2.version)).doReturn(Maybe.just(document))
        whenever(destination.put(any(), any(), any(), any())).doReturn(Completable.complete())
        whenever(transferLog.get(any(), any())).doReturn(Maybe.empty())
        whenever(transferLog.put(any(), any(), any())).doReturn(Completable.complete())

        val observer = replicator.replicate(source, destination).test()
        observer.await()

        observer.assertResult()

        verifyPut(destination, change2, SAVE, document)
        verifyPutTransferLog(transferLog, databaseIdDestination, OUTBOUND, change2.changeId)
    }

    @Test
    fun skipsKnownChanges() {
        val changeId = changeId()
        val change = change()
        val document = document()
        whenever(source.changes(changeId)).doReturn(Observable.just(change))
        whenever(destination.changes()).doReturn(Observable.empty())
        whenever(destination.exists(any(), any())).doReturn(Single.just(false))
        whenever(source.get(change.documentId, change.version)).doReturn(Maybe.just(document))
        whenever(destination.put(any(), any(), any(), any())).doReturn(Completable.complete())
        whenever(transferLog.get(databaseIdDestination, OUTBOUND)).doReturn(Maybe.just(changeId))
        whenever(transferLog.get(databaseIdDestination, INBOUND)).doReturn(Maybe.empty())
        whenever(transferLog.put(any(), any(), any())).doReturn(Completable.complete())

        val observer = replicator.replicate(source, destination).test()
        observer.await()

        observer.assertResult()

        verifyPut(destination, change, SAVE, document)
        verifyPutTransferLog(transferLog, databaseIdDestination, OUTBOUND, change.changeId)
    }

    private fun verifyExists(database: ReplicableDatabase, change: Change) =
        verify(database).exists(change.documentId, change.version)

    private fun verifyPut(database: ReplicableDatabase, change: Change, action: Action, document: Document? = null) =
        verify(database).put(change.documentId, change.version, action, document)

    private fun verifyPutTransferLog(transferLog: TransferLog, databaseId: DatabaseId, direction: ReplicationDirection, changeId: ChangeId) =
        verify(transferLog).put(databaseId, direction, changeId)
}