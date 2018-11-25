package com.github.pyracantha.coccinea.replication

import com.nhaarman.mockitokotlin2.doReturn
import com.nhaarman.mockitokotlin2.mock
import com.nhaarman.mockitokotlin2.verify
import com.nhaarman.mockitokotlin2.whenever
import com.github.pyracantha.coccinea.database.DatabaseId
import com.github.pyracantha.coccinea.database.databaseId
import io.reactivex.schedulers.Schedulers.trampoline
import com.github.pyracantha.coccinea.journal.changeId
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import com.github.pyracantha.coccinea.replication.ReplicationDirection.OUTBOUND
import java.util.concurrent.ConcurrentMap

internal class TransferLogImplTest {

    private lateinit var storage: ConcurrentMap<Pair<DatabaseId, ReplicationDirection>, TransferLogEntry>

    private lateinit var transferLog: TransferLog

    @BeforeEach
    fun setUp() {
        storage = mock()

        transferLog = TransferLogImpl(storage, trampoline())
    }

    @Test
    fun delegatesPut() {
        val databaseId = databaseId()
        val direction = OUTBOUND
        val changeId = changeId()

        val observer = transferLog.put(databaseId, direction, changeId).test()
        observer.await()

        observer.assertResult()
        verify(storage)[databaseId to direction] = TransferLogEntry(changeId)
    }

    @Test
    fun delegatesGet() {
        val databaseId = databaseId()
        val direction = OUTBOUND
        val changeId = changeId()
        whenever(storage[databaseId to direction]).doReturn(transferLogEntry(changeId))

        val observer = transferLog.get(databaseId, direction).test()
        observer.await()

        observer.assertResult(changeId)
    }
}