package com.github.pyracantha.coccinea.journal

import com.nhaarman.mockitokotlin2.doReturn
import com.nhaarman.mockitokotlin2.mock
import com.nhaarman.mockitokotlin2.verify
import com.nhaarman.mockitokotlin2.whenever
import io.reactivex.Single
import io.reactivex.schedulers.Schedulers.trampoline
import com.github.pyracantha.coccinea.journal.Action.DELETE
import com.github.pyracantha.coccinea.journal.Action.SAVE
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.concurrent.ConcurrentHashMap

internal class InMemoryJournalTest {

    private lateinit var storage: ConcurrentHashMap<DocumentId, Set<Change>>
    private lateinit var changeFactory: ChangeFactory

    private lateinit var journal: Journal

    @BeforeEach
    fun setUp() {
        storage = mock()
        changeFactory = mock()

        journal = InMemoryJournal(storage, changeFactory, trampoline())
    }

    @Test
    fun delegatesInsert() {
        val documentId = documentId()
        val version = version()
        val action = SAVE
        val change = change(documentId = documentId, version = version, action = action)
        whenever(changeFactory.create(documentId, version, action)).doReturn(Single.just(change))

        journal.insert(documentId, version, action).blockingGet()

        verify(storage)[documentId] = setOf(change)
        verify(changeFactory).create(documentId, version, action)
    }

    @Test
    fun insertReturnsChangeId() {
        val documentId = documentId()
        val version = version()
        val action = SAVE
        val change = change(documentId = documentId, version = version, action = action)
        whenever(changeFactory.create(documentId, version, action)).doReturn(Single.just(change))

        val result = journal.insert(documentId, version, action).blockingGet()

        assertThat(result)
                .isEqualTo(change.changeId)
    }

    @Test
    fun delegatesList() {
        val documentId = documentId()
        val change = change(version = version("1"))
        val keys = mock<ConcurrentHashMap.KeySetView<DocumentId, Set<Change>>> {
            on { iterator() } doReturn mutableSetOf(documentId).iterator()
        }
        whenever(storage.keys).doReturn(keys)
        whenever(storage[documentId]).doReturn(setOf(change))

        journal.list().test().await()

        verify(storage).keys
        verify(storage)[documentId]
    }

    @Test
    fun listLatestVersionSave() {
        val documentId = documentId()
        val changeVersion1 = change(documentId = documentId, version = version("1"), action = DELETE)
        val changeVersion2 = change(documentId = documentId, version = version("2"), action = SAVE)
        val keys = mock<ConcurrentHashMap.KeySetView<DocumentId, Set<Change>>> {
            on { iterator() } doReturn mutableSetOf(documentId).iterator()
        }
        whenever(storage.keys).doReturn(keys)
        whenever(storage[documentId]).doReturn(setOf(changeVersion1, changeVersion2))

        val result = journal.list().test().await()

        result.assertValue(documentId)
    }

    @Test
    fun listFiltersLatestVersionDelete() {
        val documentId = documentId()
        val changeVersion1 = change(documentId = documentId, version = version("1"), action = SAVE)
        val changeVersion2 = change(documentId = documentId, version = version("2"), action = DELETE)
        val keys = mock<ConcurrentHashMap.KeySetView<DocumentId, Set<Change>>> {
            on { iterator() } doReturn mutableSetOf(documentId).iterator()
        }
        whenever(storage.keys).doReturn(keys)
        whenever(storage[documentId]).doReturn(setOf(changeVersion1, changeVersion2))

        val result = journal.list().blockingIterable()

        assertThat(result)
                .hasSize(0)
    }

    @Test
    fun delegatesExists() {
        val documentId = documentId()
        val version = version()

        journal.exists(documentId, version).blockingGet()

        verify(storage)[documentId]
    }

    @Test
    fun existsVersionExists() {
        val documentId = documentId()
        val version = version("1")
        val change = change(version = version)
        whenever(storage[documentId]).doReturn(setOf(change))

        val result = journal.exists(documentId, version).blockingGet()

        assertThat(result)
                .isTrue()
    }

    @Test
    fun existsVersionDoesNotExist() {
        val documentId = documentId()
        val version = version("1")
        val change = change(version = version)
        whenever(storage[documentId]).doReturn(setOf(change))

        val result = journal.exists(documentId, version()).blockingGet()

        assertThat(result)
                .isFalse()
    }

    @Test
    fun delegatesLatestChangeOf() {
        val documentId = documentId()

        journal.latestChangeOf(documentId).blockingGet()

        verify(storage)[documentId]
    }

    @Test
    fun returnsLatestChangeOf() {
        val documentId = documentId()
        val changeVersion1 = change(documentId = documentId, version = version("1"))
        val changeVersion2 = change(documentId = documentId, version = version("2"))
        whenever(storage[documentId]).doReturn(setOf(changeVersion1, changeVersion2))

        val result = journal.latestChangeOf(documentId).blockingGet()

        assertThat(result)
                .isEqualTo(changeVersion2)
    }

    @Test
    fun failsOnUnknownChange() {
        val changeId = changeId()
        val observer = journal.changes(changeId).test()
        observer.await()

        observer.assertError(IllegalArgumentException::class.java)
    }

    @Test
    fun returnsAllChanges() {
        val change1 = change(sequence = sequence(1))
        val change2 = change(sequence = sequence(2))
        val journal = givenChanges(change1, change2)

        val observer = journal.changes().test()
        observer.await()

        observer.assertResult(change1, change2)
    }

    @Test
    fun returnsRemainingChanges() {
        val change1 = change(sequence = sequence(1))
        val change2 = change(sequence = sequence(2))
        val journal = givenChanges(change1, change2)

        val observer = journal.changes(change1.changeId).test()
        observer.await()

        observer.assertResult(change2)
    }

    @Test
    fun returnsNoRemainingChanges() {
        val change1 = change(sequence = sequence(1))
        val journal = givenChanges(change1)

        val observer = journal.changes(change1.changeId).test()
        observer.await()

        observer.assertResult()
    }

    private fun givenChanges(vararg changes: Change): Journal {
        val documentId = documentId()
        val version = version()
        val action = SAVE

        val journal = InMemoryJournal(ConcurrentHashMap(), changeFactory, trampoline())

        changes.forEach {
            whenever(changeFactory.create(documentId, version, action)).doReturn(Single.just(it))
            journal.insert(documentId, version, action).blockingGet()
        }

        return journal
    }
}