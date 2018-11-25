package com.github.pyracantha.coccinea.journal

import com.nhaarman.mockitokotlin2.doReturn
import com.nhaarman.mockitokotlin2.mock
import com.nhaarman.mockitokotlin2.whenever
import io.reactivex.Single
import io.reactivex.schedulers.Schedulers.trampoline
import com.github.pyracantha.coccinea.journal.Action.SAVE
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

internal class ChangeFactoryImplTest {

    private lateinit var sequenceFactory: SequenceFactory

    private lateinit var changeFactory: ChangeFactory

    @BeforeEach
    fun setUp() {
        sequenceFactory = mock()

        changeFactory = ChangeFactoryImpl(sequenceFactory, trampoline())
    }

    @Test
    fun createsChange() {
        val sequence = sequence()
        val documentId = documentId()
        val version = version()
        val action = SAVE
        val change = change(
                documentId = documentId,
                version = version,
                action = action,
                sequence = sequence
        )
        whenever(sequenceFactory.nextSequence()).doReturn(Single.just(sequence))

        val observer = changeFactory.create(documentId, version, action).test()
        observer.await()

        observer.assertResult(change)
    }
}