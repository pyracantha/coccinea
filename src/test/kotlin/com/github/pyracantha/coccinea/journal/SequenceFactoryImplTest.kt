package com.github.pyracantha.coccinea.journal

import io.reactivex.schedulers.Schedulers.trampoline
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

internal class SequenceFactoryImplTest {

    private lateinit var sequenceFactory: SequenceFactory

    @BeforeEach
    fun setUp() {
        sequenceFactory = SequenceFactoryImpl(trampoline())
    }

    @Test
    fun returnsSequences() {
        val observer1 = sequenceFactory.nextSequence().test()
        observer1.await()
        val observer2 = sequenceFactory.nextSequence().test()
        observer2.await()

        observer1.assertResult(sequence(1))
        observer2.assertResult(sequence(2))
    }
}