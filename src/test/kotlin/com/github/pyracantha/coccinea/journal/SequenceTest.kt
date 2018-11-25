package com.github.pyracantha.coccinea.journal

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

internal class SequenceTest {

    @Test
    fun resultsInLessOrGreater() {
        val sequence1 = sequence(1)
        val sequence2 = sequence(2)

        assertThat(sequence1.compareTo(sequence2) == sequence2.compareTo(sequence1) * -1).isTrue()
        assertThat(sequence1 == sequence2).isFalse()
    }

    @Test
    fun resultsInEqual() {
        val sequence1 = sequence(1)
        val sequence2 = sequence(1)

        assertThat(sequence1.compareTo(sequence2) == sequence2.compareTo(sequence1)).isTrue()
        assertThat(sequence1 == sequence2).isTrue()
    }

    @Test
    fun ensuresTransitivity() {
        val sequence1 = sequence(1)
        val sequence2 = sequence(2)
        val sequence3 = sequence(3)

        assertThat(sequence1 < sequence2).isTrue()
        assertThat(sequence2 < sequence3).isTrue()
        assertThat(sequence1 < sequence3).isTrue()
    }
}