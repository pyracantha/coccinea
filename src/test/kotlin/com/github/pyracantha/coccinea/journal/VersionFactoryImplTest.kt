package com.github.pyracantha.coccinea.journal

import com.nhaarman.mockitokotlin2.doReturn
import com.nhaarman.mockitokotlin2.mock
import com.nhaarman.mockitokotlin2.whenever
import io.reactivex.schedulers.Schedulers.trampoline
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

internal class VersionFactoryImplTest {

    private lateinit var timestampGenerator: () -> Long

    private lateinit var versionFactory: VersionFactory

    @BeforeEach
    fun setUp() {
        timestampGenerator = mock()

        versionFactory = VersionFactoryImpl(timestampGenerator, trampoline())
    }

    @Test
    fun createsInitialVersion() {
        val timestamp = 123L
        val version = version(sequence = 1, timestamp = timestamp)
        whenever(timestampGenerator.invoke()).doReturn(timestamp)

        val observer = versionFactory.create().test()
        observer.await()

        observer.assertResult(version)
    }

    @Test
    fun createsNextVersion() {
        val timestamp = 123L
        val version = version(sequence = 1)
        val nextVersion = version(sequence = 2, timestamp = timestamp)
        whenever(timestampGenerator.invoke()).doReturn(timestamp)

        val observer = versionFactory.create(version).test()
        observer.await()

        observer.assertResult(nextVersion)
    }
}