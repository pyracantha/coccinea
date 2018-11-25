package com.github.pyracantha.coccinea.journal

import com.nhaarman.mockitokotlin2.doReturn
import com.nhaarman.mockitokotlin2.mock
import com.nhaarman.mockitokotlin2.whenever
import io.reactivex.schedulers.Schedulers.trampoline
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

internal class VersionFactoryImplTest {

    private lateinit var randomIdGenerator: () -> Long

    private lateinit var versionFactory: VersionFactory

    @BeforeEach
    fun setUp() {
        randomIdGenerator = mock()

        versionFactory = VersionFactoryImpl(randomIdGenerator, trampoline())
    }

    @Test
    fun createsInitialVersion() {
        val randomId = 123L
        val version = version("1_$randomId")
        whenever(randomIdGenerator.invoke()).doReturn(randomId)

        val observer = versionFactory.create().test()
        observer.await()

        observer.assertResult(version)
    }

    @Test
    fun createsNextVersion() {
        val randomId = 123L
        val version = version("1_$randomId")
        val nextVersion = version("2_$randomId")
        whenever(randomIdGenerator.invoke()).doReturn(randomId)

        val observer = versionFactory.create(version).test()
        observer.await()

        observer.assertResult(nextVersion)
    }
}