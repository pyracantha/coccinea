/*
 * Copyright (c) 2018 github.com/pyracantha
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package com.github.pyracantha.coccinea.journal

import io.reactivex.Scheduler
import io.reactivex.Single

class VersionFactoryImpl constructor(
    private val randomIdGenerator: () -> Long = { System.nanoTime() },
    private val scheduler: Scheduler
) : VersionFactory {
    override fun create(): Single<Version> =
        Single.just(toVersion(1))
            .observeOn(scheduler)

    override fun create(version: Version): Single<Version> =
        Single.just(nextVersion(version))
            .observeOn(scheduler)

    private fun nextVersion(version: Version): Version {
        val pair = version.value.split("_")

        val sequence = pair[0].toLong().plus(1)

        return toVersion(sequence)
    }

    private fun toVersion(sequence: Long) =
        Version("${sequence}_${randomIdGenerator()}")
}