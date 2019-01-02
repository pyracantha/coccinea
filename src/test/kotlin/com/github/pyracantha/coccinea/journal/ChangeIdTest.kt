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

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows

internal class ChangeIdTest {

    @Test
    fun changeIdValue() {
        val documentId = documentId()
        val version = version()

        val changeId = ChangeId(documentId, version)

        assertThat(changeId.value)
            .isEqualTo("${documentId.value}_${version.value}")
    }

    @Test
    fun parsesValidChangeIdValue() {
        val sequence = 1L
        val timestamp = 2L
        val documentIdString = "some-+some"
        val input = "${documentIdString}_$sequence+$timestamp"
        val documentId = documentId(documentIdString)
        val version = version(sequence, timestamp)

        val changeId = ChangeId.parse(input)

        assertThat(changeId.documentId)
            .isEqualTo(documentId)
        assertThat(changeId.version)
            .isEqualTo(version)
    }

    @Test
    fun rejectsInvalidChangeId() {
        val input = "abcd-2"

        assertThrows<IllegalArgumentException> { ChangeId.parse(input) }
    }
}