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

import java.io.Serializable

data class ChangeId(
    val documentId: DocumentId,
    val version: Version
) : Serializable {
    companion object {
        private const val SEPARATOR = "_"
        private val regex = """[^$SEPARATOR]+$SEPARATOR[^$SEPARATOR]+""".toRegex()

        fun parse(input: String): ChangeId {
            if (regex.matches(input)) {
                val pair = input.split(SEPARATOR)
                return ChangeId(DocumentId(pair[0]), Version.parse(pair[1]))
            } else {
                throw IllegalArgumentException("Unsupported change id format '$input'")
            }
        }
    }

    val value = "${documentId.value}$SEPARATOR${version.value}"
}