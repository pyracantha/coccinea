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

package com.github.pyracantha.coccinea.replication

import com.github.pyracantha.coccinea.bucket.Document
import com.github.pyracantha.coccinea.database.DatabaseId
import com.github.pyracantha.coccinea.journal.Action
import com.github.pyracantha.coccinea.journal.Change
import com.github.pyracantha.coccinea.journal.ChangeId
import com.github.pyracantha.coccinea.journal.DocumentId
import com.github.pyracantha.coccinea.journal.Version
import io.reactivex.Completable
import io.reactivex.Maybe
import io.reactivex.Observable
import io.reactivex.Single

interface ReplicationPeer {
    fun databaseId(): Single<DatabaseId>
    fun exists(documentId: DocumentId, version: Version): Single<Boolean>
    fun get(documentId: DocumentId, version: Version): Maybe<Document>
    fun put(documentId: DocumentId, version: Version, action: Action, document: Document? = null): Completable
    fun changes(latestSeen: ChangeId? = null): Observable<Change>
}