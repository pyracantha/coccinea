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

package com.github.pyracantha.coccinea.bucket

import io.reactivex.Completable
import io.reactivex.Maybe
import io.reactivex.Observable
import io.reactivex.Scheduler
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentMap

class InMemoryBucket constructor(
    private val storage: ConcurrentMap<BucketDocumentId, Document> = ConcurrentHashMap(),
    private val scheduler: Scheduler
) : Bucket {

    override fun get(documentId: BucketDocumentId): Maybe<Document> =
        Maybe.defer {
            Maybe.fromCallable { storage[documentId] }
                .subscribeOn(scheduler)
        }

    override fun put(documentId: BucketDocumentId, document: Document): Completable =
        Completable.fromAction {
            storage[documentId] = document
        }.subscribeOn(scheduler)

    override fun remove(documentId: BucketDocumentId): Completable =
        Completable.defer {
            Completable.fromCallable {
                storage.remove(documentId)
            }.subscribeOn(scheduler)
        }

    override fun list(): Observable<BucketDocumentId> =
        Observable.defer {
            Observable.fromIterable(storage.keys)
                .subscribeOn(scheduler)
        }
}