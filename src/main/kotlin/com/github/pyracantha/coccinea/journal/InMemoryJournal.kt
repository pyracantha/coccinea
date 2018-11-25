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

import com.github.pyracantha.coccinea.journal.Action.DELETE
import io.reactivex.Completable
import io.reactivex.Maybe
import io.reactivex.Observable
import io.reactivex.Scheduler
import io.reactivex.Single
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentMap
import java.util.concurrent.ConcurrentSkipListMap

class InMemoryJournal constructor(
    private val storage: ConcurrentMap<DocumentId, Set<Change>> = ConcurrentHashMap(),
    private val changeFactory: ChangeFactory,
    private val scheduler: Scheduler
) : Journal {

    private val changeIdIndex: ConcurrentMap<ChangeId, Sequence> = ConcurrentHashMap()
    private val sequenceIndex: ConcurrentSkipListMap<Sequence, Change> = ConcurrentSkipListMap()

    override fun insert(documentId: DocumentId, version: Version, action: Action): Single<ChangeId> =
        Single.defer {
            changeFactory.create(documentId, version, action)
                .flatMap { change ->
                    Completable.fromAction {
                        storage[documentId] = (storage[documentId] ?: setOf()) + change
                        sequenceIndex[change.sequence] = change
                        changeIdIndex[change.changeId] = change.sequence
                    }.andThen(Single.just(change.changeId))
                }.subscribeOn(scheduler)
        }

    override fun list(): Observable<DocumentId> =
        Observable.defer {
            Observable.fromIterable(storage.keys)
                .flatMapMaybe { latestChangeOf(it) }
                .filter { change -> change.action != DELETE }
                .map { change -> change.documentId }
                .subscribeOn(scheduler)
        }

    override fun exists(documentId: DocumentId, version: Version): Single<Boolean> =
        Single.defer {
            Single.fromCallable {
                storage[documentId]?.firstOrNull { it.version == version } != null
            }.subscribeOn(scheduler)
        }

    override fun latestChangeOf(documentId: DocumentId): Maybe<Change> =
        Maybe.defer {
            Maybe.fromCallable {
                storage[documentId]?.maxBy { it.version }
            }.subscribeOn(scheduler)
        }

    override fun changes(latestSeen: ChangeId?): Observable<Change> =
        Observable.defer {
            Observable.create<Change> { emitter ->
                if (latestSeen != null) {
                    val sequence = changeIdIndex[latestSeen]
                    if (sequence != null) {
                        sequenceIndex.navigableKeySet().tailSet(sequence, false)
                    } else {
                        emitter.onError(IllegalArgumentException("Unknown change id '${latestSeen.value}'"))
                        emptyList<Sequence>()
                    }
                } else {
                    sequenceIndex.keys
                }.map { emitter.onNext(sequenceIndex[it]!!) }
                emitter.onComplete()
            }.subscribeOn(scheduler)
        }
}