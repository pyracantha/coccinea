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

import com.github.pyracantha.coccinea.journal.Action.DELETE
import com.github.pyracantha.coccinea.journal.Action.SAVE
import com.github.pyracantha.coccinea.journal.Change
import com.github.pyracantha.coccinea.journal.ChangeId
import com.github.pyracantha.coccinea.replication.ReplicationDirection.INBOUND
import com.github.pyracantha.coccinea.replication.ReplicationDirection.OUTBOUND
import io.reactivex.Completable
import io.reactivex.Maybe
import io.reactivex.Observable
import io.reactivex.Scheduler
import io.reactivex.Single

class ReplicatorImpl constructor(
    private val transferLog: TransferLog,
    private val scheduler: Scheduler
) : Replicator {

    override fun replicate(source: ReplicationPeer, destination: ReplicationPeer): Observable<ReplicationEvent> =
        replicateTo(source, destination, OUTBOUND)
            .ignoreElements()
            .andThen(replicateTo(destination, source, INBOUND))
            .subscribeOn(scheduler)

    private fun replicateTo(source: ReplicationPeer, destination: ReplicationPeer, direction: ReplicationDirection): Observable<ReplicationEvent> {
        return getFromTransferLog(source, destination, direction)
            .flatMapObservable { transferChanges(source, destination, direction, it) }
    }

    private fun transferChanges(source: ReplicationPeer, destination: ReplicationPeer, direction: ReplicationDirection, lastTransferredChange: TransferLogResult): Observable<ReplicationEvent> =
        Observable.defer {
            source.changes(lastTransferredChange.changeId)
                .concatMapMaybe { change ->
                    transferData(source, destination, direction, change)
                        .flatMap { Maybe.just(ReplicationEvent(change.documentId, change.action)) }
                }
        }

    private fun transferData(source: ReplicationPeer, destination: ReplicationPeer, direction: ReplicationDirection, change: Change): Maybe<ChangeId> =
        when (change.action) {
            DELETE -> {
                destination.put(change.documentId, change.version, change.action)
                    .andThen(addToTransferLog(source, destination, direction, change.changeId))
                    .andThen(Maybe.just(change.changeId))
            }
            SAVE -> {
                destination.exists(change.documentId, change.version)
                    .flatMapMaybe { exists ->
                        when (exists) {
                            false -> source.get(change.documentId, change.version).map { change to it }
                                .flatMap { (change, document) ->
                                    destination.put(change.documentId, change.version, change.action, document)
                                        .andThen(addToTransferLog(source, destination, direction, change.changeId))
                                        .andThen(Maybe.just(change.changeId))
                                }
                            true -> {
                                addToTransferLog(source, destination, direction, change.changeId)
                                    .andThen(Maybe.empty())
                            }
                        }
                    }
            }
        }

    private fun getFromTransferLog(source: ReplicationPeer, destination: ReplicationPeer, direction: ReplicationDirection): Single<TransferLogResult> =
        when (direction) {
            INBOUND -> source.databaseId()
            OUTBOUND -> destination.databaseId()
        }.flatMap {
            transferLog.get(it, direction)
                .map { changeId -> TransferLogResult(changeId) }
                .switchIfEmpty(Single.just(TransferLogResult()))
        }

    private fun addToTransferLog(source: ReplicationPeer, destination: ReplicationPeer, direction: ReplicationDirection, changeId: ChangeId): Completable =
        when (direction) {
            INBOUND -> source.databaseId()
            OUTBOUND -> destination.databaseId()
        }.flatMapCompletable { transferLog.put(it, direction, changeId) }

    internal data class TransferLogResult(
        val changeId: ChangeId? = null
    )
}