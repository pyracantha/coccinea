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

package com.github.pyracantha.coccinea.database

import com.github.pyracantha.coccinea.bucket.Bucket
import com.github.pyracantha.coccinea.bucket.BucketDocumentIdFactory
import com.github.pyracantha.coccinea.bucket.Document
import com.github.pyracantha.coccinea.journal.Action
import com.github.pyracantha.coccinea.journal.Action.DELETE
import com.github.pyracantha.coccinea.journal.Action.SAVE
import com.github.pyracantha.coccinea.journal.Change
import com.github.pyracantha.coccinea.journal.ChangeId
import com.github.pyracantha.coccinea.journal.DocumentId
import com.github.pyracantha.coccinea.journal.DocumentIdFactory
import com.github.pyracantha.coccinea.journal.Journal
import com.github.pyracantha.coccinea.journal.Version
import com.github.pyracantha.coccinea.journal.VersionFactory
import com.github.pyracantha.coccinea.replication.ReplicationPeer
import com.github.pyracantha.coccinea.replication.ReplicationEvent
import com.github.pyracantha.coccinea.replication.Replicator
import io.reactivex.Completable
import io.reactivex.Maybe
import io.reactivex.Observable
import io.reactivex.Single

class DatabaseImpl constructor(
    override val databaseId: DatabaseId,
    private val bucket: Bucket,
    private val journal: Journal,
    private val documentIdFactory: DocumentIdFactory,
    private val versionFactory: VersionFactory,
    private val bucketDocumentIdFactory: BucketDocumentIdFactory,
    private val replicator: Replicator
) : Database, ReplicationPeer {

    override fun get(documentId: DocumentId): Maybe<Document> =
        journal.latestChangeOf(documentId)
            .filter { it.action != DELETE }
            .flatMap { get(documentId, it.version) }

    override fun put(document: Document): Single<DocumentId> =
        documentIdFactory.create()
            .flatMap { documentId ->
                versionFactory.create().map { documentId to it }
            }
            .flatMap { (documentId, version) ->
                bucketDocumentIdFactory.create(documentId, version)
                    .map { Triple(it, documentId, version) }
            }
            .flatMap { (bucketDocumentId, documentId, version) ->
                bucket.put(bucketDocumentId, document)
                    .andThen(Single.just(documentId to version))
            }
            .flatMap { (documentId, version) ->
                journal.insert(documentId, version, SAVE)
                    .flatMap { (Single.just(documentId)) }
            }

    override fun put(documentId: DocumentId, document: Document): Completable =
        journal.latestChangeOf(documentId)
            .switchIfEmpty(Maybe.error(IllegalStateException("Unknown document: '$documentId'")))
            .flatMapSingle { change ->
                versionFactory.create(change.version).map { it }
            }
            .flatMap { version ->
                bucketDocumentIdFactory.create(documentId, version)
                    .map { it to version }
            }
            .flatMap { (bucketDocumentId, version) ->
                bucket.put(bucketDocumentId, document)
                    .andThen(Single.just(version))
            }
            .flatMapCompletable { version ->
                journal.insert(documentId, version, SAVE)
                    .ignoreElement()
            }

    override fun remove(documentId: DocumentId): Completable =
        journal.latestChangeOf(documentId)
            .switchIfEmpty(Maybe.error(IllegalStateException("Unknown document: '$documentId'")))
            .flatMapSingle { change ->
                versionFactory.create(change.version).map { it }
            }
            .flatMapCompletable {
                journal.insert(documentId, it, DELETE)
                    .ignoreElement()
            }

    override fun list(): Observable<DocumentId> =
        journal.list()

    override fun replicate(replicationPeer: ReplicationPeer): Observable<ReplicationEvent> =
        replicator.replicate(this, replicationPeer)

    override fun databaseId(): Single<DatabaseId> = Single.just(databaseId)

    override fun exists(documentId: DocumentId, version: Version): Single<Boolean> =
        journal.exists(documentId, version)

    override fun get(documentId: DocumentId, version: Version): Maybe<Document> =
        bucketDocumentIdFactory.create(documentId, version)
            .flatMapMaybe { bucket.get(it) }

    override fun put(documentId: DocumentId, version: Version, action: Action, document: Document?): Completable =
        exists(documentId, version)
            .flatMapCompletable { exists ->
                when (exists) {
                    false -> putToBucketAndJournal(documentId, version, action, document)
                    true -> Completable.complete()
                }
            }

    override fun changes(latestSeen: ChangeId?): Observable<Change> =
        journal.changes(latestSeen)

    private fun putToBucketAndJournal(documentId: DocumentId, version: Version, action: Action, document: Document?): Completable =
        when {
            action == SAVE && document != null ->
                bucketDocumentIdFactory.create(documentId, version)
                    .flatMapMaybe {
                        bucket.put(it, document)
                            .andThen(Maybe.just(Unit))
                    }
            action == DELETE && document == null -> Maybe.just(Unit)
            else -> Maybe.error(IllegalStateException("Invalid combination: action '$action' and document '$document'"))
        }.flatMapCompletable {
            journal.insert(documentId, version, action)
                .ignoreElement()
        }
}