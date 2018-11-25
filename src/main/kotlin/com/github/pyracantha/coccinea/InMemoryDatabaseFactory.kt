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

package com.github.pyracantha.coccinea

import com.github.pyracantha.coccinea.bucket.BucketDocumentIdFactoryImpl
import com.github.pyracantha.coccinea.bucket.InMemoryBucket
import com.github.pyracantha.coccinea.database.Database
import com.github.pyracantha.coccinea.database.DatabaseId
import com.github.pyracantha.coccinea.database.DatabaseImpl
import com.github.pyracantha.coccinea.journal.ChangeFactoryImpl
import com.github.pyracantha.coccinea.journal.DocumentIdFactoryImpl
import com.github.pyracantha.coccinea.journal.InMemoryJournal
import com.github.pyracantha.coccinea.journal.SequenceFactoryImpl
import com.github.pyracantha.coccinea.journal.VersionFactoryImpl
import com.github.pyracantha.coccinea.replication.ReplicatorImpl
import com.github.pyracantha.coccinea.replication.TransferLogImpl
import io.reactivex.Scheduler
import io.reactivex.schedulers.Schedulers
import java.util.UUID.randomUUID

class InMemoryDatabaseFactory {

    companion object {
        private val defaultScheduler = Schedulers.io()
        private val defaultDatabaseId: DatabaseId
            get() = DatabaseId(randomUUID().toString())

        fun create(
            databaseId: DatabaseId = defaultDatabaseId,
            scheduler: Scheduler = defaultScheduler
        ): Database =
            DatabaseImpl(
                databaseId = databaseId,
                bucket = InMemoryBucket(scheduler = scheduler),
                journal = InMemoryJournal(
                    changeFactory = ChangeFactoryImpl(
                        sequenceFactory = SequenceFactoryImpl(scheduler = scheduler),
                        scheduler = scheduler
                    ),
                    scheduler = scheduler
                ),
                documentIdFactory = DocumentIdFactoryImpl(scheduler = scheduler),
                versionFactory = VersionFactoryImpl(scheduler = scheduler),
                bucketDocumentIdFactory = BucketDocumentIdFactoryImpl(scheduler = scheduler),
                replicator = ReplicatorImpl(
                    TransferLogImpl(
                        scheduler = scheduler
                    ),
                    scheduler = scheduler
                )
            )
    }
}