package com.github.pyracantha.coccinea.scenario

import com.github.pyracantha.coccinea.bucket.document
import com.github.pyracantha.coccinea.InMemoryDatabaseFactory
import com.github.pyracantha.coccinea.database.Database
import com.github.pyracantha.coccinea.replication.ReplicableDatabase
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import com.github.pyracantha.coccinea.replication.replicationEvent

class ReplicationScenariosTest {

    private lateinit var source: Database
    private lateinit var destination: Database
    private lateinit var replicableSource: ReplicableDatabase
    private lateinit var replicableDestination: ReplicableDatabase

    @BeforeEach
    fun setUp() {
        source = InMemoryDatabaseFactory.create()
        destination = InMemoryDatabaseFactory.create()
        replicableSource = source as ReplicableDatabase
        replicableDestination = destination as ReplicableDatabase
    }

    @Test
    fun mergesTwoDatabasesTriggeredBySource() {
        val totalNumberOfDocuments = 4
        val sourceDocument1 = document()
        val sourceDocument2 = document()
        val destinationDocument1 = document()
        val destinationDocument2 = document()
        val sourceDocumentId1 = source.put(sourceDocument1).blockingGet()
        val sourceDocumentId2 = source.put(sourceDocument2).blockingGet()
        val destinationDocumentId1 = destination.put(destinationDocument1).blockingGet()
        val destinationDocumentId2 = destination.put(destinationDocument2).blockingGet()

        val observer = source.replicate(replicableDestination).test()
        observer.await()

        observer.assertResult(
                replicationEvent(documentId = destinationDocumentId1),
                replicationEvent(documentId = destinationDocumentId2)
        )
        assertThat(source.list().blockingIterable()).hasSize(totalNumberOfDocuments)
        assertThat(destination.list().blockingIterable()).hasSize(totalNumberOfDocuments)
        assertThat(source.get(sourceDocumentId1).blockingGet()).isEqualTo(sourceDocument1)
        assertThat(destination.get(sourceDocumentId1).blockingGet()).isEqualTo(sourceDocument1)
        assertThat(source.get(sourceDocumentId2).blockingGet()).isEqualTo(sourceDocument2)
        assertThat(destination.get(sourceDocumentId2).blockingGet()).isEqualTo(sourceDocument2)
        assertThat(source.get(destinationDocumentId1).blockingGet()).isEqualTo(destinationDocument1)
        assertThat(destination.get(destinationDocumentId1).blockingGet()).isEqualTo(destinationDocument1)
        assertThat(source.get(destinationDocumentId2).blockingGet()).isEqualTo(destinationDocument2)
        assertThat(destination.get(destinationDocumentId2).blockingGet()).isEqualTo(destinationDocument2)
    }

    @Test
    fun mergesTwoDatabasesTriggeredByDestination() {
        val totalNumberOfDocuments = 4
        val sourceDocument1 = document()
        val sourceDocument2 = document()
        val destinationDocument1 = document()
        val destinationDocument2 = document()
        val sourceDocumentId1 = source.put(sourceDocument1).blockingGet()
        val sourceDocumentId2 = source.put(sourceDocument2).blockingGet()
        val destinationDocumentId1 = destination.put(destinationDocument1).blockingGet()
        val destinationDocumentId2 = destination.put(destinationDocument2).blockingGet()

        val observer = destination.replicate(replicableSource).test()
        observer.await()

        observer.assertResult(
                replicationEvent(documentId = sourceDocumentId1),
                replicationEvent(documentId = sourceDocumentId2)
        )
        assertThat(source.list().blockingIterable()).hasSize(totalNumberOfDocuments)
        assertThat(destination.list().blockingIterable()).hasSize(totalNumberOfDocuments)
        assertThat(source.get(sourceDocumentId1).blockingGet()).isEqualTo(sourceDocument1)
        assertThat(destination.get(sourceDocumentId1).blockingGet()).isEqualTo(sourceDocument1)
        assertThat(source.get(sourceDocumentId2).blockingGet()).isEqualTo(sourceDocument2)
        assertThat(destination.get(sourceDocumentId2).blockingGet()).isEqualTo(sourceDocument2)
        assertThat(source.get(destinationDocumentId1).blockingGet()).isEqualTo(destinationDocument1)
        assertThat(destination.get(destinationDocumentId1).blockingGet()).isEqualTo(destinationDocument1)
        assertThat(source.get(destinationDocumentId2).blockingGet()).isEqualTo(destinationDocument2)
        assertThat(destination.get(destinationDocumentId2).blockingGet()).isEqualTo(destinationDocument2)
    }

    @Test
    fun resolvesConcurrentUpdatesByLatestTimestamp() {
        val totalNumberOfDocuments = 1
        val originalDocument = document()
        val updatedDocumentSource = document()
        val updatedDocumentDestination = document()
        val documentId = source.put(originalDocument).blockingGet()
        source.replicate(replicableDestination).test().await()

        source.put(documentId, updatedDocumentSource).blockingAwait()
        destination.put(documentId, updatedDocumentDestination).blockingAwait()
        val observer = source.replicate(replicableDestination).test()
        observer.await()

        observer.assertResult(replicationEvent(documentId = documentId))
        assertThat(source.list().blockingIterable()).hasSize(totalNumberOfDocuments)
        assertThat(destination.list().blockingIterable()).hasSize(totalNumberOfDocuments)
        assertThat(source.get(documentId).blockingGet()).isEqualTo(updatedDocumentDestination)
        assertThat(destination.get(documentId).blockingGet()).isEqualTo(updatedDocumentDestination)
    }

    @Test
    fun resolvesConcurrentUpdatesByMostChanges() {
        val totalNumberOfDocuments = 1
        val originalDocument = document()
        val updatedDocumentSource = document()
        val updatedDocumentDestination = document()
        val documentId = source.put(originalDocument).blockingGet()
        source.replicate(replicableDestination).test().await()

        source.put(documentId, updatedDocumentSource).blockingAwait()
        source.put(documentId, updatedDocumentSource).blockingAwait()
        destination.put(documentId, updatedDocumentDestination).blockingAwait()
        val observer = source.replicate(replicableDestination).test()
        observer.await()

        observer.assertResult(replicationEvent(documentId = documentId))
        assertThat(source.list().blockingIterable()).hasSize(totalNumberOfDocuments)
        assertThat(destination.list().blockingIterable()).hasSize(totalNumberOfDocuments)
        assertThat(source.get(documentId).blockingGet()).isEqualTo(updatedDocumentSource)
        assertThat(destination.get(documentId).blockingGet()).isEqualTo(updatedDocumentSource)
    }
}