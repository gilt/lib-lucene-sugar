package com.gilt.lucene

import javax.annotation.Nonnull

import org.apache.lucene.document.Document
import org.apache.lucene.queryparser.classic.QueryParser
import org.apache.lucene.search.{Query, ScoreDoc}

/**
 * Base trait for simple Lucene indexes
 * The index gets built once at construction
 */
trait ReadableLuceneIndex extends LuceneVersion { self: LuceneDirectory with LuceneAnalyzerProvider =>

  /**
   * Returns a new QueryParser that defaults to the provided field
   */
  @Nonnull
  def queryParserForDefaultField(@Nonnull field: String) = new QueryParser(field, luceneAnalyzer)

  /**
   * Process a Lucene query string and returns the resulting documents
   */
  @Nonnull
  def searchTopDocuments(@Nonnull query: Query, @Nonnull limit: Int): Iterable[Document] = withIndexSearcher { indexSearcherOption =>
    indexSearcherOption.map { indexSearcher =>
      val hits = indexSearcher.search(query, limit)

      hits.scoreDocs.map { hit =>
        indexSearcher.doc(hit.doc)
      }.toIterable

    }.getOrElse(Iterable.empty)
  }

  /**
   * Process a Lucene query string and returns all resulting documents
   */
  @Nonnull
  def searchAllTopDocuments(@Nonnull query: Query, @Nonnull limit: Int): Iterable[Document] = withIndexSearcher { indexSearcherOption =>
    indexSearcherOption.map { indexSearcher =>
      val hits = indexSearcher.search(query, limit)
      def getAllScoreDocs(scoreDocs: Iterable[ScoreDoc]): Iterable[ScoreDoc] = {
        if (scoreDocs.size == hits.totalHits) scoreDocs
        else getAllScoreDocs(
          scoreDocs ++ indexSearcher.searchAfter(scoreDocs.last, query, limit).scoreDocs
        )
      }
      getAllScoreDocs(hits.scoreDocs).map { hit =>
        indexSearcher.doc(hit.doc)
      }

    }.getOrElse(Iterable.empty)
  }

  /**
   * Returns a collection of all documents contained in the index
   */
  @Nonnull
  def allDocuments: Iterable[Document] = withDirectoryReader { directoryReaderOption =>
    directoryReaderOption.map { directoryReader =>
      (0 until directoryReader.numDocs).map(directoryReader.document)
    }.getOrElse(Iterable.empty)
  }

}
