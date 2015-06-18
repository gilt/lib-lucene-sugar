[![Build Status](https://travis-ci.org/gilt/lib-lucene-sugar.png)](https://travis-ci.org/gilt/lib-lucene-sugar)

# Lucene Sugar for Scala

Some sugar for your Lucene indexes.

## About

Lucene API is very verbose and designed around the semantic of the Java language. This library provides a more concise syntax for the Scala language that makes it easy to:

* Compose Lucene indexes using the familiar Scala cake pattern
* Add indexed and/or stored fields to a Lucene document
* Index collection of documents
* Search! (you didn't really expect that, do you?)

The basic idea of Lucene Sugar is to turn some operations on their head. Insead of

```scala
val doc = new Document
doc.add(new StringField("string_field", "aString", Store.YES))
doc.add(new LongField("long_field", 123456L, Store.NO))
doc.add(new StoredField("int_field", 10))
```

how about:

```scala
val doc = new Document
doc.addIndexedStoredField("string_field", "aString")
doc.addIndexedOnlyField("long_field", 123456L)
doc.addStoredOnlyField("int_field", 10)
```

### Disclaimer

It is possible that you will not like Lucene Sugar. That is perfectly fine! Some people like adding milk to their coffee, some add sugar. Some crazy ones don't even drink coffee, if you can imagine that... All I'm saying is that it's just a matter of taste and style.

### Contributions

Lucene Sugar started as a way to sweeten and shorten the code we needed to write to build and use Lucene indexes for a very specific project, but as we figured it could be helpful outside of Gilt we decided to open source it. 

## Requirements

* `sbt` >= 0.12.1

## Usage

Add the following dependency to `build.sbt`:

```scala
  "com.gilt" %% "lib-lucene-sugar" % "0.2.0"
```

## Dependencies

* Jsr305
* Google Guava
* Apache Lucene

## Examples

### Instantiate a memory based LuceneIndex with StandardAnalyzer

```scala
import com.gilt.lucene._

val index = new ReadableLuceneIndex
  with LuceneStandardAnalyzer
  with RamLuceneDirectory
```

### Instantiate a filesystem based Lucene with StandardAnalyzer

```scala
import com.gilt.lucene._

val index = new ReadableLuceneIndex
  with LuceneStandardAnalyzer 
  with FSLuceneDirectory
  with ServiceRootLucenePathProvider
  with SimpleFSLuceneDirectoryCreator 
```

This will create a `SimpleFSDirectory` based index in the `index` sub-directory relative to the project runtime root.

Since this is a very common usage, the above can be shortened to:

```scala
import com.gilt.lucene._

val index = new ReadableLuceneIndex
  with LuceneStandardAnalyzer 
  with DefaultFSLuceneDirectory 
```

In case you prefer to use `MMapDirectory` instead of `SimpleFSDirectory` you just have to switch the `DirectoryCreator` component:

```scala
import com.gilt.lucene._

val index = new ReadableLuceneIndex
  with LuceneStandardAnalyzer 
  with FSLuceneDirectory
  with ServiceRootLucenePathProvider
  with MMapFSLuceneDirectoryCreator 
```

### Build a Lucene document

```scala
import org.apache.lucene.document.Document
import com.gilt.lucene.LuceneFieldHelpers._
import com.gilt.lucene.LuceneText._

val doc = new Document()
doc.addIndexedStoredField("string_field", "some_string")
doc.addIndexedStoredField("text_field", "some text".toLuceneText)
doc.addIndexedOnlyField("optional_int", Option(42))
doc.addStoredOnlyField("long_value", 12345678L)
```

The `LuceneFieldHelpers` object provides implicit wrappers that augment a Lucene Document with the following methods:

* `addIndexedStoredField`: adds a field that is both indexed and stored
* `addIndexedOnlyField`: adds a field that is indexed only 
* `addStoredOnlyField`: adds a field that is stored only

The above methods accept values of `String`, `Long`, `Int`, `LuceneIndex` and their optional counterparts `Option[String]`, `Option[Long]`, `Option[Int]` and `Option[LuceneText]`. When an optional is passed as value, the field will be added only if the optional is defined. 

The `LuceneText` type is just a wrapper around `String` to help Lucene differentiate between Lucene `StringField` and `TextField`. You can easily convert a `String` to `LuceneText` with `"string".toLuceneText`.

### Add and search a Lucene Document

```scala
import org.apache.lucene.document.Document
import com.gilt.lucene._
import com.gilt.lucene.LuceneFieldHelpers._

val index = new ReadableLuceneIndex
  with WritableLuceneIndex
  with LuceneStandardAnalyzer 
  with DefaultFSLuceneDirectory 

val doc = new Document
doc.addIndexedStoredField("aField", "aValue")

index.addDocument(doc)

val queryParser = index.queryParserForDefaultField("aField")
val query = queryParser.parse("aValue")
val results = index.searchTopDocuments(query, 1)
```

### Implicit conversion of custom objects

If you provide an implicit type class to convert an object to a Lucene Document, you can use the `addDocument` method to directly add the object to the index:

```scala
import org.apache.lucene.document.Document
import com.gilt.lucene._
import com.gilt.lucene.LuceneFieldHelpers._
import com.gilt.lucene.LuceneDocumentAdder._

object Example {

	case class Person(name: String)

	implicit object PersonLuceneDocumentLike extends LuceneDocumentLike[Person] {
		def toDocuments(person: Person): Iterable[Document] = {
			val doc = new Document
			doc.addIndexedStoredField("name", person.name)
			Seq(doc)
		}
	}
	
	val index = new ReadableLuceneIndex
  		with WritableLuceneIndex
  		with LuceneStandardAnalyzer 
  		with DefaultFSLuceneDirectory

	person = Person("John")

	index.addDocument(person)
}
```


## TODO

* Add more sugar for query API
* Increase test coverage
* Cover more Lucene API

## License

Copyright 2013 Gilt Groupe, Inc.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.

