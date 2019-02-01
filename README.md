# Wikipedia-to-blockchain project

This is a project which imports one of Wikimedia dump files (the ones containing whole articles, 
named like `enwiki-20190120-pages-articles-multistream.xml.bz2`) into a SQLite database. A newer
version of the dump can be used with the same SQLite database to update it with recent data.

During such updates, the differences between the existing (old) data and the new ones
can be stored into a new SQLite database. If applied repeatedly, this results in a growing
set of SQLite databases containing updates to Wikipedia pages, essentially they contain diffs
between the dumps. 

These databases may be imported into the [Daisy](https://github.com/ivoras/daisy) blockchain,
or used in any other suitable way.

The input files may be either decompressed XML files, or original bzip2-compressed XML files.
Note that in any case this is a time-consuming process.

## Usage:

To simply import (or update) a dump file:

```
./w2b --file simplewiki-20190101-pages-articles-multistream.xml.bz2 --db simplewiki.db
```

To update an existing database and generate a diff database:

```
./w2b --file simplewiki-20190120-pages-articles-multistream.xml.bz2 --db simplewiki.db --diff-db diff20190101.db
```

## Storage requirements

As a reference, the "Simple English" Wikipedia dump file is 155 MB in size, and it results in
a SQLite database which is nearly 600 GB.

## Licensing

This code is released under the MIT license.
