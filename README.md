# Wikipedia-to-blockchain project

This is a project which imports one of Wikimedia dump files (the ones containing whole articles, 
named like `enwiki-20190120-pages-articles-multistream.xml.bz2`) into a SQLite database. This
database can be used again for importing new data, which will result in updates to the updated
records. During such updates, the differences between the existing (old) data and the new ones
can be stored into a new SQLite database. If applied repeatedly, this results in a growing
sets of SQLite databases containing updates to Wikipedia pages, essentially they contain diffs
between the dumps. 

These databases may be imported into the [Daisy](https://github.com/ivoras/daisy) blockchain,
or used in any other suitable way.

This code is released under the MIT license.
