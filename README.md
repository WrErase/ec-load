# ec-load

Clojure code written to import Eve Central CSV Dumps (http://eve-central.com/dumps) into a postgres database.
Basic validations are included, as well as history logging and generation of order statistics.

The main goal was to use [pmap](http://clojuredocs.org/clojure_core/clojure.core/pmap) to improve the performance
of the import and statistics generation code by running it across multiple cores.

My first attempt with Clojure, and still very much under construction (but working).

Written to import files into db for https://github.com/WrErase/eveyl

## Usage

### Load the csv dump
lein run [dump file]

### Build stats based on the order data
lein
  (build-stats)

## Postgres Tables

See pg_tables

## License
Copyright (c) 2013 Brad Harrell

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

## Credits
Brad Harrell: WrErase at gmail dot com
