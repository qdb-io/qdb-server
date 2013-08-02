qdb-server
==========

HTTP interface to qdb message queues. See http://qdb.io/ for full documentation.


Building
--------

QDB is built using Gradle (http://www.gradle.org/downloads). If you don't already have it just download, unzip
it somewhere and put the bin directory on your path. You need version 1.6 or newer.


Running From Source
-------------------

Use `gradle run` to start the server.

Use `gradle run-debug` to start the server in debug mode.


Running Tests
-------------

Use `gradle check` to run all tests (unit and functional).

Use `gradle test -Dtest.single=Queues*` to run matching test specs.

Use `gradle test -Dtest.single=Queues* -Dtest.debug=true` to run matching test specs with remote debugging.


License
-------

Copyright 2013 David Tinker

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
