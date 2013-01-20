qdb-server
==========

HTTP interface to qdb message queues with easy clustering.


Usage
-----

The server follows REST principles and speaks JSON. It is easy to control using curl. The following example will list
all users:

    curl -s --user admin:admin http://127.0.0.1:9554/users

HTTP Basic authentication is used. The only endpoint that does not require authentication is the root of the server:

    curl -s http://127.0.0.1:9554/

This outputs:

    {
      "startTime": "2012-12-02T10:49:21.872+0000",
      "upSince": "2012-12-02T10:49:21.908+0000",
      "status": "UP"
    }

Authenticated users using this endpoint receive a lot more information about the server.

You can also POST and PUT json data to the server using curl. This example creates a new user:

    curl -s -X POST -H "Content-Type: application/json" --user admin:admin http://127.0.0.1:9554/users -d @user.json

It is important to specify the content type.

The file user.json should contain the user data. The new user is returned:

    {
      "admin": true,
      "id": "david"
    }


Endpoints
---------

List endpoints (e.g. /users) accept optional offset and limit parameters to page the returned data. PUT requests
only update fields included in the JSON i.e. you don't have to send a complete representation of the object. POST
and PUT request return a complete representation of the new/updated object.


### Users ###

`GET /users` List users

`POST /users` Create a user

`GET /users/abc` Get the user with id 'abc'

`PUT /users/abc` Update the user with id 'abc'

`GET /users/me` Get the authenticated user


### Databases ###

Databases form a namespace for queues and control access. TODO figure out access levels and so on.

`GET /databases` List databases that the authenticated user can see

`POST /databases` Create a database

    curl -s -X POST -H "Content-Type: application/json" --user admin:admin http://127.0.0.1:9554/databases -d @database.json

`GET /databases/foo` Get the database 'foo'

`PUT /databases/foo` Update the database 'foo'


### Queues ###

`GET /databases/foo/queues` List queues in database 'foo'

`POST /databases/foo/queues` Create a new queue in database 'foo'

`GET /databases/foo/queues/tweets` Get the queue 'tweets'

`PUT /databases/foo/queues/tweets` Update the queue 'tweets'


### Appending Messages ###

`POST /databases/foo/queues/tweets/messages?routingKey=abc:def` Append a message to the queue 'tweets' with an (optional)
routing key. The body of the POST is the message.

Note that the queue being appended to might not exist on this node. The message will be forwarded to the owning
node in that case and the 'Location' header will be set to the URL to post to on that node. You can ignore this or
POST to the suggested URL instead for better performance.

By default messages are posted synchronously and the ID of the message is returned. If async=true is specified in
the URL then the POST returns a 200 before the message has been written to the queue (and hence no ID is returned).
This results in better performance but it is possible (though unlikely) that something will go wrong and the message
will be lost.

TODO what about posting more than one message at once?


### Streaming Messages ###

`GET /databases/foo/queues/tweets/messages` Stream new messages from the queue 'tweets' as they are appended.
TODO figure out how to separate messages etc.

Specify `id=123456` to get messages from id onwards. Specify `timestamp=1354457617` (millisecond since 1/1/1970) or
`timestamp=2012-12-02T10:49:21.908+0000` (ISO 8601 format) to get messages from the given time onwards.


### Queue Timelines ###

`GET /databases/foo/queues/tweets/timeline` Get the high level timeline for the queue with id 'tweets'

`GET /databases/foo/queues/tweets/timeline/123456` Get the detailed timeline around the message with id 123456


Building
--------


Running From Source
-------------------

Use `gradle run` to run a non-clustered instance.

Use `gradle run -Pinstance=1` to run a clustered instance (use 2 and 3 for the other nodes in the cluster).


Running Tests
-------------

Use `gradle check` to run all tests (unit and functional).

Use `gradle test -Dtest.single=Paxos*` to run matching test specs (all the Paxos* specs in this example).


License
-------

Copyright 2012 David Tinker

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
