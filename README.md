# person-reconsiliation-service

Microservice reconsiliating duplicate person data based on a unique person identifier.

The service ensures there is a unique person, identifier and birthdate related to one RRN
across graphs. In the end each graph contains a copy of the 'master' record. The master record
is constructed out of the duplicate data. It is as complete as possible because it combines
all properties of the several slaves.

If a property has different values across slaves, the first one found is selected as value
for the master record. Note, this strategy may not be consistent across several runs on the same dataset.

A reference from the slave to the master record is kept using owl:sameAs.

## Installation

Add the following snippet in your `docker-compose.yml`:

```
version: '3.4'
services:
    person-reconsiliation:
        image: lblod/person-reconsiliation-service
        links:
          - database:database
```

## API

### POST /reconsiliate

Reconsiliate all duplicate RRNs in the database.

Optional query params:
* `dry-run` [boolean]: whether to run the execution in test mode, only calculating the master record, but not executing INSERT/DELETE queries


### POST /reconsiliate/:rrn

Reconsiliate the duplicates of a given RRN in the database.

Optional query params:
* `dry-run` [boolean]: whether to run the execution in test mode, only calculating the master record, but not executing INSERT/DELETE queries
