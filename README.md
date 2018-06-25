# Balrog

Performing federated SPARQL queries is not always easy or doable. Public SPARQL end-points may not be properly configured, or they may time out too quickly for any meaningful federated query to be completed.
Balrog aims at solving these issues using a local PostgreSQL installation to federate the result sets of two different end-points.
(In some use cases, we even broke up an ordinary query into two queries for Balrog, in order to quickly work around some time-out issues on the public end-points of big triple stores, such as the one of Wikidata.)

## Installation
You need Postgres 9+ already installed with user `postgres`, password `postgres`
for a database called `postgres`.
```
npm install
```

## Usage
```
npm start
```

## Known bugs
- Only bind with `?key`
- Only 2 endpoint
- No others operands in the first level except for SERVICE
- No operands in the most external `SELECT` except `COUNT`
- Must use `SELECT` inside service
- No `*`
- An `OPTIONAL` without at least one value makes the code crash

## Feature request
- Better error comunication to the user
- Automatically pagination (Virtuoso endpoints have limits e.g. 10k lines)
- Smart `VALUES` usage for reduce complex queries and avoid timeouts
- Don't use fs for passing CSV

## Example working query
You can find some example of currently working federate SPARQL queries in the
`examples` directory.
