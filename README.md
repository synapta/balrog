# Balrog

I'm still very experimental!!!

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
