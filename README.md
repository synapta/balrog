# Balrog

I'm still very experimental!!!

## Known bugs
- Only bind with `?key`
- Only 2 endpoint
- Must use `SELECT` inside service
- No `*`
- No others operands in the first level except for SERVICE
- No operands in the most external `SELECT` except `COUNT`
- Returns always literal (even if URI)
- Crash if one of the prefixes is missing

## Example working query
```
PREFIX dcterms: <http://purl.org/dc/terms/>
PREFIX pc: <http://purl.org/procurement/public-contracts#>
PREFIX ter: <http://datiopen.istat.it/odi/ontologia/territorio/>
PREFIX owl: <http://www.w3.org/2002/07/owl#>

SELECT ?contract ?super
WHERE {
  SERVICE <https://contrattipubblici.org/sparql> {
    select ?contract ?key where {
      ?contract pc:contractingAutority ?pa.
      ?pa owl:sameAs ?ipa .
      ?ipa <http://www.geonames.org/ontology#locatedIn> ?o.
      ?o <http://spcdata.digitpa.gov.it/codice_catastale> ?key
    }
  }
  SERVICE <http://datiopen.istat.it/sparql/oracle> {
    SELECT ?super ?key
    WHERE {
      ?com ter:haSuperficie ?super .
      ?com ter:haCodCatastale ?key .
    }
  }
}
```
```
PREFIX dcterms: <http://purl.org/dc/terms/>
PREFIX pc: <http://purl.org/procurement/public-contracts#>
PREFIX ter: <http://datiopen.istat.it/odi/ontologia/territorio/>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX wd: <http://www.wikidata.org/entity/>
PREFIX wdt: <http://www.wikidata.org/prop/direct/>
prefix cobis: <http://dati.cobis.to.it/vocab/>
prefix bf: <http://bibframe.org/vocab/>
prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
PREFIX schema: <http://schema.org/>

SELECT ?member (count (*) as ?n)
WHERE {
  SERVICE <https://query.wikidata.org/sparql> {
    select ?member ?key where {
      ?author wdt:P214 ?key .
      ?author wdt:P463 ?member_ .
      ?author wdt:P569 ?birthDate .
      ?member_ rdfs:label ?member__ .
      FILTER (lang(?member__) = "it")
      #FILTER (?birthDate >= "1700"^^xsd:dateTime && ?birthDate <= "1800"^^xsd:dateTime)
      BIND(REPLACE(?member__, "Accademia", "A.") as ?member) .
    }
  }
  SERVICE <https://dati.cobis.to.it/sparql> {
    select distinct ?key where {
      ?inst a bf:Instance .
      VALUES ?p { schema:author schema:contributor}
      ?inst ?p ?agent .
      ?agent cobis:hasViafID ?key .
    }
    GROUP BY ?key
    HAVING (count (?inst) > 9)
  }
}
GROUP BY ?member
```
