# Balrog

I'm still very experimental!!!

## Known bugs
- Only bind with `?key`
- Only 2 endpoint
- Must use `SELECT` inside service
- No `*`
- No others operands in the first level except for SERVICE
- No operands in the most external `SELECT`
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
