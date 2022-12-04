import json
import requests

from elasticsearch import Elasticsearch


def main():
    from datetime import datetime
    inicio = datetime.now()

    # Password para el usuario 'lectura'
    READONLY_PASSWORD = "abretesesamo"

    # cliente elastic
    global es

    es = Elasticsearch(
        "https://localhost:9200",
        ca_certs="./http_ca.crt",
        basic_auth=("lectura", READONLY_PASSWORD)
    )

    # consulta para encontrar los significant terms
    significantTermsJson = es.search(
        index="tweets-20090624-20090626-en_es-10percent",
        body={
            "size": 0,
            "query": {
                "query_string": {
                    "query": "text:\"rip\" AND lang:en"
                }
            },
            "aggs": {
                "Most significant terms": {
                    "significant_terms": {
                        "field": "text",
                        "size": 7
                    }
                }
            }
        }
    )

    # saco las palabras del json de la respuesta
    sigTerms = []
    for terms in significantTermsJson["aggregations"]["Most significant terms"]["buckets"]:
        sigTerms.append(terms["key"])

    # construyo la query
    query = "text:\"rip\" AND "
    for i in range (0, len(sigTerms)):
        if(i==len(sigTerms)-1):
            query += "text:\""+sigTerms[i]+"\""
        else:
            query += "text:\""+sigTerms[i]+"\" AND "

    # hago la consulta expandida
    expandida = es.search(
        index="tweets-20090624-20090626-en_es-10percent",
        body={
            "size": 0,
            "query": {
                "query_string": {
                    "query": query
                }
            }
        }
    )

    fin = datetime.now()
    print(fin-inicio)


if __name__ == '__main__':
    main()
