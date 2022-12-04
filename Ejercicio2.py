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
        index="tweets-20090624-20090626-en_es-10percent-ejercicio2",
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
                        "size": 5
                    }
                }
            }
        }
    )

    # saco las palabras del json de la respuesta
    sigTerms = []
    for terms in significantTermsJson["aggregations"]["Most significant terms"]["buckets"]:
        sigTerms.append(terms["key"])

    # construyo la nueva query empezando por el idioma y el termino original 
    query = "lang:en AND (text:rip OR "
    for i in range (0, len(sigTerms)):
        if(i==len(sigTerms)-1):
            query += "text:"+sigTerms[i]+""
        else:
            query += "text:"+sigTerms[i]+" OR "
    query += ")"
    print(query)

    # hago la consulta expandida
    expandida = es.search(
        index="tweets-20090624-20090626-en_es-10percent-ejercicio2",
        body={
            "track_total_hits": "true",
            "query": {
                "query_string": {
                    "query": query
                }
            }
        }
    )

    # volcado al ndjson
    file = open("Ejercicio2_Volcado.ndjson", "w")
    for tweet in expandida["hits"]["hits"]:
        infoTweet = tweet["_source"]
        contenido = json.dumps(
            {
                "Fecha": infoTweet["created_at"],
                "Autor": infoTweet["user_id_str"],
                "Texto": infoTweet["text"]
            }
        )
        file.write(contenido+"\n")

    fin = datetime.now()
    print(fin-inicio)


if __name__ == '__main__':
    main()
