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

    # encuentra los 50 trending topics por cada hora
    results = es.search(
        index="tweets-20090624-20090626-en_es-10percent-ejercicio1",
        body={
            "size": 0,
            "query": {
                "query_string": {
                    "query": "lang:es"
                }
            },
            "aggs": {
                "Trending topics per hour": {
                    "date_histogram": {
                        "field": "created_at",
                        "fixed_interval": "1h"
                    },
                    "aggs": {
                        "Trending topics": {
                            "significant_terms": {
                                "field": "text",
                                "size": 50,
                                "gnd": {}
                            }
                        }
                    }
                }
            }
        }
    )

    # abre un fichero para escribir los resultados
    file = open("Ejercicio1_Entidades.txt", 'w')

    # llamada a la api de wikidata para encontrar las entidades
    keys = {}   # diccionario para guardar las entidades
    for fechas in results["aggregations"]["Trending topics per hour"]["buckets"]:
        for topics in (fechas["Trending topics"]["buckets"]):
            url = "https://www.wikidata.org/w/api.php?action=wbsearchentities&language=es&format=json&search="

            # construyo la URL (para poder utilizar los shingles)
            for key in topics["key"].split(" "):
                url = url + str(key)+"+"

            # consulto con wikidata
            r = requests.get(url)
            resultadoJSON = r.json()
            if len(resultadoJSON["search"])>0:
                entidades = resultadoJSON["search"]
                for entidad in entidades:
                    if(topics["key"] in keys):
                        keys[topics["key"]].append(entidad["id"])
                    else:
                        keys[topics["key"]] = [entidad["id"]]

                file.write(topics["key"]+str(keys[topics["key"]])+"\n")
                
    fin = datetime.now()
    print(fin-inicio)
    file.close()


if __name__ == '__main__':
    main()
