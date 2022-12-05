import json

from elasticsearch import Elasticsearch
from elasticsearch import helpers

def queryConTermSig(tema, numTerminosSignificativos, metrica):
    """
    Función para encontrar los x terminos significativos de un tema con la métrica indicada
    Genera y devuelve la query que incluye los términos encontrados
    """
    significantTermsJson = es.search(
        index="tweets-20090624-20090626-en_es-10percent-ejercicio2",
        body={
           "track_total_hits": "true",
            "query": {
                "query_string": {
                    "query": tema + " AND lang:en"
                }
            },
            "aggs": {
                "Most significant terms": {
                    "significant_terms": {
                        "field": "text",
                        "size": numTerminosSignificativos,
                        metrica:{}
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
    query = "lang:en AND ("+tema
    for i in range (0, len(sigTerms)):
        if(i==len(sigTerms)-1):
            query += " OR text:"+sigTerms[i]
    query += ")"

    return query


def crearNDJSON(resultado):
    """
    Crea el fichero NDJSON con los resultados obtenidos. Una línea por tweet
    """
    file = open("Ejercicio2_Volcado.ndjson", "w")
    for tweet in resultado:
        infoTweet = tweet["_source"]
        contenido = json.dumps(
            {
                "Fecha": infoTweet["created_at"],
                "Autor": infoTweet["user_id_str"],
                "Texto": infoTweet["text"]
            }
        )
        file.write(contenido+"\n")


def resultadosMedida(tema, numTerminosSignificativos, metrica):
    """
    Genera un fichero con 20 resultados obtenidos de realizar la consulta del tema con el número
    de términos significativos y métrica especificados
    """
    query = queryConTermSig(tema, numTerminosSignificativos, metrica)
    
    # hago la consulta expandida
    expandida = es.search(
        index="tweets-20090624-20090626-en_es-10percent-ejercicio2",
        body={
            "track_total_hits": "true",
            "size": 20,
            "query": {
                "query_string": {
                    "query": query
                }
            }
        }
    )

    file = open("metrica_"+str(numTerminosSignificativos)+"_"+str(metrica)+".txt", "w")
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

    #------------------- CONFIGURACION --------------------------
    tema = "text:\"togo\""
    numTerminosSignificativos = 2
    metricaNDJSON = "gnd"
    #------------------------------------------------------------

    # consulta para encontrar los significant terms y generar la query expandida
    query = queryConTermSig(tema, numTerminosSignificativos, metricaNDJSON)

    # hago la consulta expandida
    expandida = helpers.scan(es,
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
    crearNDJSON(expandida)

    # generar archivos para medidas con 2 y 5 términos significativos usando mutual information
    resultadosMedida(tema, numTerminosSignificativos=2, metrica="mutual_information")
    resultadosMedida(tema, numTerminosSignificativos=5, metrica="mutual_information")

    # generar archivos para medidas con 2 y 5 términos significativos usando jlh
    resultadosMedida(tema, numTerminosSignificativos=2, metrica="jlh")
    resultadosMedida(tema, numTerminosSignificativos=5, metrica="jlh")
    
    fin = datetime.now()
    print(fin-inicio)


if __name__ == '__main__':
    main()