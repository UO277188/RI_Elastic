import json

from elasticsearch import Elasticsearch


def consultaMLT(palabras):
    """
    Realiza la consulta MLT a partir del texto del resultado del primer tweet encontrado
    con la consulta original
    """
    return es.search(
        index="tweets-20090624-20090626-en_es-10percent-ejercicio2",
        body={
            "query": {
                "more_like_this": {
                    "fields": ["text"],
                    "like": palabras,
                            "min_term_freq": 1,
                            "max_query_terms": 12
                }
            }
        }
    )


def crearNDJSON(resultado):
    """
    Crea el fichero NDJSON con los resultados obtenidos. Una línea por tweet
    """
    file = open("Ejercicio3_Volcado.ndjson", "w")
    for tweet in resultado["hits"]["hits"]:
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

    consulta = input("Introduce la consulta: ")

    # hace una primera consulta normal pero obtiene solo el primer tweet
    results = es.search(
        index="tweets-20090624-20090626-en_es-10percent-ejercicio2",
        body={
            "size": 1,
            "query": {
                "simple_query_string": {
                    "query": consulta
                }
            }
        }
    )

    # hace la consulta MLT con el texto del tweet como parámetro like
    resultadoMLT = consultaMLT(results["hits"]["hits"][0]["_source"]["text"])

    # escribe los resultados para comprobar el funcionamiento
    crearNDJSON(resultadoMLT)

    fin = datetime.now()
    print(fin-inicio)


if __name__ == '__main__':
    main()
