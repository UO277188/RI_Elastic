import json

from elasticsearch import Elasticsearch
from elasticsearch import helpers

def main():

    from datetime import datetime

    inicio = datetime.now()

    # contraseña
    ELASTIC_PASSWORD = "xrAKxQx4YsCeB3pa=J9X"

    # Cliente de elastic
    global es

    es = Elasticsearch(
        "https://localhost:9200",
        ca_certs="./http_ca.crt",
        basic_auth=("elastic", ELASTIC_PASSWORD)
    )

    # Crea el indice de tuits en español con shingles de dos y tres terminos
    argumentos={
        "settings": {
            "analysis": {
                "filter": {
                    "uni_bi_tri_gramas": {
                        "type":"shingle",
                        "min_shingle_size":2,
                        "max_shingle_size":3,
                        "output_unigrams":"true"
                    }
                },
                "analyzer": {
                    "analizador_personalizado": {
                        "tokenizer":"standard",
                        "filter":["lowercase","uni_bi_tri_gramas"]
                    }
                }
            }
        },
        "mappings": {
            "properties": {
                "created_at": {
                  "type":"date",
                  "format": "EEE MMM dd HH:mm:ss Z yyyy"
                },
                "text": {
                    "type":"text",
                    "analyzer":"analizador_personalizado",
                    "fielddata": "true"
                }
            }
        }
    }

    es.indices.create(index="tweets-20090624-20090626-en_es-10percent-ejercicio1",ignore=400,body=argumentos)

    # Ahora se indexan los documentos en bloques
    global contador
    contador = 0

    tamano = 40*1024*1024
    fh = open("tweets-20090624-20090626-en_es-10percent.ndjson", 'rt')
    lineas = fh.readlines(tamano)
    while lineas:
      procesarLineas(lineas)
      lineas = fh.readlines(tamano)
    fh.close()

    fin = datetime.now()

    print(fin-inicio)

# indexa en bloques
def procesarLineas(lineas):
  jsonvalue = []

  for linea in lineas:
    datos = json.loads(linea)

    ident = datos["id_str"]

    if datos["lang"]=="es":
        datos["_index"] = "tweets-20090624-20090626-en_es-10percent-ejercicio1"
        datos["_id"] = ident

        jsonvalue.append(datos)

  num_elementos = len(jsonvalue)
  resultado = helpers.bulk(es,jsonvalue,chunk_size=num_elementos,request_timeout=200)

  global contador

  contador += num_elementos
  print(contador)

if __name__ == '__main__':
    main()