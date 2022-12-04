import json
import requests

from elasticsearch import Elasticsearch


def main():
    escribir = open("palabrasVaciasEN.txt", "w")
    file = open("file.txt")

    for linea in file:
        palabra = str(linea.split("|")[0].strip())
        if(len(palabra)!=0):
            escribir.write(palabra+"\n")
    
    escribir.close()
    file.close()



if __name__ == '__main__':
    main()
