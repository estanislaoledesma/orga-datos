#encoding: latin1

import csv

NOMBRE_ARCHIVO_ENTRADA = "train.csv"
NOMBRE_ARCHIVO_SALIDA = "formato-rw.txt"


def parser(nombre_archivo_entrada, nombre_archivo_salida):

    try:
        archivo_entrada = open(nombre_archivo_entrada)
        archivo_salida = open(nombre_archivo_salida, 'w')
    except IOError:
        print "¡Error! "


    linea_salida = []

    archivo_csv = csv.reader(archivo_entrada)
    encabezado = archivo_csv.next()

    print "encabezado:", encabezado
    print " "

    for linea in archivo_csv:
		print "linea: ", linea
		print " "
		linea_salida = linea[6] + " 1.0 " + " 0 " + linea[1] + "|Texto:" + " " + linea[9] + " |Resumen:" + " " + linea[8]


		archivo_salida.write(linea_salida + "\n")

	finally:
	    archivo_entrada.close()
	    archivo_salida.close()


def main():
	parser(NOMBRE_ARCHIVO_ENTRADA, NOMBRE_ARCHIVO_SALIDA)

main()