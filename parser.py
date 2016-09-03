
#encoding: latin1

import csv

NOMBRE_ARCHIVO_ENTRADA = "train.csv"
NOMBRE_ARCHIVO_SALIDA = "formato-rw.txt"
	
#"Id","ProductId","UserId","ProfileName","HelpfulnessNumerator","HelpfulnessDenominator","Prediction","Time","Summary","Text"

def parser(nombre_archivo_entrada, nombre_archivo_salida, pesoTexto, pesoResumen):

    try:
        #archivo_entrada = open(nombre_archivo_entrada)
        archivo_salida = open(nombre_archivo_salida, 'w')
        csvfile = open(nombre_archivo_entrada, 'rb')
        archivo_csv = csv.reader(csvfile, delimiter = ',', quotechar = '"')
    except IOError:
        print "¡Error! "


    linea_salida = []
    contador = 0
    promedioHelpDenom = obtenerPromedio(nombre_archivo_entrada)

    for linea in archivo_csv:
    	print contador
    	tag = linea[1]
    	resumen = linea[8]
    	texto = linea[9]
    	prediction = float(linea[6])

    	HelpNumerator = float(linea[4])
    	HelpDenominator = float(linea[5])
    	if HelpDenominator != 0:
    		importancia = 1 + ((HelpNumerator/HelpDenominator)*(HelpDenominator/promedioHelpDenom))
    	else:
    		importancia = 1
    	base = 0

    	campoResumen = resumen
    	campoTexto = texto


    	linea_salida = str(prediction) + " " + str(importancia) + " " + str(base) + " " + str(tag) \
                        + "|Resumen:" + str(pesoResumen) + " " + str(campoResumen) \
                        + " |Texto:" + str(pesoTexto) + " " + str(campoTexto) + "\n"
    	
    	archivo_salida.write(linea_salida)

    	contador = contador + 1


def obtenerPromedio(setPath):
	with open(setPath, 'rb') as csvfile:
		reader = csv.reader(csvfile, delimiter = ',', quotechar = '"')
		contador = 0
		acumulador = 0
		for linea in reader:
			if contador != 0:
				acumulador = acumulador + int(linea[5])
			contador = contador + 1		
		return acumulador / float(contador)


def main():
	parser(NOMBRE_ARCHIVO_ENTRADA, NOMBRE_ARCHIVO_SALIDA, 1, 0.5)

main()