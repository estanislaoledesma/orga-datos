
#encoding: latin1

import csv

NOMBRE_ARCHIVO_ENTRADA = "data_train.csv"
NOMBRE_ARCHIVO_SALIDA = "formato-rw.txt"
	
#"Id","ProductId","UserId","ProfileName","HelpfulnessNumerator","HelpfulnessDenominator","Prediction","Time","Summary","Text"

def parser(nombre_archivo_entrada, nombre_archivo_salida, pesoTexto, pesoResumen):

    try:
        #archivo_entrada = open(nombre_archivo_entrada)
        archivo_salida = open(nombre_archivo_salida, 'w')
        csvfile = open(nombre_archivo_entrada, 'rb')
        archivo_csv = csv.DictReader(csvfile)
    except IOError:
        print "¡Error! "

    linea_salida = []
    unstringfeo = "vowpal no cabe: esta no le cabe:"
    unstringlindo = str(unstringfeo).replace(':',"")
    print unstringlindo
    contador = 0
    promedioHelpDenom = obtenerPromedio(nombre_archivo_entrada)

    for linea in archivo_csv:
    	tag = linea["Id"]
    	resumen = linea["Summary"]
    	texto = linea["Text"]
        try:
    	   prediction = float(linea["Prediction"])

    	   HelpNumerator = float(linea["HelpfulnessNumerator"])
    	   HelpDenominator = float(linea["HelpfulnessDenominator"])
        except ValueError as exc:
            print "Error al convertir a flotante, info: ", exc
    	if HelpDenominator != 0:
    		importancia = 1 + ((HelpNumerator/HelpDenominator)*(HelpDenominator/promedioHelpDenom))
    	else:
    		importancia = 1
    	base = 0

    	campoResumen = resumen
    	campoTexto = texto

    	linea_salida = str(prediction) + " " + str(importancia) + " " + str(base) + " '" + str(tag) \
        + " |Resumen:" + str(pesoResumen) + " " + str(str(campoResumen).replace(':',"")) \
        + " |Texto:" + str(pesoTexto) + " " + str(str(campoTexto).replace(':',"")) + "\n"
    	
    	archivo_salida.write(linea_salida)

    	contador += 1


def obtenerPromedio(setPath):
	with open(setPath, 'rb') as csvfile:
		reader = csv.DictReader(csvfile, delimiter = ',', quotechar = '"')
		contador = 0
		acumulador = 0
		for linea in reader:
			if contador != 0:
				acumulador += int(linea["HelpfulnessDenominator"])
			contador += 1		
		return acumulador / float(contador)


def main():
	parser(NOMBRE_ARCHIVO_ENTRADA, NOMBRE_ARCHIVO_SALIDA, 1.0, 0.5)

main()