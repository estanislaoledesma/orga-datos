
#encoding: utf-8

import csv

ARCHIVO_ENTRADA_ENTRENAMIENTO = "data_train.csv"
ARCHIVO_SALIDA_ENTRENAMIENTO = "data_train_vw.txt"
ARCHIVO_ENTRADA_TEST = "data_test.csv"
ARCHIVO_SALIDA_TEST = "data_test_vw.txt"


    
#"Id","ProductId","UserId","ProfileName","HelpfulnessNumerator","HelpfulnessDenominator","Prediction","Time","Summary","Text"

def parser(nombre_archivo_entrada, nombre_archivo_salida, pesoTexto, pesoResumen, conPrediccion):

    try:
        #archivo_entrada = open(nombre_archivo_entrada)
        archivo_salida = open(nombre_archivo_salida, 'w')
    except IOError:
        print "Error al arir el archivo de entrada, más información: ", exc
        return

    try:
        csvfile = open(nombre_archivo_entrada, 'rb')
        archivo_csv = csv.DictReader(csvfile)
    except IOError as exc:
        print "Error al abrir el archivo de entrada, más información: ", exc
        archivo_salida.close()
        return

    promedioHelpDenom = obtenerPromedio(nombre_archivo_entrada)

    for linea in archivo_csv:
        tag = linea["Id"]
        resumen = linea["Summary"]
        texto = linea["Text"]
        HelpNumerator = float(linea ["HelpfulnessNumerator"])
        HelpDenominator = float(linea ["HelpfulnessDenominator"])
        if HelpDenominator != 0:
            importancia = 1 + ((HelpNumerator / HelpDenominator) * (HelpDenominator / promedioHelpDenom))
        else:
            importancia = 1
        base = 0

        campoResumen = resumen
        campoTexto = texto

        if (conPrediccion):
            prediction = float(linea["Prediction"])
            linea_salida = str(prediction) + " "
        else:
            linea_salida = ""

        linea_salida += str(importancia) + " " + str(base) + " '" + str(tag) \
        + " |Resumen:" + str(pesoResumen) + " " + str(str(campoResumen).replace(':',"")) \
        + " |Texto:" + str(pesoTexto) + " " + str(str(campoTexto).replace(':',"")) + "\n"
        
        archivo_salida.write(linea_salida)

    csvfile.close()
    archivo_salida.close()


def obtenerPromedio(setPath):
    with open(setPath, 'rb') as csvfile:
        reader = csv.DictReader(csvfile)
        contador = 0
        acumulador = 0
        for linea in reader:
            if contador != 0:
                acumulador += int(linea["HelpfulnessDenominator"])
            contador += 1       
        return acumulador / float(contador)


def main():
    parser(ARCHIVO_ENTRADA_ENTRENAMIENTO, ARCHIVO_SALIDA_ENTRENAMIENTO, 1.0, 0.5, True)
    parser(ARCHIVO_ENTRADA_TEST, ARCHIVO_SALIDA_TEST, 1.0, 0.5, False)

main()