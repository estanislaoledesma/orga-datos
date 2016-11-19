
#encoding: utf-8

import csv

TEST_SALIDA_VW = "test_predicciones.txt"
TEST = "test.csv"
TEST_ENTREGA = "predicciones.csv"


#"Id","ProductId","UserId","ProfileName","HelpfulnessNumerator","HelpfulnessDenominator","Prediction","Time","Summary","Text"

def parser(test_salida_vw, test):

    try:
        #archivo_entrada = open(nombre_archivo_entrada)
        salida_vw = open(test_salida_vw, 'rb')
        salida_vw_csv = csv.reader(salida_vw)
    except IOError as exc:
        print "Error al arir el archivo de entrada, más información: ", exc
        return

    try:
        test_aux = open(test, 'rb')
        test_csv = csv.DictReader(test_aux)
    except IOError as exc:
        print "Error al abrir el archivo de entrada, más información: ", exc
        salida_vw.close()
        return

    try:
        predic = open(TEST_ENTREGA, 'w')
    except IOError as exc:
        print "Error al abrir el archivo de entrada, más información: ", exc
        salida_vw.close()
        test_aux.close()
        return

#    cabecera = "\"Id\",\"ProductId\",\"UserId\",\"ProfileName\",\"HelpfulnessNumerator\",\"HelpfulnessDenominator\",\"Prediction\",\"Time\",\"Summary\",\"Text\" \n"
    cabecera = "\"Prediction\",\"Id\"\n"
    predic.write(cabecera);

    for linea in salida_vw_csv:
        linea_salida = linea[0].replace(" ", ",")
        linea_aux = linea_salida.split(",")
        if ( float(linea_aux[0]) < 1.0 ):
        	num = 1.0

        elif( float(linea_aux[0]) > 5.0 ):
        	num = 5.0

        else:
        	num = float(linea_aux[0])

        linea_salida = str(num) + "," + str(linea_aux[1]) + "\n"


#    for linea in test_csv:
#        linea_vw = salida_vw_csv.next();

#        ProductId = "\"" + linea["ProductId"] + "\""
#        UserId = "\"" + linea["UserId"] + "\""
#        ProfileName = "\"" + linea["ProfileName"] + "\""
#        Summary = "\"" + linea["Summary"] + "\""
#        Text = "\"" + linea["Text"] + "\""


#        linea_salida = linea["Id"] +"," + ProductId + "," + UserId + "," + ProfileName + "," +\
#                        linea["HelpfulnessNumerator"] + "," + linea["HelpfulnessDenominator"] + "," +\
#                         str(linea_vw[0][0]) + "," + linea["Time"] + "," + Summary + "," + Text + "\n"

        predic.write(linea_salida)


    salida_vw.close()
    test_aux.close()
    predic.close()


def main():
    parser(TEST_SALIDA_VW, TEST)


main()