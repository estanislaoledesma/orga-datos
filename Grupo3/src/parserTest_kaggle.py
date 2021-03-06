import csv
import string

header = "\"ID\",\"Texto\"\n"


try:
	f = open('test_procesado.csv', 'w')
except IOError:
    print "Error al arir el archivo de salida.", exc

try:
    csvfile = open('test.csv', 'rb')
    archivo_csv = csv.DictReader(csvfile)
except IOError as exc:
    print "Error al abrir el archivo de entrada.", exc
    f.close()


replace_punctuation = string.maketrans(string.punctuation, ' '*len(string.punctuation))
for linea in archivo_csv:
    iduser = str(linea['Id'])
    texto = str(linea['Text']).translate(replace_punctuation).lower()
    linea = iduser + "|" + texto + "\n"

    f.write(linea)

csvfile.close()
f.close()
