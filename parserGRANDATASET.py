import json
import gzip
import string

header = "\"ID\",\"Texto\",\"Score\"\n"
totalreviews = 83680000
vistas = 0
porc = -1
porcAnt = -2
f = open('/media/tino/Tera/bigdata/elcsvTEST.csv', 'w')
replace_punctuation = string.maketrans(string.punctuation, ' '*len(string.punctuation))
with gzip.open("/home/tino/Downloads/item_dedup.json.gz", "rb") as file:
	for line in file:
		linea = json.loads(line)
		iduser = str(linea['reviewerID'].replace('|','')).translate(replace_punctuation).lower()
		texto = str(linea['reviewText'].replace('|','')).translate(replace_punctuation).lower()
		puntaje = str(linea['overall'])
		linea = iduser + "|" + texto + "|" + puntaje + "\n"
		f.write(linea)
		if (vistas % 100000 == 0):
			print vistas #/ totalreviews * 100
			if (vistas == 1000000):
				f = open('/media/tino/Tera/bigdata/elcsvTRAIN.csv', 'w')
			if (vistas == 1300000):
				break
		vistas = vistas + 1
		
