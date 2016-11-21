#
#
#	TP ORGANIZACION DE DATOS - GRUPO 3 - ESTANISLAO LEDESMA, MARTIN BOSCH Y MARTIN QUEIJA
#						28/11/16
#			FACULTAD DE INGENIERIA DE LA UNIVERSIDAD DE BUENOS AIRES
#
#

#DATASETS=== http://jmcauley.ucsd.edu/data/amazon/links.html
#https://nickgrattan.wordpress.com/2014/03/03/lsh-for-finding-similar-documents-from-a-large-number-of-documents-in-c/
#https://github.com/chrisjmccormick/MinHash/blob/master/runMinHashExample.py

from random import randint, random
import math
from pyspark import SparkContext, SparkConf
import string
import sys, os
import itertools
import json
import gzip
import shutil
import datetime

maxint = sys.maxint
bandas = 15
hashesPorBanda = 1
cantHashes = bandas * hashesPorBanda
nGram = 15
xorHash = True
shingleaWords = False
escribeScoresKaggle = True
estimaFaltantes = True

#lineasEnLearn = 300000					#Big Dataset Learn File
lineasEnLearn = 454761 #TrainKaggle00

#cantReviewsToPredict = 82680000
#cantReviewsToPredict = 600001 
cantReviewsToPredict = 113691 #kaggleTestFile
hashDisplaces = []

valoresPorBanda = dict()

rutaLearnFile = "train_procesado.csv"
rutaTestFile = "test_procesado.csv"
rutaScores = "grupo3.csv"



#
#
#	Funciones
#
#
def esPrimo(n):
	for i in range(3, n):
		if n % i == 0:
			return False
		return True

def damePrimoSiguiente(numero):
	numero = numero + 1
	while (not esPrimo(numero)):
		numero = numero + 1
	return numero

primoC = damePrimoSiguiente(bandas * lineasEnLearn)

def log(text):
	with open("log.txt", "a") as myfile:
		myfile.write(text + "\n")

def logInit():
	log("\n\n" + str(datetime.datetime.now()))
	log("-------------LOG---------------")
	log("Learn file: " + rutaLearnFile)
	log("Reviews to learn: " + str(lineasEnLearn))
	log("Test file: " + rutaTestFile)
	log("Reviews to predict: " + str(cantReviewsToPredict))
	log("Bandas: " + str(bandas))
	log("Hashes por banda: " + str(hashesPorBanda))
	log("Bandas calculadas: " + str(bandas * lineasEnLearn))
	log("Cantidad de hashes: " + str(hashesPorBanda * bandas))
	log("XOR: " + str(xorHash))
	log("Escribe scores Kaggle: " + str(escribeScoresKaggle))
	if (escribeScoresKaggle):
		log("En : " + rutaScores)
	if (shingleaWords):
		log("Shinglea por palabras con K-Grams: " + str(nGram))
	else:
		log("Shinglea por caracteres con K-Grams: " + str(nGram))
	log(" ")

#
#	generateRandoms()
#	Genera una lista de numeros semi-aleatorios, con estos se XORea la funcion de Hash de python para obtener variantes de la funcion de hash.
#
def generateRandoms():
	randoms = []
	for x in range(cantHashes):					
		randoms.append(int(randint(0,maxint)))
	return randoms


#
#	dameShingles()
#	Si shingleaWords = True, devuelve una lista de listas. Cada subLista contiene nGram cantidad de palabras (< maxLargoPalabra) por shingle.
#	si shingleaWords = False, devuelve una lista de shingles de nGram caracteres.
#
def dameShingles(texto, nGram, maxLargoPalabra):
	if (shingleaWords):
		texto = texto.split()
		return [texto[i:i+nGram] for i in range(len(texto) - nGram + 1) if len(texto[i]) < maxLargoPalabra]
	else:
		return [texto[i:i + nGram] for i in range(len(texto) - nGram + 1)]	

	
#
#	dameMinhashShingles()
#	Devuelve la lista de minhashes para la lista de shingles por caracteres o palabra.
#	Si xorHash = True, el displace random para la funcion hash() se XOrea, de lo contrario se suma.
#
def dameMinhashShingles(shingles):
	minhashes = []
	for i in range(cantHashes):
		minhash = maxint
		esteHash = 0
		if shingleaWords:
			for conjunto in shingles:
				if xorHash:
					esteHash = abs(hash(" ".join(conjunto)) ^ hashDisplaces[i])
				else :
					esteHash = abs(hash(" ".join(conjunto)) + hashDisplaces[i])
				if esteHash < minhash:
					minhash = esteHash
				esteHash = 0
		else:
			for shingle in shingles:
				if xorHash:
					esteHash = abs(hash(shingle) ^ hashDisplaces[i])
				else:
					esteHash = abs(hash(shingle) + hashDisplaces[i])
				if esteHash < minhash:
					minhash = esteHash
		minhashes.append(minhash)
	return minhashes

#
#	dameHashBandas()
#	Acumula los hashes de los minhashes de acuerdo a la cantidad de Bandas y hashesPorBandas.
#	Si xorHash = True, el displace random para la funcion hash() se XOrea, de lo contrario se suma.
#
def dameHashBandas(minhashes):
	codigosBandas = []
	hashBanda = 0
	intervalo = 0
	for i in range(cantHashes):
		hashBanda += hash(minhashes[i])	
		intervalo = intervalo + 1
		if (intervalo == hashesPorBanda):
			codigosBandas.append(hashBanda)
			hashBanda = 0
			intervalo = 0
	return codigosBandas

#
#	myFlatMap()
#	Para un review, un puntaje y un array de claves de bandas:
#	Genera tuplas del estilo: (idReview, Puntaje, claveBanda)
#	
def myFlatMap(idReview, puntaje, array):
	lista = []
	for x in array:
		lista.append((idReview,puntaje,x))
	return lista

#
#	damePromedioBucket()
#	Para una clave de Bandas, busca en el diccionario y 
#	devuelve el promedio de los scores que se encuentran en ese bucket.
#	
def damePromedioBucket(indiceDict):
	count = 0
	acum = 0
	try:
		for i in valoresPorBanda[indiceDict]:
			count += 1
			acum += float(i)
		if (count != 0):
			return float(acum / float(count))
		else:
			return 0
	except:
		return 0



#
#	calcScore()
#	Para una lista de claves de Bandas, consulta los promedios para cada una
#	y devuelve el promedio de los promedios de los buckets.
#	
def calcScore(bandas):
	acum = 0
	count = 0
	for unaBanda in bandas:
		promedioBucket = damePromedioBucket(unaBanda)
		if (promedioBucket != 0):
			count += 1
		acum += promedioBucket
	if (acum == 0):
		if estimaFaltantes:
			return 3
		return -1
	else:
		promedioBuckets = acum / count
		if (promedioBuckets < 1):
			promedioBuckets = 1
		return promedioBuckets
#
#	myReduce()
#	Se utiliza en el reduce del calculo del error cuadratico. 
#	En una parte de la tupla almacena la sumatoria de los errores al cuadrado,
#	y en la otra la cantidad de reviews con predictedScore != -1
#	
def myReduce(tup0,tup1):
	return (tup0[0]+tup1[0], tup0[1]+tup1[1])

#
#	
#	Plain Locality Sensitive Hashing
#
#


logInit()
hashDisplaces = generateRandoms()

sc = SparkContext(conf = SparkConf())
learn = sc.textFile(rutaLearnFile,4)

learn = learn.map(lambda x: x.split('|'))
	#(ID, ReviewText, Score)
learn = learn.map(lambda x: (x[0],x[2], dameShingles(x[1],nGram,500)))
	#(ID, Score, [Shingles])
learn = learn.map(lambda x: (x[0],x[1], dameMinhashShingles(x[2])))
	#(ID, Score, [minHashShingles])
learn = learn.map(lambda x: (x[0],x[1], dameHashBandas(x[2])))
	#(ID, Score, [clavesBandas])
learn = learn.flatMap(lambda x: myFlatMap(x[0], x[1], x[2])) 
	#(ID, Score, claveBanda)
learn = learn.map(lambda x: (x[2], x[1]))
	#(claveBanda, Score)
data = learn.collect()						#Non Lazy Operation

print("\nDICCIONARIO\n")
for learnedReview in data:
	valoresPorBanda.setdefault(learnedReview[0], [])			#Establece el formato del diccionario
	valoresPorBanda[learnedReview[0]].append(float(learnedReview[1]))	#Agrega el valor del score de learnedReview al diccionario
log("Claves en el diccionario: " + str(len(valoresPorBanda)))
print("\nFIN DICCIONARIO\n")


#
#	Comienza la prediccion
#
print("\nCOMIENZA PREDICCION\n")

test = sc.textFile(rutaTestFile,4)
test = test.map(lambda x: x.split('|'))
	#(ID, ReviewText)
test = test.map(lambda x: (x[0], dameShingles(x[1],nGram,500)))
	#(ID, [Shingles])
test = test.map(lambda x: (x[0], dameMinhashShingles(x[1])))
	#(ID, [MinHashes])
test = test.map(lambda x: (x[0], dameHashBandas(x[1])))
	#(ID, [ClavesBandas])
test = test.map(lambda x: (x[0], calcScore(x[1])))
	#(ID, predictedScore)
#test = test.filter(lambda x: x[2] != -1)

print("\n1\n")

if escribeScoresKaggle:
	data2 = test.collect()
	print("\n2\n")

	f = open(rutaScores, 'w')

	cabecera = "\"Prediction\",\"Id\"\n"
	f.write(cabecera);
	for pred in data2:
			linea = str(pred[1]) + "," + str(pred[0]) + "\n"
			f.write(linea)
	f.close()
	sys.exit(0)


#
#	Predecir reviews con score para obtener error del algoritmo
#

"""
test = sc.textFile(rutaTestFile,3)
test = test.map(lambda x: x.split('|'))
	#(ID, ReviewText, Score)
test = test.map(lambda x: (x[0],x[2], dameShingles(x[1],nGram,500)))
	#(ID, Score, [Shingles])
test = test.map(lambda x: (x[0],x[1], dameMinhashShingles(x[2])))
	#(ID, Score, [MinHashes])
test = test.map(lambda x: (x[0],x[1], dameHashBandas(x[2])))
	#(ID, Score, [ClavesBandas])
test = test.map(lambda x: (x[0],x[1],calcScore(x[2])))
	#(ID, Score, predictedScore)
test = test.filter(lambda x: x[2] != -1)

if escribeScoresKaggle:
	data = test.collect()

	cabecera = "\"Prediction\",\"Id\"\n"
	predic.write(cabecera);
	f = open(rutaScores, 'w')
	for pred in data:
			linea = str(pred[2]) + "," + str(pred[0]) + "\n"
			f.write(linea)
	f.close()
	sys.exit(0)

#
#	Obtenemos el Error en las predicciones
#
	#(ID, Score, predictedScore)
test = test.map(lambda x: abs(float(x[2])-float(x[1])))
	#(abs(predictedScore - Score)
test = test.map(lambda x: (x*x))
	#(diff^2)
test = test.map(lambda x: (x,1))
	#((diff^2,1))
tupla = test.reduce(lambda x,y: myReduce(x,y)) 
	#tupla = (Sum(diff^2), cantReviewsPredicted)

sumaCuadradosDiferencia = tupla[0]
cantReviewsPredicted = tupla[1]
error = sumaCuadradosDiferencia / cantReviewsPredicted


log("Predicted / To Predict: " + str(float(float(cantReviewsPredicted) / float(cantReviewsToPredict))))
log("Reviews con score: " + str(cantReviewsPredicted))
log("Error cuadratico medio: " + str(error))

"""