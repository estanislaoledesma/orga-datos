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
bandas = 10
hashesPorBanda = 4
cantHashes = bandas * hashesPorBanda
nGram = 15
xorHash = True
shingleaWords = False
escribeScores = False

#lineasEnLearn = 300000					#Big Dataset Learn File
lineasEnLearn = 227380 #TrainKaggle00

#cantReviewsToPredict = 73680000
cantReviewsToPredict = 227380 #TrainKaggle01
#cantReviewsToPredict = 113691 #kaggleTestFile
hashDisplaces = []

valoresPorBanda = dict()

rutaTestFile = "/home/tino/Documents/trainKaggle01.csv"
rutaLearnFile = "/home/tino/Documents/trainKaggle00.csv"
rutaScores = "/home/tino/Documents/kagglePredictions.csv"



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
	with open("/media/tino/Tera/bigdata/log.txt", "a") as myfile:
		myfile.write(text + "\n")

def logInit():
	log("\n\n" + str(datetime.datetime.now()))
	log("-------------LOG---------------")
	log("Learn file: " + rutaLearnFile)
	log("Reviews to learn: " + str(lineasEnLearn))
	log("Test file: " + rutaTestFile)
	log("Bandas: " + str(bandas))
	log("Bandas calculadas: " + str(bandas * lineasEnLearn))
	log("XOR: " + str(not xorHash))
	log("Hashes por banda: " + str(hashesPorBanda))
	log("Cantidad de hashes: " + str(hashesPorBanda * bandas))
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
		#hashBanda += minhashes[i]		
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
		return 2.5
	else:
		promedioBuckets = acum / count
		if (promedioBuckets < 1):
			promedioBuckets = 1
		return promedioBuckets


def myMap(predictedScore,actualScore):
	if predictedScore == -1:
		return 0
	else:
		return abs(predictedScore - actualScore)


#
#	
#	Plain Locality Sensitive Hashing
#
#


logInit()
hashDisplaces = generateRandoms()
sc = SparkContext(conf = SparkConf())
learn = sc.textFile(rutaLearnFile,8)

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

print("\n\n\n\n\nEMPIEZA DICCIONARIO\n\n\n\n\n")
for learnedReview in data:
	valoresPorBanda.setdefault(learnedReview[0], [])			#Establece el formato del diccionario
	valoresPorBanda[learnedReview[0]].append(float(learnedReview[1]))	#Agrega el valor del score de learnedReview al diccionario
print("\n\n\n\nTERMINA DICCIONARIO\n\n\n\n\n")

log("CLAVES EN EL DICC: " + str(len(valoresPorBanda)))


#
#	Comienza la prediccion
#
print("\n\n\n\COMIENZA PREDICCION\n\n\n\n\n")

test = sc.textFile(rutaTestFile,8)
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


if escribeScores:
	data = test.collect()
	f = open(rutaScores, 'w')
	for pred in data:
			linea = str(pred[2]) + "," + str(pred[0]) + "\n"
			f.write(linea)

print("\n\n\n\CHECKPOINT\n\n\n\n\n")


#cantReviewsPredicted = test.count()		#Non Lazy Operation
cantReviewsPredicted = cantReviewsToPredict

log("RELACION: " + str(float(float(cantReviewsPredicted) / float(cantReviewsToPredict))))
log("REVIEWS A PREDECIR: " + str(cantReviewsToPredict))
log("REVIEWS CON SCORE: " + str(cantReviewsPredicted))

test = test.map(lambda x: abs(float(x[2])-float(x[1])))
#test = test.map(lambda x: myMap(float(x[2]),float(x[1])))
test = test.map(lambda x: (x*x))
number = test.reduce(lambda x,y: x+y)		#Non Lazy Operation
number = number / cantReviewsPredicted

log("PROMEDIO DE LOS CUADRADOS DE LAS DIFERENCIAS: " + str(number))
