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


import numpy as np
from random import randint, seed, choice, random
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
nGram = 3
shingleaWords = True
lineasEnLearn = 1001
maxCantCodigosBandas = bandas * lineasEnLearn

valoresPorBanda = dict()

def log(text):
	with open("/media/tino/Tera/bigdata/log.txt", "a") as myfile:
		myfile.write(text + "\n")

log("\n\n" + str(datetime.datetime.now()))
log("-------------LOG---------------")
log("Bandas: " + str(bandas))
log("Hashes por banda: " + str(hashesPorBanda))
log("Cantidad de hashes: " + str(hashesPorBanda * bandas))
if (shingleaWords):
	log("Shinglea por palabras con K-Grams: " + str(nGram))
else:
	log("Shinglea por caracteres con K-Grams: " + str(nGram))
log(" ")




hashDisplaces = []
for x in range(cantHashes):
	hashDisplaces.append(int(random()))
	#hashDisplaces.append(0)

hashDisplacesNuevo = []
for x in range(cantHashes):
	hashDisplacesNuevo.append((int(random()), int(random())))

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

primoC = damePrimoSiguiente(maxCantCodigosBandas)

def hashInt(integer,indice):
	a = hashDisplacesNuevo[indice][0]
	b = hashDisplacesNuevo[indice][1]
	return ((a*integer)+b) % primoC





def dame_shingles_chars(texto, cantidadChars):

	return [texto[i:i + cantidadChars] for i in range(len(texto) - cantidadChars + 1)]

def dame_shingles(texto, nGram, maxLargoPalabra):

	
	if (shingleaWords):
#Devuelve una lista con los shingles de texto, procesado palabra por palabra, donde los mismos tienen un 
#tamano de nGram y un maximo de largo de palabra igual a maxLargoPalabra.'''
		texto = texto.split()
		return [texto[i:i+nGram] for i in range(len(texto) - nGram + 1) if len(texto[i]) < maxLargoPalabra]
	else:
#Devuelve una lista con los shingles de texto, procesado caracter por caracter, donde los mismos tienen
#un tamano de nGram.'''

		return [texto[i:i + nGram] for i in range(len(texto) - nGram + 1)]		

def dame_minhashes_shingles2(shingles):
	'''Devuelve una lista de minhashes para la lista de shingles pasada por parametro, utiliza hash and displace.'''
	minhashes = []
	for i in range(cantHashes):
		minhash = maxint
		esteHash = 0
		if shingleaWords:
			for conjunto in shingles:
				#for shingle in conjunto:
				#	esteHash += abs(hash(shingle) + hashDisplaces[i])
				

				#esteHash = abs(hash(" ".join(conjunto)) + hashDisplaces[i])
				
				esteHash = abs(hashInt(hash(" ".join(conjunto)),i))				

				if esteHash < minhash:
					minhash = esteHash
				esteHash = 0
		else:
			for shingle in shingles:
				esteHash = abs(hash(shingle) + hashDisplaces[i])
				if esteHash < minhash:
					minhash = esteHash
		minhashes.append(minhash)
	return minhashes



def dame_hash_bandasNumpy(minhashes):
	'''Devuelve una lista de hashes de banda a partir de una lista de minhashes en la cual hay un numero de bandas
	y un numero de hashesPorBanda para la misma.'''
	a = np.zeros(bandas)
	codigosBandas = []
	hashBanda = 0
	intervalo = 0
	indiceArreglo = 0
	for i in range(cantHashes):
		intervalo = intervalo + 1
		if (intervalo == hashesPorBanda):
			a[indiceArreglo] = hashBanda
			indiceArreglo = indiceArreglo + 1
			hashBanda = 0
			intervalo = 0
		hashBanda += hash(minhashes[i])
	return a

def dame_hash_bandas(minhashes):
	'''Devuelve una lista de hashes de banda a partir de una lista de minhashes en la cual hay un numero de bandas
	y un numero de hashesPorBanda para la misma.'''
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


def flatmapeo(idReview, puntaje, array):
	'''Devuelve una lista donde cada posicion es una tupla donde la primera posicion es idReview, la segunda
	puntaje y la tercera, domde el valor de la misma posicion en array.'''
	lista = []
	for x in array:
		lista.append((idReview,puntaje,x))
	return lista


def damePromedioBucket(indiceDict):
	'''Devuelve el promedio del bucket dado por la clave indiceDict en dicto (un diccionario), donde el valor
	de la misma es una lista de enteros. En caso de error, devuelve 0.'''
	count = 0
	acum = 0
	try:
		for i in valoresPorBanda[indiceDict]:
			count += 1
			acum += float(i)
		if (count != 0):
			return acum / count
		else:
			return 0
	except:
		return 0

def calcScore(bandas):
	'''Devuelve el promedio del promedio de cada banda en bandas (lista de bandas), donde el primer promedio
	se calcula mediante damePromedioBucket de la banda (en el diccionario dicto), y el segundo es un promedio
	de todos esos promedios. En caso de error, devuelve -1.'''
	acum = 0
	count = 0
	for unaBanda in bandas:
		promedioBucket = damePromedioBucket(unaBanda)
		if (promedioBucket != 0):
			count += 1
		acum += promedioBucket
	if (acum == 0):
		return -1
	else:
		return acum / count



sc = SparkContext(conf = SparkConf())
learn = sc.textFile('/media/tino/Tera/bigdata/parsedTrainSmall.csv',8)
lineasEnLearn = learn.count()
learn = learn.map(lambda x: x.split('|')).map(lambda x: (x[0],x[2], dame_minhashes_shingles2(dame_shingles(x[1],nGram,500))))

#from pyspark.mllib.tree import DecisionTree, DecisionTreeModel
#from pyspark.mllib.regression import LabeledPoint
#from pyspark.mllib.linalg import DenseVector

#data_for_decision_tree = learn.map(lambda x: LabeledPoint(label = x [1], features = DenseVector(x[2])))
#(dataTrain, dataTest) = data_for_decision_tree.randomSplit([0.7, 0.3])
#model = DecisionTree.trainRegressor(dataTrain, categoricalFeaturesInfo={}, impurity='variance', maxDepth=5, maxBins=32)
#predictions = model.predict(dataTest.map(lambda x: x.features))
#labelsAndPredictions = dataTest.map(lambda x: x.label).zip(predictions)
#testMSE = labelsAndPredictions.map(lambda (v, p): (v - p) * (v - p)).sum() / float(dataTest.count())
#log("MSE = %f"%(testMSE))

learn = learn.map(lambda x: (x[0],x[1], dame_hash_bandas(x[2])))
learn = learn.flatMap(lambda x: flatmapeo(x[0], x[1], x[2])) #(u'a9wx8dk93sn5', u'1.0', 813759583895638922)
learn = learn.map(lambda x: (x[2], x[1]))
cantBandasCalculadas = learn.count()
data = learn.collect()

for a in data:
	valoresPorBanda.setdefault(a[0], [])
	valoresPorBanda[a[0]].append(a[1])
	
#print valoresPorBanda
#flatmapeado = learn.count()
#learn = learn.groupByKey()
#agrupado = learn.count()
#learn = learn.filter(lambda x: len(x[1].data) >= minClusterSize)
#filtrado = learn.count()
#dicto = learn.collectAsMap()

try:
	shutil.rmtree('finalout.csv')
except:
	print "Folder does not exist."

#learn.saveAsTextFile('/media/tino/Tera/bigdata/finalout.csv')





#
#	Comienza la prediccion
#


test = sc.textFile('/media/tino/Tera/bigdata/parsedTestSmall.csv',8)
test = test.map(lambda x: x.split('|')).map(lambda x: (x[0],x[2], dame_hash_bandas(dame_minhashes_shingles2(dame_shingles(x[1],nGram,500)))))
test = test.map(lambda x: (x[0],x[1],calcScore(x[2])))
cantReviewsToPredict = test.count()
test = test.filter(lambda x: x[2] != -1)
cantReviewsPredicted = test.count()

test = test.map(lambda x: abs(float(x[2])-float(x[1])))
test = test.map(lambda x: (x*x))
number = test.reduce(lambda x,y: x+y)
number = number / cantReviewsPredicted


try:
	shutil.rmtree('scores.csv')
except:
	print "Folder does not exist."
#test.saveAsTextFile('scores.csv')
log("LINEAS EN EL LEARN: " + str(lineasEnLearn))
log("CLAVES EN EL DICC: " + str(len(valoresPorBanda)))
log("BANDAS CALCULADAS: " + str(cantBandasCalculadas))
log("REVIEWS A PREDECIR: " + str(cantReviewsToPredict))
log("REVIEWS CON SCORE: " + str(cantReviewsPredicted))
log("RELACION: " + str(float(float(cantReviewsPredicted) / float(cantReviewsToPredict))))
log("PROMEDIO DE LOS CUADRADOS DE LAS DIFERENCIAS: " + str(number))

