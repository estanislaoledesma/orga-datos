#empiezo el approach por LSH
#DATASETS=== http://jmcauley.ucsd.edu/data/amazon/links.html
#https://nickgrattan.wordpress.com/2014/03/03/lsh-for-finding-similar-documents-from-a-large-number-of-documents-in-c/
#https://github.com/chrisjmccormick/MinHash/blob/master/runMinHashExample.py
import numpy as np
from random import randint, seed, choice, random
from pyspark import SparkContext, SparkConf
import string
import sys
import itertools
import json
import gzip
import shutil

primo = 1007
minClusterSize = 5
maxint = sys.maxint
bandas = 17
hashesPorBanda = 2
cantHashes = bandas * hashesPorBanda
#hashDisplaces = [str(random() % primo) for x in range(cantHashes)] #OFFSET PARA GENERAR DISTINTOS VALORES DE FUNCIONES DE HASH
hashDisplaces = []
for x in range(cantHashes):
	hashDisplaces.append(int(random()))



shingleaWords = True
valoresPorBanda = dict()


def dame_shingles_chars(texto, cantidadChars):
	return [texto[i:i + cantidadChars] for i in range(len(texto) - cantidadChars + 1)]

def dame_shingles_words(texto, cantidadPalabras, maxLargoPalabra):
	texto = texto.split()
	return [texto[i:i+cantidadPalabras] for i in range(len(texto) - cantidadPalabras + 1) if len(texto[i]) < maxLargoPalabra]

def dame_minhashes_shingles(shingles):
	minhashes = []
	for i in range(cantHashes):
		minhash = maxint


		if shingleaWords:
			for conjunto in shingles:
				for shingle in conjunto:
					shingle += hashDisplaces[i]
					esteHash = abs(hash(shingle))
					if esteHash < minhash:
						minhash = esteHash
		else:
			for shingle in shingles:
				shingle += hashDisplaces[i]
				esteHash = abs(hash(shingle))
				if esteHash < minhash:
					minhash = esteHash



		minhashes.append(minhash)
	return minhashes


def dame_minhashes_shingles2(shingles):
	minhashes = []
	for i in range(cantHashes):
		minhash = maxint
		esteHash = 0
		if shingleaWords:
			for conjunto in shingles:
				for shingle in conjunto:
					esteHash += abs(hash(shingle) + hashDisplaces[i])
				if esteHash < minhash:
					minhash = esteHash
		else:
			for shingle in shingles:
				esteHash = abs(hash(shingle + hashDisplaces[i]))
				if esteHash < minhash:
					minhash = esteHash
		minhashes.append(minhash)
	return minhashes



def dame_hash_bandasNumpy(minhashes):
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
	codigosBandas = []
	hashBanda = 0
	intervalo = 0
	for i in range(cantHashes):
		#hashBanda += hash(minhashes[i])
		hashBanda += minhashes[i]		
		intervalo = intervalo + 1
		if (intervalo == bandas):
			codigosBandas.append(hashBanda)
			hashBanda = 0
			intervalo = 0
	return codigosBandas


def proc_texto_rating(texto, rating):
	if shingleaWords:
		codigosBandas = dame_hash_bandas(dame_minhashes_shingles(dame_shingles_words(texto, 2, 15)))
	else:
		codigosBandas = dame_hash_bandas(dame_minhashes_shingles(dame_shingles_chars(texto, 5)))
	for codigo in codigosBandas:
		valoresPorBanda.setdefault(codigo, [])
		valoresPorBanda[codigo].append(rating)


def flatmapeo(idReview, puntaje, array):
	lista = []
	for x in array:
		lista.append((idReview,puntaje,x))
	return lista

def damePromedioBucket(indiceDict):
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

learn = learn.map(lambda x: x.split('|')).map(lambda x: (x[0],x[2], dame_hash_bandas(dame_minhashes_shingles2(dame_shingles_words(x[1],3,15)))))
learn = learn.flatMap(lambda x: flatmapeo(x[0], x[1], x[2])) #(u'a9wx8dk93sn5', u'1.0', 813759583895638922)
learn = learn.map(lambda x: (x[2], x[1]))
data = learn.collect()

for a in data:
	valoresPorBanda.setdefault(a[0], [])
	valoresPorBanda[a[0]].append(a[1])
	

#flatmapeado = learn.count()
#learn = learn.groupByKey()
#agrupado = learn.count()
#learn = learn.filter(lambda x: len(x[1].data) >= minClusterSize)
#filtrado = learn.count()
#dicto = learn.collectAsMap()

try:
	shutil.rmtree('/media/tino/Tera/bigdata/finalout.csv')
except:
	print "Folder does not exist."

#learn.saveAsTextFile('/media/tino/Tera/bigdata/finalout.csv')





#
#	Comienza la prediccion
#


test = sc.textFile('/media/tino/Tera/bigdata/parsedTestSmall.csv',8)
test = test.map(lambda x: x.split('|')).map(lambda x: (x[0],x[2], dame_hash_bandas(dame_minhashes_shingles2(dame_shingles_words(x[1],3,15)))))
test = test.map(lambda x: (x[0],x[1],calcScore(x[2])))
cantReviewsToPredict = test.count()
test = test.filter(lambda x: x[2] != -1)
cantReviewsPredicted = test.count()

try:
	shutil.rmtree('/media/tino/Tera/bigdata/scores.csv')
except:
	print "Folder does not exist."
test.saveAsTextFile('/media/tino/Tera/bigdata/scores.csv')

print "REVIEWS A PREDECIR: " + str(cantReviewsToPredict)
print "REVIEWS CON SCORE: " + str(cantReviewsPredicted)
print "RELACION: " + str(cantReviewsPredicted / cantReviewsToPredict)

