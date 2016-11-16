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

try:
	shutil.rmtree('/media/tino/Tera/bigdata/finalout.csv')
except:
	print "asd"
primo = 1007
minClusterSize = 5
maxint = sys.maxint
bandas = 5
hashesPorBanda = 5
cantHashes = bandas * hashesPorBanda
hashDisplaces = [str(random() % primo) for x in range(cantHashes)] #OFFSET PARA GENERAR DISTINTOS VALORES DE FUNCIONES DE HASH
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

def dame_hash_bandasNumpy(minhashes):
	a = np.zeros(bandas)
	codigosBandas = []
	hashBanda = 0
	intervalo = 0
	indiceArreglo = 0
	for i in range(cantHashes):
		intervalo = intervalo + 1
		if (intervalo == bandas):
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
		intervalo = intervalo + 1
		if (intervalo == bandas):
			codigosBandas.append(hashBanda)
			hashBanda = 0
			intervalo = 0
		hashBanda += hash(minhashes[i])
	return codigosBandas

def dame_hash_bandasDEPREC(minhashes):
	codigosBandas = []
	hashBanda = 0
	for i in range(cantHashes):
		if (i % hashesPorBanda == 0 and i > 0):
			codigosBandas.append(hashBanda)
			hashBanda = 0
		hashBanda += hash(minhashes[i])
	codigosBandas.append(hashBanda % 100000)
	return codigosBandas


def proc_texto_rating(texto, rating):
	if shingleaWords:
		codigosBandas = dame_hash_bandas(dame_minhashes_shingles(dame_shingles_words(texto, 2, 15)))
	else:
		codigosBandas = dame_hash_bandas(dame_minhashes_shingles(dame_shingles_chars(texto, 5)))
	for codigo in codigosBandas:
		valoresPorBanda.setdefault(codigo, [])
		valoresPorBanda[codigo].append(rating)


def flatmapeo(puntaje, array):
	lista = []
	for x in array:
		lista.append((puntaje,x))
	return lista

def damePromedioBucket(indiceDict):
	count = 0
	acum = 0
	try:
		for i in dicto[indiceDict]:
			count += 1
			acum += i
		if (count != 0):
			return acum / count
		else:
			return 0
	except:
		return 0

def calcScore(bandas):
	acum = 0
	count = 0
	for banda in bandas:
		promedioBucket = damePromedioBucket(banda)
		if (promedioBucket != 0):
			count += 1
		acum += promedioBucket
	if (acum == 0):
		return -1
	else:
		return acum / count



sc = SparkContext(conf = SparkConf())
learn = sc.textFile('/media/tino/Tera/bigdata/parsedTrain.csv',8)
#learn = learn.map(lambda x: x.split('|')).map(lambda x: (x[2], dame_shingles_words(x[1],2,15)))
learn = learn.map(lambda x: x.split('|')).map(lambda x: (x[2], dame_hash_bandas(dame_minhashes_shingles(dame_shingles_words(x[1],3,15)))))
learn = learn.flatMap(lambda x: flatmapeo(x[0], x[1]))
learn = learn.map(lambda x: (x[1],x[0]))
flatmapeado = learn.count()
learn = learn.groupByKey()
agrupado = learn.count()
learn = learn.filter(lambda x: len(x[1].data) >= minClusterSize)
filtrado = learn.count()
dicto = learn.collectAsMap()

#learn.saveAsTextFile('/media/tino/Tera/bigdata/outtest.csv')
#learn = learn.flatMap(lambda x: (x[0], x[1]))
#learn = learn.map(lambda x: (x[1], x[0], 1))
#learn.saveAsTextFile('/media/tino/Tera/bigdata/finalout.csv')

test = sc.textFile('/media/tino/Tera/bigdata/parsedTest.csv',8)
test = test.map(lambda x: x.split('|')).map(lambda x: (x[0],x[2], dame_hash_bandas(dame_minhashes_shingles(dame_shingles_words(x[1],3,15)))))
test = test.map(lambda x: (x[0],x[1], calcScore(x[2])))

try:
	shutil.rmtree('/media/tino/Tera/bigdata/scores.csv')
except:
	print "asd"
test = test.filter(lambda x: x[2] != -1)
test.saveAsTextFile('/media/tino/Tera/bigdata/scores.csv')

print "FLATMAPEADO: " + str(flatmapeado)
print "AGRUPADO: " + str(agrupado)
print "FILTRADO: " + str(filtrado)
print str(type(dicto))


