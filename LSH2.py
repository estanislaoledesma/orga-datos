#encoding=latin-1
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
	'''Devuelve una lista con los shingles de texto, procesado caracter por caracter, donde los mismos tienen
	un tamaño de cantidadChars.'''
	return [texto[i:i + cantidadChars] for i in range(len(texto) - cantidadChars + 1)]

def dame_shingles_words(texto, cantidadPalabras, maxLargoPalabra):
	'''Devuelve una lista con los shingles de texto, procesado palabra por palabra, donde los mismos tienen un 
	tamaño de cantidadChars y un máximo de largo de palabra igual a maxLargoPalabra.'''
	texto = texto.split()
	return [texto[i:i+cantidadPalabras] for i in range(len(texto) - cantidadPalabras + 1) if len(texto[i]) < maxLargoPalabra]

def dame_minhashes_shingles(shingles):
	'''Devuelve una lista de minhashes para la lista de shingles pasada por parámetro, utiliza hash and displace.'''
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
	'''Devuelve una lista de hashes de banda a partir de una lista de minhashes en la cual hay un número de bandas
	y un número de hashesPorBanda para la misma.'''
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
	'''Devuelve una lista de hashes de banda a partir de una lista de minhashes en la cual hay un número de bandas
	y un número de hashesPorBanda para la misma.'''
	codigosBandas = []
	hashBanda = 0
	intervalo = 0
	for i in range(cantHashes):
		intervalo = intervalo + 1
		if (intervalo == hashesPorBanda):
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
	'''Genera los shingles de texto, los minhashes y luego los hashes de cada banda. Por cada hash de banda,
	lo agrega al diccionario valoresPorBanda y le asigna a cada uno una lista donde el valor en la misma es 
	rating.'''
	if shingleaWords:
		codigosBandas = dame_hash_bandas(dame_minhashes_shingles(dame_shingles_words(texto, 2, 15)))
	else:
		codigosBandas = dame_hash_bandas(dame_minhashes_shingles(dame_shingles_chars(texto, 5)))
	for codigo in codigosBandas:
		valoresPorBanda.setdefault(codigo, [])
		valoresPorBanda[codigo].append(rating)


def flatmapeo(puntaje, array):
	'''Devuelve una lista donde cada posición es una tupla donde la primera posición es puntaje y la segunda,
	el valor de la misma posición en array.'''
	lista = []
	for x in array:
		lista.append((puntaje,x))
	return lista


def damePromedioBucket(indiceDict):
	'''Devuelve el promedio del bucket dado por la clave indiceDict en dicto (un diccionario), donde el valor
	de la misma es una lista de enteros. En caso de error, devuelve 0.'''
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
	'''Devuelve el promedio del promedio de cada banda en bandas (lista de bandas), donde el primer promedio
	se calcula mediante damePromedioBucket de la banda (en el diccionario dicto), y el segundo es un promedio
	de todos esos promedios. En caso de error, devuelve -1.'''
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


