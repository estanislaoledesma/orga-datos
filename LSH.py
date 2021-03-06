#empiezo el approach por LSH
#DATASETS=== http://jmcauley.ucsd.edu/data/amazon/links.html
#https://nickgrattan.wordpress.com/2014/03/03/lsh-for-finding-similar-documents-from-a-large-number-of-documents-in-c/
#https://github.com/chrisjmccormick/MinHash/blob/master/runMinHashExample.py


from random import randint, seed, choice, random
from pyspark import SparkContext, SparkConf
import string
import sys
import itertools
import json
import gzip


primo = 1007
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
	for i in range(cantHashes): #Para cada función de hash
		minhash = maxint


		if shingleaWords: #Si usé shingles de palabras
			for conjunto in shingles:
				for shingle in conjunto: 
					shingle += hashDisplaces[i] #Obtengo un nuevo shingle a partir de un hashDisplace, para cambiar el valor de hash
					esteHash = abs(hash(shingle)) #Obtengo el hash del shingle
					if esteHash < minhash:
						minhash = esteHash #Si este hash es menor al minimo, se guarda
		else: #Si usé shingles de chars
			for shingle in shingles:
				shingle += hashDisplaces[i]
				esteHash = abs(hash(shingle))
				if esteHash < minhash:
					minhash = esteHash



		minhashes.append(minhash) #Guardo el minhash para la actual función de hash
	return minhashes

def dame_hash_bandas(minhashes):
	codigosBandas = []
	hashBanda = 0
	for i in range(cantHashes): #Para cada función de hash
		if (i % hashesPorBanda == 0 and i > 0): #Para diferenciar las bandas
			codigosBandas.append(hashBanda) #Guaro el nuevo hash de la banda
			hashBanda = 0
		hashBanda += hash(minhashes[i]) #Obtengo un nuevo hash para la banda como la suma de todos los hashes de la misma (Para comparar)
	codigosBandas.append(hashBanda) #Guardo los hashes de banda en una lista, para luego asociarlos a un valor de predicción
	return codigosBandas

def proc_texto_rating(texto, rating):
	if shingleaWords:
		codigosBandas = dame_hash_bandas(dame_minhashes_shingles(dame_shingles_words(texto, 2, 15)))
	else:
		codigosBandas = dame_hash_bandas(dame_minhashes_shingles(dame_shingles_chars(texto, 5)))
	for codigo in codigosBandas:
		valoresPorBanda.setdefault(codigo, [])
		valoresPorBanda[codigo].append(rating)


sc = SparkContext(conf = SparkConf())
rdd = sc.textFile('/media/tino/Tera/bigdata/elcsvTEST.csv',10)
#rdd = rdd.map(lambda x: x.split('|')).map(lambda x: (x[2], dame_shingles_words(x[1],2,15)))
rdd = rdd.map(lambda x: x.split('|')).map(lambda x: (x[2], dame_hash_bandas(dame_minhashes_shingles(dame_shingles_words(x[1],2,15)))))
#rdd.saveAsTextFile('/media/tino/Tera/bigdata/outtest.csv')
rdd = rdd.flatMap(lambda x: (x[0], x[1]))
rdd = rdd.map(lambda x: (x[1], x[0], 1))
rdd.saveAsTextFile('/media/tino/Tera/bigdata/outtest.csv')




