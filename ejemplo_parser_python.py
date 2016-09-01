#encoding=latin-1
ARCHIVO_MATERIAS = "materias_informatica.csv"
ARCHIVO_APROBADAS = "aprobadas.csv"

import csv

#Opción 1: Marcar materia como aprobada.

def agregar_aprobada(codigo):
    '''Agrega un código al final de un archivo ARCHIVO_APROBADAS con un caracter de salto de línea. De 
    no existir lo crea, y al final lo cierra.'''
    aprobadas = open(ARCHIVO_APROBADAS, "a")
    aprobadas.write(codigo + '\n')
    aprobadas.close()


def diccionario_cat1_cats(lista_dic, cat1, cats):
    '''Devuelve un diccionario cuyas claves son los valores de cada diccionario en lista_dic correspondientes
    a la clave cat1. Y sus valores son listas de los valores correspondientes a lista_dic según las claves en cats.
    Estos valores se convierten a entero si su correspondiente valor en cats es "créditos", y si este es "correlativas",
    entonces se convierten a lista.'''
    dic_cat1_cats = {}
    for dic in lista_dic:
        dic_cat1_cats [dic [cat1]] = []
        for cat in cats:
            if cat == "créditos":
                dic_cat1_cats [dic [cat1]].append(int(dic [cat]))
            elif cat == "correlativas":
                if "-" in dic [cat]:
                    dic_cat1_cats [dic [cat1]].append(dic [cat].split("-"))
                else:
                    dic_cat1_cats [dic [cat1]].append([dic [cat]])
            else:
                dic_cat1_cats [dic [cat1]].append(dic [cat])
    return dic_cat1_cats

def pedir_codigo_aprobada(set_aprobadas, dic_materias):
    '''Pide el código de una materia, si esta no está en el diccionario de todas las materias disponibles
    (dic_materias), o si está en el set de aprobadas (set_aprobadas), entonces lo sigue pidiendo 
    hasta obtener un dato válido. Cuando lo obtiene, devuelve el código y la materia correspondiente según
    el primer diccionario. Si se ingresa "*", se devuleve dos valores None.'''
    codigo = raw_input("Ingrese el código de la materia, * para salir: ")
    while codigo not in dic_materias or codigo in set_aprobadas:
        if codigo == "*":
            return None, None
        codigo = raw_input("Código inválido: ")
    return codigo, dic_materias [codigo] [0]

def marcar_materia_aprobada(set_aprobadas, dic_materias):
    '''Pide un código de materia hasta que esta esté en el dic_materias y no está en set_aprobadas, 
    cuando lo tiene, lo imprime junto con la materia correspondiente, y pide que se confirme si se quiere 
    agregar la materia como aprobada a ARCHIVO_APROBADAS. Si se acepta, la agrega, sino, se sigue pidiendo 
    un código de materia. Si el código es None, se devuelve None. Además, agrega el código a set_aprobadas.'''
    while True:
        codigo, materia = pedir_codigo_aprobada(set_aprobadas, dic_materias)
        if codigo == None:
            return
        print codigo, " - ", materia
        confirmar = raw_input("¿Marcar materia como aprobada? [s/n]: ").lower()
        while confirmar != "s" and confirmar != "n":
            confirmar = raw_input("¿Marcar materia como aprobada? [s/n]: ").lower()
        if confirmar == "s":
            break
        if confirmar == "n":
            continue
    agregar_aprobada(codigo)
    set_aprobadas.add(codigo)

#Opción 2: Ver la cantidad de materias y créditos.

def mostrar_aprobadas_y_creditos(set_aprobadas, dic_materias):
    '''A partir de los códigos en set_aprobadas, imprime cada código, la materia correspondiente y los
    códigos correspondientes (dados por dic_materias), y al final imprime la suma de todos 
    los créditos.'''
    creditos_totales = 0
    for codigo in set_aprobadas:
        print codigo + " - " + dic_materias [codigo] [0] + " " * (60 - len(dic_materias [codigo] [0])) + str(dic_materias [codigo] [1]) + "c"
        creditos_totales += dic_materias [codigo] [1]
    print "Total:" + " " * 60 + str(creditos_totales) + "c"

#Opción 3: Ver materias habilitadas para cursar.

def mostrar_materias_habilitadas(set_aprobadas, dic_materias):
    '''Imprime los códigos, nombres y créditos de todas las materias habilitadas a cursar
    (obtenidas de dic_materias), a partir de verificar que los codigos de las materias 
    correlativas (obtenidos de dic_materias también) se encuentren en el set_aprobadas, y además si estos 
    códigos de aprobadas están en esa lista de materias habilitadas, no los agrega.'''
    lista_habilitadas = []
    for codigo in dic_materias:
        if codigo in set_aprobadas:
            continue
        correlativas = dic_materias [codigo] [2]
        if len(correlativas) < 1 or correlativas [0] == "":
            lista_habilitadas.append(codigo)
        else:
            for correlativa in correlativas:
                if correlativa not in set_aprobadas:
                    break
            else:
                lista_habilitadas.append(codigo)
    for codigo in lista_habilitadas:
        print codigo, " - ", dic_materias [codigo] [0], " " * (60 - len(dic_materias [codigo] [0])), str(dic_materias [codigo] [1]) + "c"

#Opción 4: Ver información detallada de una materia.

def mostrar_informacion_materia(set_aprobadas, dic_materias):
    '''Pide un codigo de materia, el cual debe estar en dic_materias, e imprime un mensaje con todos los detalles del mismo
    según la lista correspondiente a la clave código ingresado en dic_materias: nombre, cantidad de créditos si está aprobada
    o no, y sus correlativas, diciendo si estas están aprobadas o no (según set_aprobadas).'''
    codigo = raw_input("Ingrese el código de una materia, : ")
    while codigo not in dic_materias:
        codigo = raw_input("Código no válido: ")
    print codigo, " - ", dic_materias [codigo] [0]
    print dic_materias [codigo] [1], " créditos"
    if codigo in set_aprobadas:
        aprobada = "si"
    else:
        aprobada = "no"
    print "Aprobada: ", aprobada, "\n"
    print "Correlativas:"
    correlativas = dic_materias [codigo] [2]
    if len(correlativas) < 1 or correlativas [0] == "":
        print "No tiene"
    else:
        for correlativa in correlativas:
            if correlativa in set_aprobadas:
                print correlativa, " - ", dic_materias [correlativa] [0], " " * (60 - len(dic_materias [correlativa] [0])), str(dic_materias [correlativa] [1]) + "c (aprobada)"
            else:
                print correlativa, " - ", dic_materias [correlativa] [0], " " * (60 - len(dic_materias [correlativa] [0])), str(dic_materias [correlativa] [1]) + "c"

#Opción 5: Salir.

def salir(a, b):
    '''Termina la ejecución del programa, se le pasan dos parámetros, los cuales pueden ser cualquier cosa.'''
    exit()

#Main: Ejecución del programa.

def main():
    '''Se intenta abrir ARCHIVO_MATERIAS, en caso de haber algún error, se imprime un mensaje y se sale del programa.
    Una vez abierto, se intenta leer el archivo en formato csv, en caso de haber algún problema, se imprime un 
    mensaje y se sale del programa, antes cerrando el archivo. El archivo csv debe tener encabezados "código", "nombre",
    "créditos", "correlativas", donde los valores de "créditos" son enteros y "correlativas" son códigos. Se intenta abrir
    ARCHIVO_APROBADAS, en caso de haber algún error o no poseer encabezado "código", lo borra y crea uno nuevo con ese
    encabezado, lo mismo sucede si hay algún error durante la apertura del mismo. Luego se intenta leer el archivo en formato
    csv, en caso de haber algún problema se imprime un mensaje de error y se sale del programa, antes cerrando el archivo. Se
    imprime una serie de 5 opciones que representan distintas acciones, y se pide al usuario que ingrese una válida, en caso 
    contrario sigue pidiendo el ingreso. Si se elige "1" se ejecuta marcar_materia_aprobada, "2" se ejecuta mostrar_aprobadas_y_creditos,
    "3" se ejecuta mostrar_materias_habilitadas, "4" mostrar_informacion_materia, "5" salir.'''
    try:
        archivo = open(ARCHIVO_MATERIAS, "r")
    except IOError as e:
        print "Error al abrir el archivo de materias, más información: ", e
        return
    try:
        arch_csv = csv.DictReader(archivo)
        materias = list(arch_csv)
    except IOError as e:
        print "Error al leer el archivo de materias, más información: ", e
        return
    finally:
        archivo.close()
    dic_materias = diccionario_cat1_cats(materias, "código", ("nombre", "créditos", "correlativas"))
    try:
        aprobadas = open(ARCHIVO_APROBADAS, "r")
        if aprobadas.readline() != "código\n":
            aprobadas.close()
            aprobadas = open(ARCHIVO_APROBADAS, "w")
            aprobadas.write("código\n")
        aprobadas.close()
    except IOError as exc:
            print "Sucedió un error al intentar abrir el archivo de aprobadas, se creará uno; más información: ", exc
            aprobadas = open(ARCHIVO_APROBADAS, "w")
            aprobadas.write("código\n")
            aprobadas.close()
    try:
        aprobadas = open(ARCHIVO_APROBADAS, "r")
    except IOError as e:
        print "Error al intentar abrir el archivo de aprobadas, más información: ", e
        return
    try:
        aprobadas.readline()
        set_aprobadas = set()
        for codigo in csv.reader(aprobadas):
            set_aprobadas.add("".join(codigo))
    except IOError as exc:
        print "Error al leer el archivo de aprobadas, más información: ", exc
        return
    finally:
        aprobadas.close()
    diccionario_opciones = {"1" : marcar_materia_aprobada, "2" : mostrar_aprobadas_y_creditos, "3" : mostrar_materias_habilitadas, "4" : mostrar_informacion_materia, "5" : salir}
    while True:
        print "1 - Marcar una materia como aprobada."
        print "2 - Ver las materias aprobadas y la cantidad de créditos."
        print "3 - Ver las materias habilitadas para cursar."
        print "4 - Ver información detallada de una materia."
        print "5 - Salir."
        opcion = raw_input("Elija una acción a realizar: ")
        while opcion not in diccionario_opciones:
            opcion = raw_input("Acción no válida: ")
        try:
            diccionario_opciones [opcion](set_aprobadas, dic_materias)
        except IOError as ex:
            print "Hubo algún error en la ejecución de los archivos, más información: ", ex
            return None
        except MemoryError:
            print "Error de memoria."
            return None
        
main()