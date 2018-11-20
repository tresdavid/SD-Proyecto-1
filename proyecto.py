#!/usr/bin/env python
"""
Parallel Hello World
"""

from mpi4py import MPI
import sys
import os
import numpy as np

#Importar libreria para manejo de ip

from geoip2.errors import AddressNotFoundError
import geoip2.database

#Variables globales

comm = MPI.COMM_WORLD
size = MPI.COMM_WORLD.Get_size()
rank = MPI.COMM_WORLD.Get_rank()
name = MPI.Get_processor_name()
req = MPI.Request
file_name = ""
#----PRUEBA IP
reader = geoip2.database.Reader('GeoLite2-Country_20181023/GeoLite2-Country.mmdb')

#Este metodo calcula la linea de inicio para cada nodo
def set_file_begin(file_name,rank,size):
	desc = open(file_name,'r')
	count_lines = len(desc.readlines())
	desc.close()

	print "Cantidad de logs: ", count_lines

	wload = count_lines / size
	begin = rank * wload
	rest = count_lines % size

	return [begin,wload,rest]


def find_warns():

	with open(file_name, 'r+') as file_d:
        	walk = 0

		#Saltar lineas que no le correspondan al nodo
        	while walk < begin:
            		line = file_d.readline()
            		walk = walk + 1

		#Leer lineas

		warn_count = 0
		dictionary = {}
		while walk < end:

			line = file_d.readline()

			#if len(line.split()) < 3:
			#	print "ESTA ----- ",line.split()

			if line.split()[2] == "WARN":
				warn_count += 1

				#-------- PRUEBA DE IP --------
				if(len(line.split()) < 10):
					print "Peligro: ",line.split()

				if(line.split()[9] != "protocol=soap;"):
					cadena = line.split()[4]

					oip = cadena.split(";")
					#print oip
					oip1 = oip[1].replace("oip=","")
					#oip1 = oip[1][:-2]

					try:
						response = reader.country(oip1) #Hacer esto al tener la ip
			#			print("{}: {}".format(oip1, response.country.name)) #El segundo parametro te da el nombre del pais

						#Agregar el pais al dictionary
						add_country_count(dictionary,response.country.name)

					except AddressNotFoundError:
            					print("{}: {}".format(oip1, 'NOOOOOOOOOOOOO'))
#					except:
#						print "Otro error, seguro en el dictionary"

			walk += 1

		#Si hubo resto, que el ultimo nodo se encargue de procesarla

		if rank == size-1 and rest > 0:
			line = file_d.readline()
			print "Hubo resto"
                        if line.split()[2] == "WARN":
                                warn_count += 1

		print "Proceso: ",rank,", Cantidad de warns: ",warn_count



		return [warn_count,dictionary]

""" Dado un diccionario y un pais, si el pais no existe lo agrega, si existe aumenta su contador """

def add_country_count(dictionary,country_name):

	#Si existe actualizar el valor si no, crear una nueva clave con un valor de 1

	if (dictionary.get(country_name) != None):
		dictionary[country_name] += 1
	else:
		dictionary[country_name] = 1


""" Enviar el diccionario de ip """

def send_or_recieve_results_1(dictionary):

	data = dictionary
	diccionario_completo = comm.gather(data,root=0)

	if(rank==0):
		resultado = {}
		#Sumar los diccionarios
		for dict in diccionario_completo:

			for key,value in dict.items():

				if( resultado.get(key) != None):
					resultado[key] += value
				else:
					resultado[key] = value
		l = resultado.items()
		l.sort(key= lambda x: x[1])
		l.reverse()
		print "Pais                Ocurrencias"
		print "---------          ---------------"
		i = 0
		if (len(l) <= 20):
			i = len(l)
		else:
			i = 20

		for row in l[:i]:
			for column in row:
				print column, "          ",
			print ""

		print "---- El resultado fue: ",resultado

""" Enviar datos al nodo maestro"""

def send_or_recieve_results(warns_count):

	value = np.array(warns_count,'d')
	value_sum  = np.array(0.0,'d')
	comm.Reduce(value, value_sum, op=MPI.SUM, root=0)

	if(rank == 0):
		print "---- Cantidad total de warnings despues del reduce: ",value_sum


if __name__ == '__main__':

	file_name = ".//logs//audit.log.2018-10-02"

	begin,wload,rest = set_file_begin(file_name,rank,size)
	end = begin + wload

	print "---- Proceso: ",rank,", inicio de archivo: ",begin,", carga de trabajo: ",wload,", fin: ",end,", resto: ",rest," ----"
	warns_count,dictionary = find_warns()
	send_or_recieve_results(warns_count)
	send_or_recieve_results_1(dictionary)
