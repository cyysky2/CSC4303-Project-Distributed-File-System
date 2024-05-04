# directory service
import os
import csv      #To work with csv file
from socket import *

serverPort = 9090
serverSocket = socket(AF_INET,SOCK_STREAM)
serverSocket.bind(('localhost', serverPort))
serverSocket.listen(10)
print ('DIRECTORY SERVICE is ready to receive...')

def check_mappings(client_msg, list_files):

	filename = client_msg.split('|')[0]
	RW = client_msg.split('|')[1]

	with open("file_mappings.csv",'rt') as infile:        # open the .csv file storing the mappings
		d_reader = csv.DictReader(infile, delimiter=',')    # read file as a csv file, taking values after commas
		header = d_reader.fieldnames    	# skip header of csv file
		file_row = ""
		for row in d_reader:
			if list_files == False:
				# use the dictionary reader to read the values of the cells at the current row
				user_filename = row['user_filename']
				primary_copy = row['primary']

				if user_filename == filename and RW == 'w':		# check if file inputted by the user exists	(eg. file123)
					print("WRITING")
					actual_filename = row['actual_filename']	# get actual filename (eg. file123.txt)
					server_addr = row['server_addr']			# get the file's file server IP address
					server_port = row['server_port']			# get the file's file server PORT number

					print("actual_filename: " + actual_filename)
					print("server_addr: " + server_addr)
					print("server_port: " + server_port)

					return actual_filename + "|" + server_addr + "|" + server_port	# return string with the information on the file

				elif user_filename == filename and RW == 'r' and primary_copy == 'no':
					print("READING")
					actual_filename = row['actual_filename']	# get actual filename (eg. file123.txt)
					server_addr = row['server_addr']			# get the file's file server IP address
					server_port = row['server_port']			# get the file's file server PORT number

					print("actual_filename: " + actual_filename)
					print("server_addr: " + server_addr)
					print("server_port: " + server_port)

					return actual_filename + "|" + server_addr + "|" + server_port	# return string with the information on the file

			else:
				user_filename = row['user_filename']
				file_row = file_row + user_filename +  "\n"		# append filename to return string
		if list_files == True:
			return file_row		
	return None 	# if file does not exist return None

def add_file_in_csv(client_msg):
	user_filename = client_msg.split('|')[0]

	with open("file_mappings.csv",'rt') as infile:
		d_reader = csv.DictReader(infile, delimiter=',')
		for row in d_reader:
			if user_filename == row['user_filename']:
				print("Creation failed. File already exist.")
				return None

	with open("file_mappings.csv",'a', newline='') as infile:
		writer = csv.writer(infile)
		actual_filename = user_filename+'.txt'
		# if file does not exist, creat. 
		rowA = [user_filename, actual_filename, 'localhost', '12001', 'yes']
		rowB = [user_filename, actual_filename, 'localhost', '12002', 'no']
		rowC = [user_filename, actual_filename, 'localhost', '12003', 'no']
		writer.writerow(rowA)
		writer.writerow(rowB)
		writer.writerow(rowC)

	return actual_filename + "|localhost|12001" 

def del_file_in_csv(client_msg):
	filename = client_msg.split('|')[0]
	new_csv_row = []
	file_exist = False
	new_csv_row.append(["user_filename","actual_filename","server_addr","server_port","primary"])
	with open("file_mappings.csv",'rt') as infile:
		d_reader = csv.DictReader(infile, delimiter=',')
		for row in d_reader:
			user_filename = row['user_filename']
			actual_filename = row['actual_filename']
			server_addr = row['server_addr']			
			server_port = row['server_port']
			primary_copy = row['primary']
			if filename == user_filename:
				file_exist = True
				continue
			new_csv_row.append([user_filename, actual_filename, server_addr, server_port, primary_copy])

	with open("file_mappings.csv", 'w', newline='') as file:
		writer = csv.writer(file)
		writer.writerows(new_csv_row)

	return file_exist


def main():

	while 1:
		connectionSocket, addr = serverSocket.accept()

		response = ""
		recv_msg = connectionSocket.recv(1024)
		recv_msg = recv_msg.decode()

		if recv_msg.split('|')[1] == 'new':
			print("Creating new file.")
			response = add_file_in_csv(recv_msg)
			if response is None:
				response = "FILE_ALREADY_EXIST"
			connectionSocket.send(response.encode())	# send the file information or non-existance message to the client		
			connectionSocket.close()

		elif recv_msg.split('|')[1] == 'del':
			file_exist = del_file_in_csv(recv_msg)
			if file_exist is False:
				response = "FILE_DOES_NOT_EXIST"
				print("The file does not exist to be deleted. Abort.")
			else:
				response = recv_msg.split('|')[0] + ".txt|localhost|12001" 
			connectionSocket.send(response.encode())	# send the file information or non-existance message to the client		
			connectionSocket.close()

		else:	
			if "LIST" not in recv_msg:
				response = check_mappings(recv_msg, False)		# check the mappings for the file
			elif "LIST" in recv_msg:
				response = check_mappings(recv_msg, True)

			# check mapping respose check. 
			if response is not None:	# for existance of file
				response = str(response)
				print("RESPONSE: \n" + response)
				print("\n")
			else:
				response = "FILE_DOES_NOT_EXIST"
				print("RESPONSE: \n" + response)
				print("\n")
			connectionSocket.send(response.encode())	# send the file information or non-existance message to the client
			connectionSocket.close()


if __name__ == "__main__":
	main()