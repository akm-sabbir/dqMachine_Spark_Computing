def readfile():
	with open('partition_output','r') as datareader:
		data = datareader.readlines()
		collects = []
		counting = 0
		for datum in data:
			collects.append(int(datum.split(' ')[-1]))
			if int(datum.split(' ')[-1]) > 5000000:
				print str(datum.split(' ')[-1])
				counting += 1
	print "end of processing"
	print counting

if __name__ == '__main__':
	readfile()
		
