
import csv

links = set()
numWritten = 0

#read all links from the pending-links file.
with open('pending-links.csv', newline='') as csvfile:
	reader = csv.reader(csvfile, delimiter=' ', quotechar='|')
	for row in reader: 
		links.add(row[0])

#open the visited links file and grab existing links
with open('visited-links.csv', newline='') as csvfile:
	reader = csv.reader(csvfile, delimiter=' ', quotechar='|')
	for row in reader: 
		links.add(row[0])

#write the new visited links file
with open('visited-links.csv', 'w', newline='') as csvfile:
    writer = csv.writer(csvfile, delimiter=' ',quotechar='|', quoting=csv.QUOTE_MINIMAL)
    for url in links:
        writer.writerow([url])
        numWritten = numWritten + 1

#trucate the pending links file
#with open('pending-links.csv', "w"):
#	pass

print('Written: ' + str(numWritten) + ' to visited-links.csv.')     
