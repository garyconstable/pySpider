

import csv
links = set()
numWritten = 0


'''
read all links from the pending-links file.
pending links that have been created in the last session
'''
with open('csv/session-links.csv', newline='') as csvfile:
	reader = csv.reader(csvfile, delimiter=' ', quotechar='|')
	for row in reader: 
		links.add(row[0])


'''
open the visited links file and grab existing links
currently this is the master list of links
'''
with open('csv/visited-links.csv', newline='') as csvfile:
	reader = csv.reader(csvfile, delimiter=' ', quotechar='|')
	for row in reader: 
		links.add(row[0])


'''
write the combination of the two lists to the file
'''
with open('csv/visited-links.csv', 'w', newline='') as csvfile:
    writer = csv.writer(csvfile, delimiter=' ',quotechar='|', quoting=csv.QUOTE_MINIMAL)
    for url in links:
        writer.writerow([url])
        numWritten = numWritten + 1


'''
truncate the pending links file
'''
with open('csv/session-links.csv', "w"):
   pass

print('Written: ' + str(numWritten) + ' to visited-links.csv.')  


'''
write the combination of the two lists to the file
'''

tmp = set()
numWritten = 0

with open('csv/horizon.csv', newline='') as csvfile:
    reader = csv.reader(csvfile, delimiter=' ', quotechar='|')
    for row in reader: 
        tmp.add(row[0])

with open('csv/horizon.csv', 'w', newline='') as csvfile:
    writer = csv.writer(csvfile, delimiter=' ',quotechar='|', quoting=csv.QUOTE_MINIMAL)
    for url in tmp:
        writer.writerow([url])
        numWritten = numWritten + 1

print('Written: ' + str(numWritten) + ' to horizon.csv.')  


