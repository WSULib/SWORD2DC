#Utility to batch upload SWORD deposited objects from Fedora Commons into DigitalCommons@WayneState.

from sensitive import *
import os
import sys
import xml.etree.ElementTree as ET
import urllib, urllib2
from string import Template
import time
import datetime
from lxml import etree
from pyPdf import PdfFileWriter, PdfFileReader
from StringIO import StringIO
from subprocess import call
import re
import codecs
import smtplib
from email.mime.text import MIMEText




###########################
#articleBlob Class
###########################
class articleBlob:
	toUpdate = []
	BMC_DC_label = ''
	meta = {}
	articleMetaList = []
	METSRoot = ''
	articleRoot = ''
	PID = ''
	now = datetime.datetime.now().isoformat()


###########################
# Open up security policies for new objects (they come in locked down)	
###########################
def openUpSecurity():
	pass


###########################
#Get Date of Last BioMed record extraction from Fedora
###########################
def getLastBioMedDate():	
	#evaluate solr response string as python dictionary
	LastBioMedDict = eval(urllib.urlopen("http://localhost/solr4/fedobjs/select?q=id%3ALastBioMedExtract&fl=last_modified&wt=python&indent=true").read())	
	LastBioMedDate = LastBioMedDict['response']['docs'][0]['last_modified']
	print "Last Extraction of BioMed records:",LastBioMedDate,"\n"
	return LastBioMedDate


###########################
#Get Objects/Datastreams modified on or after this date
###########################
def getToUpdate(LastFedoraIndexDate):
	#Pulls date last time Fedora was indexed in Solr
	risearch_query = "select $object from <#ri> where $object <fedora-view:lastModifiedDate> $modified and $modified <mulgara:after> '{LastFedoraIndexDate}'^^<xml-schema:dateTime> in <#xsd> and $object<info:fedora/fedora-system:def/relations-external#isMemberOfCollection> <info:fedora/wayne:collectionBMC>".format(LastFedoraIndexDate=LastFedoraIndexDate)	

	risearch_params = urllib.urlencode({
		'type': 'tuples', 
		'lang': 'itql', 
		'format': 'CSV',
		'limit':'',
		'dt': 'on', 
		'query': risearch_query
		})
	risearch_host = "http://{username}:{password}@localhost/fedora/risearch?".format(username=username,password=password)
	modified_PIDs = urllib.urlopen(risearch_host,risearch_params)	

	#push unique PIDs to toUpdate list
	iterPIDs = iter(modified_PIDs)	
	next(iterPIDs)	
	for PIDstring in iterPIDs:
		PIDproper = PIDstring.split("/")[1].rstrip()
		if PIDproper not in articleBlob.toUpdate:
			articleBlob.toUpdate.append(PIDproper)

	print "PIDs to update:",articleBlob.toUpdate,"\n"
	print "Total to update:",len(articleBlob.toUpdate),"\n"

	#exit if nothing to update
	if len(articleBlob.toUpdate) < 1:
		print "It does not appear any BioMed deposits have been made since last record extraction."
		sys.exit()



###########################
#load METS
###########################
def loadMETS():	
	response = urllib.urlopen("http://{username}:{password}@localhost/fedora/objects/{PID}/datastreams/SWORD-mets/content".format(PID=PID, username=username, password=password))
	METSXML = response.read()
	METSRoot = etree.fromstring(METSXML)
	
	#METS doc namespace map
	nsmap = {'epdcx': 'http://purl.org/eprint/epdcx/2006-11-16/','mets': 'http://www.loc.gov/METS/', 'MIOJAVI': 'http://purl.org/eprint/epdcx/2006-11-16/'}

	#get BMC DS label (resets global)	
	articleBlob.BMC_DC_label =  METSRoot.xpath('//epdcx:valueString[@epdcx:sesURI="http://purl.org/dc/terms/URI"]', namespaces=nsmap)[0].text.split('/')[-1]
	articleBlob.meta['BMC_DC_label'] = articleBlob.BMC_DC_label
	print "\n***************************************************\nWorking on:",articleBlob.BMC_DC_label,"\nBMC deposit label:",articleBlob.meta['PID_label_string'],"\n***************************************************"


###########################
#load article XML <arc>
###########################	
def loadArticleXML():
	response = urllib.urlopen("http://{username}:{password}@localhost/fedora/objects/{PID}/datastreams/SWORD-{BMC_DC_label}/content".format(PID=PID,BMC_DC_label=articleBlob.BMC_DC_label,username=username,password=password))	
	articleXML = response.read()	
	articleBlob.articleRoot = etree.fromstring(articleXML)
	

###########################
#load PDF
###########################
def loadPDF():
	
		
	fhand = urllib.urlopen("http://{username}:{password}@localhost/fedora/objects/{PID}/datastreams/SWORD-{BMC_DC_label}-2/content".format(PID=PID,BMC_DC_label=articleBlob.BMC_DC_label,username=username,password=password))	
	localFile = open('./tmp/{BMC_DC_label}.pdf'.format(BMC_DC_label=articleBlob.BMC_DC_label), 'w')
	localFile.write(fhand.read())
	localFile.close()

	#extract text to text file
	print "Extracting text from PDF:",articleBlob.BMC_DC_label
	call(['pdf2txt.py', '-o', './tmp/{BMC_DC_label}.txt'.format(BMC_DC_label=articleBlob.BMC_DC_label), './tmp/{BMC_DC_label}.pdf'.format(BMC_DC_label=articleBlob.BMC_DC_label)])
	call(['rm', './tmp/{BMC_DC_label}.pdf'.format(BMC_DC_label=articleBlob.BMC_DC_label)])

	#get keywords	
	fhand = open('./tmp/{BMC_DC_label}.txt'.format(BMC_DC_label=articleBlob.BMC_DC_label), 'r')
	article = fhand.read()	
	try:
		keywords = re.findall("Keywords:(.+?)\n\s*\n", article, re.S)[0].strip()
		keywords = re.sub("\n"," ",keywords)
		articleBlob.meta['keywords'] = keywords
		# print "Keywords:",keywords
		fhand.close()
	except:
		# print "Keywords: none found, set to empty"
		articleBlob.meta['keywords'] = ''

	#get citation
	fhand = open('./tmp/{BMC_DC_label}.txt'.format(BMC_DC_label=articleBlob.BMC_DC_label), 'r')
	article = fhand.read()
	try:
		citation = re.findall("Cite this article as:(.+?)Submit", article, re.S)[0].strip()
		citation = re.sub("\n"," ",citation)		
		articleBlob.meta['citation'] = citation	
		fhand.close()
	except:
		articleBlob.meta['citation'] = ""


###########################
#extract metadata
###########################
def extractMetadata():
	
	#title
	fm = articleBlob.articleRoot.find('fm')
	title = etree.tostring(fm.find('./bibl/title/p'))
	#remove newlines
	title = re.sub("\n"," ",title)
	#remove start / end p tags
	title = re.sub("(<p>|</p>)","",title)	
	articleBlob.meta['title_string'] = title.strip()

	#fulltetx_URL
	fulltext_URL = "http://silo.lib.wayne.edu/fedora/objects/{PID}/datastreams/SWORD-{BMC_DC_label}-2/content".format(PID=PID,BMC_DC_label=articleBlob.BMC_DC_label)
	articleBlob.meta['fulltext_URL'] = fulltext_URL

	#abstract
	abstract = etree.tostring(fm.find("./abs/sec"))
	abstract = re.sub('(\t|\n)',' ',abstract)
	abstract = re.sub("(<sec>|</sec>|<st>|</st>)","",abstract)
	articleBlob.meta['abstract'] = abstract.strip()

	#authors
	articleBlob.meta['authors'] = []		
	author_sets = fm.findall("./bibl/aug/au")	
	for author in author_sets:
		tempAuthDict = {}
		tempAuthDict['id'] = author.get('id')
		print tempAuthDict['id']
		
		#if non-individual authorship, returns "on_behalf"
		if author.get('type') == None:
			tempAuthDict['type'] = 'person'
		if author.get('type') != None:
			tempAuthDict['type'] = 'non-person'

		# print tempAuthDict['type']


		if tempAuthDict['type'] == "person":			
			#"affil" will allow us to pull in institution from metadata - UNFORMATTED
			# tempAuthDict['affil'] = author.find('insr').get('iid')
			try:
				iid = author.find('insr').get('iid')
				institution = etree.tostring(fm.find('./bibl/insg/ins[@id="{iid}"]/p'.format(iid=iid)))
				institution = re.sub('(\t|\n)',' ',institution)
				tempAuthDict['institution'] = institution
			except:
				tempAuthDict['institution'] = ""		
			try:
				tempAuthDict['snm'] = author.find('./snm').text
			except:
				tempAuthDict['snm'] = ""
			#try middle name initial, empty if vacant
			try:
				tempAuthDict['mi'] = author.find('./mi').text
			except:		
				tempAuthDict['mi'] = ''
			#try suffix, empty if vacant
			try:
				tempAuthDict['suf'] = author.find('./suf').text
			except:		
				tempAuthDict['suf'] = ''
			tempAuthDict['fnm'] = author.find('./fnm').text
			tempAuthDict['email'] = author.find('./email').text
			#push to authors list
			articleBlob.meta['authors'].append(tempAuthDict)
		
		elif tempAuthDict['type'] == "non-person":
			try:
				tempAuthDict['fnm'] = ""
				tempAuthDict['snm'] = author.find('./cnm').text
				#push to authors list
				articleBlob.meta['authors'].append(tempAuthDict)
			except:
				tempAuthDict['snm'] = ""

		else:
			print "Could not determine author type"

	#disciplines (cannot be done programatically)

	#doctype
	articleBlob.meta['doctype'] = "article"

	# #publication date
	articleBlob.meta['pubdate'] = fm.find('./bibl/pubdate').text


###########################
#get metadata from article XML
###########################
def createArticleMetadata(PID):
	#pull DC batch upload metadata from METS, article XML, and article PDF then push to temp meta dictionary	 	
	articleBlob.meta = {}

	#push PID to articleBlob.meta for this article
	articleBlob.meta['PID'] = PID

	#pulls in label for PID passed to function
	risearch_query = "select $label from <#ri> where <info:fedora/{PID}> <info:fedora/fedora-system:def/model#label> $label".format(PID=PID)
	risearch_params = urllib.urlencode({
	  'type': 'tuples', 
	  'lang': 'itql', 
	  'format': 'CSV',
	  'limit':'',
	  'dt': 'on', 
	  'query': risearch_query
	  })
	risearch_host = "http://{username}:{password}@localhost/fedora/risearch?".format(username=username,password=password)
	PID_label = urllib.urlopen(risearch_host,risearch_params)  
	iterLabel = iter(PID_label)  
	next(iterLabel)
	PID_label_string = iterLabel.read().strip()
	articleBlob.meta['PID_label_string'] = PID_label_string	

	#run extraction functions
	loadMETS() #sets articleBlob.BMC_DC_label
	articleRoot = loadArticleXML() #loads article XML
	loadPDF() #pulls in citation from PDF
	extractMetadata() #pulls metadata from article XML

	#push meta dictionary to list of article dictionaries
	articleBlob.articleMetaList.append(articleBlob.meta)

	###########################
	#print results for testing
	###########################
	# print articleBlob.articleMetaList
	# print articleBlob.meta['authors']

def cleanArticleBlob():

	print "\n\n***************************************************\nCleaning Article Blob...\n***************************************************\n"

	#recursive dictionary function
	def encodeUTF8(article,article_dict_element):
		try:		
			if article_dict_element == "authors":
				for author_dict in article[article_dict_element]:
					for key in author_dict:
						author_dict[key] = author_dict[key].encode('utf-8') 
			else:						
				article[article_dict_element] = article[article_dict_element].encode('utf-8')
		except:
			print "Problem with:",article['PID']

	#feeds keys to clean, recurses where necessary
	for article in articleBlob.articleMetaList:
		for key in article:
			encodeUTF8(article,key)

	print "finished."


###########################
# write meta dictionary to CSV file
###########################
def writeToCSV():

	print "\n\n***************************************************\nWriting articles metadata to CSV file\n***************************************************\n"	

	#check if articleMetaList empty...
	if len(articleBlob.articleMetaList) < 1:
		print "Nothing to write."
		return	
	
	#open file to write to	
	print "Output filename: ",articleBlob.CSV
	fhand = open(articleBlob.CSV,'w')
	

	#determine largest author load
	maxCount = []
	for article in articleBlob.articleMetaList:		
		maxCount.append(len(article['authors']))
	maxCount.sort(reverse=True)
	maxCountInt = maxCount[0]
	print "Authors maxCount list:",maxCount

	#iterate through articles and add to CSV
	for article in articleBlob.articleMetaList:

		#begin with new line to avoid halfies breaking the stride
		fhand.write("\n")

		try:
			#get number of authors and differenc from maxCountInt
			authCount = len(article['authors'])		
			authDiff = 75 - authCount #75 is hard coded into Excel sheet for max number of authors, can be increased
			print "Will create {authDiff} blank author records to maintain CSV integrity for {BMC_DC_label}".format(authDiff=str(authDiff),BMC_DC_label=article['BMC_DC_label'])

			#title
			fhand.write(article['title_string']+"\t")
			#fulltext_URL
			fhand.write(article['fulltext_URL']+"\t")
			#keywords
			fhand.write(article['keywords']+"\t")
			#abstract
			fhand.write(article['abstract']+"\t")		
			#authors
			for author in article['authors']:
				fhand.write(author['fnm']+"\t")
				fhand.write(author['mi']+"\t")
				fhand.write(author['snm']+"\t")
				fhand.write(author['suf']+"\t")
				fhand.write(author['email']+"\t")
				fhand.write(author['institution']+"\t")
				#author[num]iscorporation
				fhand.write("\t")
			#fill blank author slots, 6 \t's create an author chunk
			while authDiff > 0:
				fhand.write("\t\t\t\t\t\t\t")			
				authDiff = authDiff - 1
			#disciplines
			fhand.write("\t")
			#comments
			fhand.write("\t")
			#create_openurl
			fhand.write("\t")
			#custom_citation
			fhand.write(article['citation']+"\t")
			#doctype
			fhand.write("article\t")
			#publication date
			fhand.write(str(article['pubdate'])+"\t")

		#finally, new line and close	
		except:
			fhand_exceptions = open(articleBlob.exceptions,'a+')
			fhand_exceptions.write(str(article['PID'])+" - CSV writing\n")
			fhand_exceptions.close()
			continue
	

	#close file
	fhand.close()

###########################
# Update last batch BMC update to "NOW"
###########################
def updateLastBioMedDate():

	print "\n\n***************************************************\nUpdating 'LastBioMedExtract' date in Solr\n***************************************************\n"

	now = datetime.datetime.now().isoformat()
	print now	

	#Updated LastFedoraIndex in Solr
	updateURL = "http://localhost/solr4/fedobjs/update/?commit=true"
	dateUpdateXML = "<add><doc><field name='id'>LastBioMedExtract</field><field name='last_modified'>NOW</field></doc></add>"
	solrReq = urllib2.Request(updateURL, str(dateUpdateXML))
	solrReq.add_header("Content-Type", "text/xml")
	solrPoster = urllib2.urlopen(solrReq)
	response = solrPoster.read()
	solrPoster.close()
	print "SWORD deposits processed, batch upload sheet for DC created at:",now
	print response


###########################
# Send email notification
###########################
def sendEmail():	
	
	msg = MIMEText("******************************************************************************\n\n"+"SWORD deposits from BioMed Central have been harvested from Fedora\n"+"Project Output Directory: "+os.getcwd()+"/CSV_output\n"+"Successful Outputs Filename: "+articleBlob.now+"_output.csv\n"+"Failed / Exceptions Filename: "+articleBlob.now+"_exceptions.csv\n"+"Thanks for playing, see you next month.\n\n"+"******************************************************************************")	

	sender = "BioMed_SWORD_server@silo.lib.wayne.edu"
	# recipients = recipients_list #TESTING FOR A COUPLE WEEKS, THEN SEND TO libwebmaster@wayne
	msg['Subject'] = 'BioMed SWORD harvest - '+articleBlob.now
	msg['From'] = sender
	msg['To'] = ', '.join( recipients_list )

	# Send the message via our own SMTP server, but don't include the
	# envelope header.
	s = smtplib.SMTP('mail.wayne.edu')
	s.sendmail(sender, recipients_list, msg.as_string())
	s.quit()





###########################
#Go Time.
###########################

#Set output filenames
if os.path.dirname(__file__) != '':
	os.chdir(os.path.dirname(__file__))

articleBlob.exceptions = './CSV_output/'+articleBlob.now+'_exceptions.txt'
articleBlob.CSV = "./CSV_output/"+articleBlob.now+"_output.csv"


#analyze
openUpSecurity()
LastBioMedDate = getLastBioMedDate()
getToUpdate(LastBioMedDate)

#extract
totalToUpdate = len(articleBlob.toUpdate)
print "Total articles to process:",str(totalToUpdate)

for PID in articleBlob.toUpdate:	
	try:	
		createArticleMetadata(PID)
	except:
		print "Could not process article from PID",PID
		fhand_exceptions = open(articleBlob.exceptions,'a')
		fhand_exceptions.write(str(PID)+"\n")
		fhand_exceptions.close()
	print str(totalToUpdate)," / ",str(len(articleBlob.toUpdate)),"remaining to process."
	totalToUpdate = totalToUpdate - 1

#clean, write and update
cleanArticleBlob()
writeToCSV()
updateLastBioMedDate()
sendEmail()
print "finis."




