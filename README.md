SWORD2DC
========

Utility to batch upload BioMed Central (BMC) SWORD deposited objects from Fedora Commons into DigitalCommons@WayneState.


<h3>Dependencies:</h3>
<ul>
	<li>PDFMiner - http://www.unixuser.org/~euske/python/pdfminer/</li>
	<li>Python lxml - http://lxml.de/</li>
</ul>

<h3>Overview:</h3>
<p>Because Digital Commons (DC) does not offer a SWORD service to receive article deposits, we created a utility to recieve these deposits in Fedora Commons, parse the deposit and extract metadata, and prepare a CSV for batch uploading to DC.  The process is almost entirely automated, with the exception of a couple fields for the DC batch upload sheet.<p>
<p>The overall workflow is as follows:</p>
<ul>
	<li>Fedora Commons SWORD server periodically accepts deposits from BMC, with metadata, article XML, and the PDF documents as part of the submission package (METSDSpaceSIP format)</li>
	<li>We run this utility to create a CSV sheet with all objects from the Fedora collection "wayne:collectionBMC" that have NOT yet been uploaded to DC</li>
		<ul><li>Currently timestamp based, moving to local ontology RDF statement "uploadedToDC" either being True/False</li></ul>
	<li>Insert this CSV into customized DC batch upload Excel sheet, primary differences being a 75 author span to accomdate articles with many authors</li>
	<li>Run utility that confirms uploads were successful, preventing them from showing up on future CSV's for upload</li>	
</ul>

<h3>To Run:</h3>

<h3>Post Script Steps before Ingest into DC:</h3>
<!-- the DC fields <strong>"disciplines"</strong> and <strong>"author#_institutions"</strong>   -->