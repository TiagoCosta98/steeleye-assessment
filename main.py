import requests
import xml.etree.ElementTree as ET
import zipfile
import csv
import boto3
import logging


def get_file_from_url(url: str, filename: str):
    """
    Download a file from a web link
    :param url: link of the page to be downloaded
    :param filename: name of the file in which to write the page contents
    :return: None
    """
    page = requests.get(url)
    open(filename, "wb").write(page.content)


def find_download_link(filename: str) -> {str, str}:
    """
    Parse the initial .xml file to find the download link and the file name of the first DLTINS type file
    :param filename: name of the .xml file containing the data to be parsed
    :return: {download link, file name}
    """
    xmltree = ET.parse(filename)
    root = xmltree.getroot()
    result = root.find('result')
    if result is None:
        logging.warning('Result element not found')
    for doc in result.iter('doc'):
        typeelem = doc.find(".//str[@name='file_type']")
        if typeelem.text == 'DLTINS':
            logging.debug('DLTINS file found')
            download_link = doc.find(".//str[@name='download_link']").text
            file_name = doc.find(".//str[@name='file_name']").text
            return download_link, file_name


def unzip(filename: str):
    """
    Unzip a .zip file
    :param filename: name of the .zip file to be unzipped
    :return: None
    """
    file = zipfile.ZipFile(filename, "r")
    file.extractall()


def write_csv_file(csv_filename: str, xml_filename: str):
    """
    Write the required information from the DTLINS type .xml file into a .csv file
    :param csv_filename: Name of the .csv file in which to write the data
    :param xml_filename: Name the .xml where the data originaly is
    :return: None
    """
    csv_file = open(csv_filename, 'w', encoding='utf-8', newline='')
    csv_writer = csv.writer(csv_file)
    csv_writer.writerow(["FinInstrmGnlAttrbts.Id", "FinInstrmGnlAttrbts.FullNm", "FinInstrmGnlAttrbts.ClssfctnTp",
                         "FinInstrmGnlAttrbts.CmmdtyDerivInd", "FinInstrmGnlAttrbts.NtnlCcy", "Issr"])
    xml_tree = ET.parse(xml_filename)
    root = xml_tree.getroot()
    for record in root.iter("{urn:iso:std:iso:20022:tech:xsd:auth.036.001.02}TermntdRcrd"):
        if record is None:
            logging.warning('Data record not found')
        data = get_line_contents(record)
        csv_writer.writerow(data)
    csv_file.close()


def get_line_contents(elem: ET) -> list:
    """
    Extract the required data from a single tree element
    :param elem: element containing the data to be extracted
    :return: list containing the required data [FinInstrmGnlAttrbts.Id, FinInstrmGnlAttrbts.Id,
    FinInstrmGnlAttrbts.FullNm, FinInstrmGnlAttrbts.ClssfctnTp, FinInstrmGnlAttrbts.CmmdtyDerivInd,
    FinInstrmGnlAttrbts.NtnlCcy, Issr]
    """
    return [elem[0][0].text, elem[0][1].text, elem[0][3].text, elem[0][5].text, elem[0][4].text, elem[1].text]


def file_upload(filename: str, bucketname: str, uploaded_filename: str):
    """
    Upload a file to an AWS s3 bucket
    :param filename: name of the file to be uploaded
    :param bucketname: name of the bucket to where the file is uploaded
    :param uploaded_filename: name the file should have in the bucket
    :return: None
    """
    s3 = boto3.client("s3")
    s3.upload_file(Filename=filename, Bucket=bucketname, Key=uploaded_filename)


if __name__ == '__main__':
    logging.basicConfig(filename='example.log', encoding='utf-8', level=logging.DEBUG)  # Setup logging
    # Link from the requirements file
    link = "https://registers.esma.europa.eu/solr/esma_registers_firds_files/select?q=*&fq=publication_date:%5B2021" \
           "-01-17T00:00:00Z+TO+2021-01-19T23:59:59Z%5D&wt=xml&indent=true&start=0&rows=100"
    xml_name = "filelist.xml"
    get_file_from_url(link, xml_name)  # Download initial .xml file from the link
    dl_link, dltins_name = find_download_link(xml_name)  # Get the filename and download link from the initial .xml file
    zip_name = "xmlzip.zip"
    get_file_from_url(dl_link, zip_name)  # Download the .zip file from the download link
    unzip(zip_name)  # Unzip it
    dltins_name = dltins_name.split(".")[0] + ".xml"  # Name of the .xml file taken from the .zip file
    target_filename = "final.csv"
    write_csv_file(target_filename, dltins_name)  # Transfer required information to a .csv file
    bucket_name = "myassessmentbucket"
    file_upload(target_filename, bucket_name , "data.csv")  # Upload .csv file to a pre-made AWS s3 bucket

