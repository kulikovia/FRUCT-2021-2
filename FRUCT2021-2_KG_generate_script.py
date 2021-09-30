import xml.etree.ElementTree as xml
import random
from random import randrange
from datetime import datetime
from datetime import timedelta

def random_date(start, end):
    """
    This function will return a random datetime between two datetime
    objects.
    """
    delta = end - start
    int_delta = (delta.days * 24 * 60 * 60) + delta.seconds
    random_second = randrange(int_delta)
    return start + timedelta(seconds=random_second)

d1 = datetime.strptime('3/1/21 00:00:00', '%m/%d/%y %H:%M:%S')
d2 = datetime.strptime('3/3/21 23:59:59', '%m/%d/%y %H:%M:%S')

#print(random_date(d1, d2))

Max_Hubs = 3
#Devices numbers: 1500 / 15000 / 45000
Max_Devices = 45000
#Users numbers: 1000 / 10000 / 30000
Max_Users = 30000
#Maximum event per RDF/XML file 1500 or 150000 according to Max_Devices value
Max_Step_1 = 15000
Max_Step_2 = 15000

SPARQL_path = "C:/Blazegraph/1"
Device_Users_Map_Seg1 = []
Device_Users_Map_Seg2 = []
Max_Traffic_Step = 100000

def createXML(filename):
    """
    Создаем XML файл.
    """

#Map Device-User creation
    for i in range(Max_Devices):
        Device_Users_Map_Seg1.append(random.randint(1, Max_Users))
        Device_Users_Map_Seg2.append(random.randint(1, Max_Users))


#Open SPARQL file
    spql = open("sparql_script.spql", "wt")

# Add header
    header = '''<?xml version="1.0"?>\n<rdf:RDF\nxmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"\nxmlns:vCard="http://www.w3.org/2001/vcard-rdf/3.0#"\nxmlns:geo="http://www.w3.org/2003/01/geo/"\nxmlns:net="http://purl.org/toco/"\nxmlns:tnmo="http://127.0.0.1/tnmo/"\nxmlns:TNSeg-1="http://127.0.0.1/TNSeg-1/"\nxmlns:TNSeg-2="http://127.0.0.1/TNSeg-2/"\n>\n'''
    print (header)

    # SEGMENT #1

    # Add Users  definitions
    FileNum = 0
    i = 1
    k = 1
    while i <= Max_Users:
        FileNum = FileNum + 1
        f = open(filename + "_users_segment_1_" + str(FileNum) + "_.nq", "at")
        f.write(header)
        f.write("\n<!--Users definitions-->\n")
        while k <= Max_Step_1:
            body = str('<rdf:Description rdf:about="http://127.0.0.1/TNSeg-1/User_') + str(i) + str('/">\n<TNSeg-1:ID>') + str(i) + str('</TNSeg-1:ID>\n<tnmo:sameAs><rdf:Description rdf:about="http://127.0.0.1/TNSeg-2/User_') + str(i) + str('/"></rdf:Description></tnmo:sameAs>\n</rdf:Description>\n')
            f.write(body)
            i = i + 1
            k = k + 1
        f.write("\n</rdf:RDF>\n")
        f.close()
        spql.write("\nLOAD <file:///" + str(SPARQL_path) + "/" + filename + "_users_segment_1_" + str(FileNum) + "_.nq>;\n")
        k = 1


    # Add Device  definitions
    FileNum = 0
    i=1
    k=1
    while i <= Max_Devices:
        FileNum = FileNum + 1
        f = open(filename + "_device_segment_1_" + str(FileNum) + "_.nq", "at")
        f.write(header)
        f.write("\n<!--Device definitions-->\n")
        while k <= Max_Step_1:
            body = str('<rdf:Description rdf:about="http://127.0.0.1/TNSeg-1/Device_') + str(i) + str('/">\n<net:hasMAC>MAC_Seg-1_') + str(i) + str('</net:hasMAC>\n</rdf:Description>\n')
            f.write(body)
            i=i+1
            k=k+1
        f.write("\n</rdf:RDF>\n")
        f.close()
        spql.write("\nLOAD <file:///" + str(SPARQL_path) + "/" + filename + "_device_segment_1_" + str(FileNum) + "_.nq>;\n")
        k=1

# Add Monitoring items definitions
        FileNum = 0
        i = 1
        k = 1
        l = 1
        while i <= Max_Devices:
            FileNum = FileNum + 1
            f = open(filename + "_monitoring_segment_1_" + str(FileNum) + "_.nq", "at")
            f.write(header)
            f.write("\n<!--Monitoring items definitions-->\n")
            while k <= Max_Step_2:
                device_num = i
                traffic_value = 0
                for ih in range(24):
                    for im in range(6):
                        traffic_value = traffic_value + random.randint(1, Max_Traffic_Step)
                        body = '''<rdf:Description rdf:about='http://127.0.0.1/TNSeg-1/Parameter_M_''' + str(l) + '''/'>\n<rdf:type>TNSeg-1:Parameter_M</rdf:type>\n<tnmo:parameter_timestamp rdf:datatype='http://www.w3.org/2001/XMLSchema#datetime'>2021-08-29 ''' + str(ih) + ':' + str(im) + '''0:00</tnmo:parameter_timestamp>\n<geo:Point geo:lat="55.701" geo:long="12.552"/>\n<tnmo:has_parameter_type>tnmo:device_state</tnmo:has_parameter_type>\n<tnmo:parameter_detailes>\n<rdf:Description>\n<rdf:type>rdf:statement</rdf:type>\n<rdf:predicat>tnmo:parameter_monitoring</rdf:predicat>\n<rdf:subject><rdf:Description rdf:about='http://127.0.0.1/TNSeg-1/User_''' + str(Device_Users_Map_Seg1[device_num-1]) + '''/'></rdf:Description></rdf:subject>\n<rdf:object rdf:datatype='http://www.w3.org/2001/XMLSchema#integer'>''' + str(traffic_value) + '''</rdf:object>\n</rdf:Description>\n</tnmo:parameter_detailes>\n<tnmo:device_parameter>\n<rdf:Description rdf:about='http://127.0.0.1/TNSeg-1/Device_''' + str(device_num) + '''/'></rdf:Description>\n</tnmo:device_parameter>\n</rdf:Description>\n'''
                        f.write(body)
                        l = l + 1
                i = i + 1
                k = k + 1
            f.write("\n</rdf:RDF>\n")
            f.close()
            spql.write("\nLOAD <file:///" + str(SPARQL_path) + "/" + filename + "_monitoring_segment_1_" + str(FileNum) + "_.nq>;\n")
            k = 1

        # SEGMENT #2

        # Add Users  definitions
        FileNum = 0
        i = 1
        k = 1
        while i <= Max_Users:
            FileNum = FileNum + 1
            f = open(filename + "_users_segment_2_" + str(FileNum) + "_.nq", "at")
            f.write(header)
            f.write("\n<!--Users definitions-->\n")
            while k <= Max_Step_1:
                body = str('<rdf:Description rdf:about="http://127.0.0.1/TNSeg-2/User_') + str(i) + str(
                    '/">\n<TNSeg-2:ID>') + str(i) + str(
                    '</TNSeg-2:ID>\n<tnmo:sameAs><rdf:Description rdf:about="http://127.0.0.1/TNSeg-1/User_') + str(i) + str('/"></rdf:Description></tnmo:sameAs>\n</rdf:Description>\n')
                f.write(body)
                i = i + 1
                k = k + 1
            f.write("\n</rdf:RDF>\n")
            f.close()
            spql.write("\nLOAD <file:///" + str(SPARQL_path) + "/" + filename + "_users_segment_2_" + str(
                FileNum) + "_.nq>;\n")
            k = 1

        # Add Device  definitions
        FileNum = 0
        i = 1
        k = 1
        while i <= Max_Devices:
            FileNum = FileNum + 1
            f = open(filename + "_device_segment_2_" + str(FileNum) + "_.nq", "at")
            f.write(header)
            f.write("\n<!--Device definitions-->\n")
            while k <= Max_Step_1:
                body = str('<rdf:Description rdf:about="http://127.0.0.1/TNSeg-2/Device_') + str(i) + str(
                    '/">\n<net:hasMAC>MAC_Seg-2_') + str(i) + str('</net:hasMAC>\n</rdf:Description>\n')
                f.write(body)
                i = i + 1
                k = k + 1
            f.write("\n</rdf:RDF>\n")
            f.close()
            spql.write("\nLOAD <file:///" + str(SPARQL_path) + "/" + filename + "_device_segment_2_" + str(
                FileNum) + "_.nq>;\n")
            k = 1

            # Add Monitoring items definitions
            FileNum = 0
            i = 1
            k = 1
            l = 1
            while i <= Max_Devices:
                FileNum = FileNum + 1
                f = open(filename + "_monitoring_segment_2_" + str(FileNum) + "_.nq", "at")
                f.write(header)
                f.write("\n<!--Monitoring items definitions-->\n")
                while k <= Max_Step_2:
                    device_num = i
                    traffic_value = 0
                    for ih in range(24):
                        for im in range(6):
                            traffic_value = traffic_value + random.randint(1, Max_Traffic_Step)
                            body = '''<rdf:Description rdf:about='http://127.0.0.1/TNSeg-2/Parameter_M_''' + str(
                                l) + '''/'>\n<rdf:type>TNSeg-2:Parameter_M</rdf:type>\n<tnmo:parameter_timestamp rdf:datatype='http://www.w3.org/2001/XMLSchema#datetime'>2021-08-29 ''' + str(
                                ih) + ':' + str(
                                im) + '''0:00</tnmo:parameter_timestamp>\n<geo:Point geo:lat="55.701" geo:long="12.552"/>\n<tnmo:has_parameter_type>tnmo:device_state</tnmo:has_parameter_type>\n<tnmo:parameter_detailes>\n<rdf:Description>\n<rdf:type>rdf:statement</rdf:type>\n<rdf:predicat>tnmo:parameter_monitoring</rdf:predicat>\n<rdf:subject><rdf:Description rdf:about='http://127.0.0.1/TNSeg-2/User_''' + str(
                                Device_Users_Map_Seg1[
                                    device_num - 1]) + '''/'></rdf:Description></rdf:subject>\n<rdf:object rdf:datatype='http://www.w3.org/2001/XMLSchema#integer'>''' + str(
                                traffic_value) + '''</rdf:object>\n</rdf:Description>\n</tnmo:parameter_detailes>\n<tnmo:device_parameter>\n<rdf:Description rdf:about='http://127.0.0.1/TNSeg-2/Device_''' + str(
                                device_num) + '''/'></rdf:Description>\n</tnmo:device_parameter>\n</rdf:Description>\n'''
                            f.write(body)
                            l = l + 1
                    i = i + 1
                    k = k + 1
                f.write("\n</rdf:RDF>\n")
                f.close()
                spql.write("\nLOAD <file:///" + str(SPARQL_path) + "/" + filename + "_monitoring_segment_2_" + str(
                    FileNum) + "_.nq>;\n")
                k = 1

    spql.close()


if __name__ == "__main__":
    createXML("KG_telecom")
