import os

os.system("sqoop import --bindir ./ --connect jdbc:mysql://192.168.0.101/Gaana --username hduser --password interlog --table lyrics --m 1")
