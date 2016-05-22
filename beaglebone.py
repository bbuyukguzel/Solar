import socket
import os
from shutil import move
import time
import tarfile


# TODO
# buffer boyutunu test et 4K vs 8K

class Beaglebone:

    def __init__(self):
        self.host = '46.101.107.70'
        self.port = 17642
        self.buffer = 4096
        self.rawData = 'raw'
        self.backupData = 'backup'
        self.readyToSendData = ''
        self.rawFileCount = 0
        self.s = socket.socket()


    def prepareFolders(self):
        if not os.path.exists(self.backupData):
            os.makedirs(self.backupData)


    def getCurrentTime(self):
        return time.strftime('%d-%m-%Y %H:%M:%S')


    def connectToServer(self):
        try:
            self.s.connect((self.host, self.port))
            return True
        except socket.error as err:
            # LOG ERROR
            return False


    def waitForCompression(self):
        while True:
            try:
                fList = [os.path.join(self.rawData,fName) for fName in next(os.walk(self.rawData))[2]]
                print(len(fList))
                if len(fList) == 15: break
                # !!!!!! == e dikkat et. direk x+1 degeri olursa sonlanmiyor
                time.sleep(1)
            except StopIteration:
                print('SI')
                time.sleep(5)
        self.compress(fList)


    def compress(self, fList):
        print('compression started')
        tName = (self.getCurrentTime()).split(',')[0]
        tName = tName.replace(' ', '_').replace(':', '-')
        tName = '{}/{}.tar.bz2'.format(self.backupData, tName)
        with tarfile.open(tName, 'w:bz2') as tFile:
            for fName in fList:
                tFile.add(fName, arcname=fName.split('/')[1])
        self.removeRawData(fList)


    def removeRawData(self, fList):
        for fName in fList:
            os.unlink(fName)
        print('remove complete')


    def sendCompressedData(self):
        print('send started')
        self.connectToServer()
        compressedFile = next(os.walk(self.backupData))[2][0]
        compFileWithPath = '{}/{}'.format(self.backupData, compressedFile)
        self.s.send(compressedFile.encode('utf-8'))

        file = open(compFileWithPath, 'rb')
        data = file.read(self.buffer)
        self.s.send(data)
        while (data):
            data = file.read(self.buffer)
            self.s.send(data)
        file.close()
        print('*****    GONDERME    ISLEMI    TAMAMLANDI    *****')

        print('->', compFileWithPath)
        os.unlink(compFileWithPath)



b = Beaglebone()
b.prepareFolders()
while True:
    b.waitForCompression()
    b.sendCompressedData()