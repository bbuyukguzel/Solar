import socket
import os
import logging
import time
import sys
import tarfile
from shutil import move


# TODO
# buffer boyutunu test et 4K vs 8K
# timeout tcp (after filename etc)

class Beaglebone:

    def __init__(self):
        ## DIRECTORIES ##
        self.rawData = 'raw'
        self.backupData = 'backup'

        ## CONSTANTS ##
        self.compressThreshold = 15
        self.rawFileCount = 0

        ## SOCKET ##
        self.host = '46.101.107.70'
        self.port = 17642
        self.buffer = 4096
        self.s = socket.socket()
        logging.info('Initialize basarili')


    def prepareFolders(self):
        logging.info('Dizinler ayarlaniyor')
        if not os.path.exists(self.backupData):
            try:
                logging.info('(%s) klasoru olusturuluyor', self.backupData)
                os.makedirs(self.backupData)
            except Exception as err:
                logging.error('(%s) klasoru olusturulamadi | [%s]', self.backupData, err)
                sys.exit(1)


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
                if len(fList) >= self.compressThreshold: break
                time.sleep(1)
            except StopIteration:
                # There is no file in directory. Just wait.
                time.sleep(5)
        return fList[:self.compressThreshold]


    def compress(self, fList):
        tName = (self.getCurrentTime()).split(',')[0]
        tName = tName.replace(' ', '_').replace(':', '-')
        tName = '{}/{}.tar.bz2'.format(self.backupData, tName)
        try:
            with tarfile.open(tName, 'w:bz2') as tFile:
                for fName in fList:
                    tFile.add(fName, arcname=fName.split('/')[1])
            logging.info('Sikistirma islemi tamamlandi')
        except Exception as err:
            logging.error('Sikistirma islemi sirasinda bir hata olustu | ( %s )', err)
        self.removeRawData(fList)


    def removeRawData(self, fList):
        try:
            for fName in fList:
                os.unlink(fName)
            logging.info('Sikistirilmis dosyalar silindi')
        except OSError as err:
            # Dosya silinemezse ne gibi hatalara neden olabilir???
            logging.error('Sikistirilmis dosyalar silinirken bir hata olustu | ( %s )', err)


    def sendCompressedData(self):
        """
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
        os.unlink(compFileWithPath)"""


def main():
    logging.basicConfig(filename='example.log',
                        filemode='a',
                        level=logging.DEBUG,
                        format='%(asctime)s | %(levelname)s: %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')
    logging.info('\n\n\n{}'.format('-'*30))
    logging.info('Program baslatildi')

    b = Beaglebone()
    b.prepareFolders()
    while True:
        lst = b.waitForCompression()
        b.compress(lst)
        b.sendCompressedData()


if __name__ == "__main__":
    main()