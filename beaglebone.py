import socket
import os
import logging
import time
import sys
import tarfile
from shutil import move


# TODO
# buffer boyutunu test et 4K vs 8K [ciddi bir fark yok gibi gozukuyor]
#
# timeout tcp (after filename etc)
#
# baglanti koptuğu zaman, backup klasorundeki dosyalar birikiyor. Baglanti duzeldikten sonra da
# bunlarin sayisinda azalma olmayacak cunku yalnızca sıkıstırılan gonderilecek. Gerekli eklemeyi yap.
#
# sort filenames


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
        self.s = None
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


    def isConnAvailable(self):
        ret = False
        try:
            self.s = socket.socket()
            print('socket obj created')
            self.s.settimeout(5)
            self.s.connect((self.host, self.port))
            self.s.settimeout(None)
            print('socket connected')
            ret = True
        except socket.error as err:
            print('exception in ISA')
            # logging.error('Sunucuya baglanilamadi | [%s]', err)
            self.s.close()
        except Exception as err:
            print('xx', err)
        return ret


    def waitForCompression(self):
        while True:
            try:
                fList = sorted([os.path.join(self.rawData, fName) for fName in next(os.walk(self.rawData))[2]])
                if len(fList) >= self.compressThreshold: break
                time.sleep(10)
                print('not enough file, waiting...')
                # check ekle!!!!!
            except StopIteration:
                print('exception in WFC')
                # There is no file in directory. Just wait.
                time.sleep(30)
        print('end of WFC')
        return fList[:self.compressThreshold]


    def compress(self, fList):
        print('beginning of compress')
        tName = (self.getCurrentTime()).split(',')[0]
        tName = tName.replace(' ', '_').replace(':', '-')
        tName = '{}/{}.tar.bz2'.format(self.backupData, tName)
        try:
            with tarfile.open(tName, 'w:bz2') as tFile:
                for fName in fList:
                    tFile.add(fName, arcname=fName.split('/')[1])
            self.removeRawData(fList)
        except Exception as err:
            logging.error('Sikistirma islemi sirasinda bir hata olustu | ( %s )', err)


    def removeRawData(self, fList):
        try:
            for fName in fList:
                os.unlink(fName)
        except OSError as err:
            # Dosya silinemezse ne gibi hatalara neden olabilir???
            logging.error('Sikistirilmis dosyalar silinirken bir hata olustu | ( %s )', err)

    def checkZipQueue(self):
        # If more than one zipped file waiting to send, send them all at once
        try:
            zippedFileList = sorted(next(os.walk(self.backupData))[2])
            if len(zippedFileList) == 1:
                self.sendCompressedData(zippedFileList[0])
            # If more than 5 zipped file, send only first 5 file every turn
            else:
                if 1 < len(zippedFileList) < 6:
                    for zFile in zippedFileList:
                        print(zFile)
                        self.sendCompressedData(zFile)
                else:
                    for i in range(0, 5):
                        self.sendCompressedData(zippedFileList[i])
        except StopIteration:
            pass

    def sendCompressedData(self, compressedFile):
        print('gonderme islemi baslatildi')
        start = time.time()

        if self.isConnAvailable():
            # compressedFile = next(os.walk(self.backupData))[2][0]
            compFileWithPath = '{}/{}'.format(self.backupData, compressedFile)

            try:
                self.s.send(compressedFile.encode('utf-8'))
                with open(compFileWithPath, 'rb') as file:
                    data = file.read(self.buffer)
                    print(data)
                    self.s.send(data)
                    while (data):
                        data = file.read(self.buffer)
                        self.s.send(data)
                self.s.close()
                end = time.time()
                print(compressedFile, ' gonderdildi')
                print(end - start)
                os.unlink(compFileWithPath)
            except Exception as err:
                print('err', err)
                self.s.close()
                logging.error('Sikistirilmis dosyalar gonderilirken bir hata olustu | ( %s )', err)
                return
            except socket.error as err:
                print('---', err)
        else:
            print('baglanti yok')


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
        b.checkZipQueue()


if __name__ == "__main__":
    main()