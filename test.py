import random
import time
import shutil
import os


def generateSample(folder):
    temp = '{0:.1f}'.format(random.uniform(10, 40))
    rad = '{0:.1f}'.format(random.uniform(0, 5))
    cur = ['{0:.4f}'.format(random.uniform(0, 5)) for i in range(1024)]
    volt = ['{0:.4f}'.format(random.uniform(0, 5)) for i in range(1024)]
    with open('{}/{}.txt'.format(folder, random.randint(0, 5000)), 'w') as file:
        t = getCurrentTime()
        file.write('{}, {}, {}\n'.format(t, temp, rad))
        for i in range(1024):
            file.write('{}, {}, {}\n'.format(t, cur[i], volt[i]))


def getCurrentTime():
    return time.strftime('%d-%m-%Y %H:%M:%S')


folder = 'raw'
try:
    shutil.rmtree(folder)
except:
    pass
try:
    if not os.path.exists(folder):
        os.makedirs(folder)
except:
    pass

while True:
    generateSample(folder)
    time.sleep(1)