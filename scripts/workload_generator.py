import random

class WorkLoadGenerator():

    def generate(self, skewed = -1, keys_num = 10, scale = int(1e2), filename='data.txt'):
        data_list = []
        for _ in range(scale):
            key, value = None, 1
            rand = random.randint(0, keys_num)
            if (skewed != -1):
                if (random.randint(0, 100) < skewed):
                    key = 0
                else:
                    key = _ % keys_num
            else:
                key = _ % keys_num
            data_list.append((key, value))
        with open(filename, 'w', encoding='utf-8') as f:
            for data in data_list:
                f.write('key' + str(data[0]) + '\t' + str(value) + '\n')
            f.close()
            
        print(f'workload {filename} is generated.')

if __name__ == '__main__':
    workLoadGenerator = WorkLoadGenerator()

    workLoadGenerator.generate(skewed=-1, keys_num=int(10), scale = int(1e7), filename='input/data_lab1.txt')
    
    workLoadGenerator.generate(skewed=-1, keys_num=int(1e7), scale=int(1e7), filename='input/data_lab2.txt')

    workLoadGenerator.generate(skewed=80, keys_num=int(20), scale=int(1e4), filename='input/data_lab3.txt')