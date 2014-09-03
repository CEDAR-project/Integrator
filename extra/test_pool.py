import multiprocessing
import time

def do_calculation(x):
    time.sleep(x)
    print x
    return x

def start_process():
    print 'Starting ', multiprocessing.current_process().name

if __name__ == '__main__':
    inputs = list(range(0,10,4))
    print 'Input   :', inputs
    pool_size = multiprocessing.cpu_count() * 2
    print pool_size
    pool = multiprocessing.Pool(processes=pool_size)
    pool_outputs = pool.map(do_calculation, inputs)
    pool.close()
    pool.join()
    print 'Pool    :', pool_outputs