#!/usr/bin/env python 
import numpy as np
import argparse
import os, sys, shutil
import time
import socket
import pandas as pd
import spts.config
import spts.worker

# Add SPTS stream handler to other loggers
import h5writer
import concurrent.futures
import logging


# Add the parent directory to the sys.path
parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(parent_dir)

import utils.log_book as log_book

# Configure logging
original_level = logging.getLogger().level
logging.basicConfig(level=original_level)  # Set initial logging level

def tong_spts_ana(args, conf, out_file, silent = False):
    if silent:  # Suppress all logging output
        original_level = logging.getLogger().level
        logging.getLogger().setLevel(logging.CRITICAL)

    
    if args.mpi:    #not really happening anyway
        import mpi4py
        comm = mpi4py.MPI.COMM_WORLD
        is_worker = comm.rank > 0
        H = h5writer.H5WriterMPISW(out_file, comm=comm, chunksize=100, compression=None)
        if is_worker:
            W = spts.worker.Worker(conf, i0_offset=comm.rank-1, step_size=comm.size-1)
    else:
        is_worker = True
        H = h5writer.H5Writer(out_file)
        W = spts.worker.Worker(conf)

    if is_worker:
        while True:
            t0 = time.time()
            #print("Read work package (analysis)")
            w = W.get_work()
            if w is None:
                #print("No more images to process")
                break
            #print("Start work")
            l = W.work(w)
            t1 = time.time()
            t_work = t1-t0
            t0 = time.time()
            H.write_slice(l)
            t1 = time.time()
            t_write = t1-t0
            #print("work %.2f sec / write %.2f sec" % (t_work, t_write))            

    H.write_solo({'__version__': spts.__version__})
    H.close()

    #print("SPTS - Clean exit.")

    if silent:
        logging.info("This will not appear in the console.")
        logging.getLogger().setLevel(original_level)


def run_spts_all():
    filenames = [f for f in os.listdir(".") if f.endswith(".cxi")]

    if len(sys.argv) > 1:
        timestamp = sys.argv[1]
        filenames = [filename for filename in filenames if timestamp in filename]

    for f in filenames:
        n = f[:-4]
        
        d = "./"+n+"_analysis"
        if not os.path.exists(d):
            os.mkdir(d)

        #cmds = ["cp ./spts.conf %s/" % (d), "ln -s ../%s %s/frames.cxi" % (f,d), "cd %s; run_spts.py -c 9 -v; cd .." % (d)]
        cmds = ["cp ./spts.conf %s/" % (d), "ln -s ../%s %s/frames.cxi" % (f,d), "cd %s; run_spts.py -v; cd .." % (d)]
        for cmd in cmds:
            print(cmd) 
            os.system(cmd)

def get_args():
    parser = argparse.ArgumentParser(description='Mie scattering imaging data analysis')
    parser.add_argument('-dp', '--directory', type=str, help='directory for input', default=None)
    parser.add_argument('-cf', '--config_file', type=str, help='config file containing analysis data', default=None)
    parser.add_argument('-lf', '--log_file', type=str, help='log book file containing experiment data', default=None)
    parser.add_argument('-out', '--out_name_appx', type=str, help='Appendix added onto output file, ie: ana_d25 => data00000_ana_d25.cxi', default=None)
    parser.add_argument('-sn', '--start_number', type=int, help='start number of to be analyzed data', default=None)
    parser.add_argument('-en', '--end_number', type=int, help='end number of to be analyzed data', default=None)
    parser.add_argument('-sd', '--save_directory', type=str, help='output directory', default=None)
    parser.add_argument('-wd', '--window_size', type=int, help='window width, pixel limits', default=None)
    parser.add_argument('-v', '--verbose', dest='verbose',  action='store_true', help='verbose mode', default=False)
    parser.add_argument('-d', '--debug', dest='debug',  action='store_true', help='debugging mode (even more output than in verbose mode)', default=False)
    parser.add_argument('-c','--cores', type=int, help='number of cores', default=1)
    parser.add_argument('-m','--mpi', dest='mpi', action='store_true', help='mpi processes = reader(s) + writer', default=False)
    parser.add_argument('-ow','--overwrite', type=str, help='Standard False, if True overwrites file if found in folder', default=1)
    
    args = parser.parse_args()

    #check essential inputs
    if args.directory is None:
        parser.error(f"No data directory was given. -dp=None")
    if args.log_file is None:
        parser.error(f"No log book file was given. -lf=None")

    #check if data directory exists
    if not os.path.exists(args.directory):
        parser.error(f"Cannot find data path {args.directory} in current directory.")

    #check if config file exists and finds standard otherwise
    if args.config_file:
        try:
            conf = spts.config.read_configfile(args.config_file)
            print("Config file found and loaded.")
        except:
            print("ERROR: Config file not found.")
            sys.exit(-1)
    else:
        print(f"Trying to find default config file in directory: {args.directory}")
        try:
            conf = spts.config.read_configfile(args.directory + "spts.conf")
            args.config_file = args.directory + "spts.conf"
            print("Default config file found and loaded.")
        except:
            print("ERROR: No config file found.")
            sys.exit(-1)

    #check if log file exists
    if args.log_file:
        try:
            log = pd.read_csv(args.log_file)
            print("Logfile found and loaded.")
        except:
            print("ERROR: Log file not found or not in CSV format.")
            sys.exit(-1)

    return args

def check_dataset_exists(file_path, dataset_name):
    import h5py
    with h5py.File(file_path, 'r') as f:
        if dataset_name in f:
            return True
        else:
            return False

def prepare_save_directory(args, file, conf, log):

    #check and creat save directory
    if args.save_directory is None:
        if args.window_size < 10:
            args.save_directory = "./data/analysis/ana_w0" + str(conf['analyse']['window_size']) + "/"
        else:
            args.save_directory = "./data/analysis/ana_w" + str(conf['analyse']['window_size']) + "/"
    if not os.path.exists(args.save_directory):
        os.makedirs(args.save_directory)
    
    appendix = ""
    if args.out_name_appx is not None:
        if args.out_name_appx[0] != "_":
            args.out_name_appx = "_" + args.out_name_appx
        appendix = args.out_name_appx
    
    #add 0 in front of window size if smaller than 10
    if args.window_size < 10:
        out_file = args.save_directory + file[:-4] + "_ana_w0" + str(args.window_size) + appendix + ".cxi"
    else:
        out_file = args.save_directory + file[:-4] + "_ana_w"  + str(args.window_size) + appendix + ".cxi"

    return out_file

def run_process(args, file, conf, log, silent=False):

    print("Processing file: ", file)

    conf = prepare_config(conf, args, file, log)
    out_file = prepare_save_directory(args, file, conf, log)

    # Check if the output file already exists
    if os.path.exists(out_file):
        if args.overwrite:
            print(f"Overwriting file {out_file}.")
            os.remove(out_file)
        else:
            print(f"File {out_file} already exists. Skipping processing.")
            return
    
    if silent:  # Suppress all logging output
        original_level = logging.getLogger().level
        logging.getLogger().setLevel(logging.CRITICAL)

    tong_spts_ana(args, conf, out_file, silent)

    if silent:
        logging.info("This will not appear in the console.")
        logging.getLogger().setLevel(original_level)

    print("Saved file: ", file)

def prepare_config(conf, args, file, log):
    
    #set window size
    if args.window_size is None:
        args.window_size = conf['analyse']['window_size']
    else:
        conf['analyse']['window_size'] = args.window_size

    #define path of to be analyzed file
    conf['general']['filename'] = args.directory + file[:-4] + ".cxi"

    #read number of frames from log file
    row = log.loc[log['File'] == file[:-4] + ".cxd"]
    n_max = int(row['frames'].values[0])
    conf['general']['n_images'] = n_max

    #spts.config.write_configfile(conf, args.config_file)   #if generated config wants to be stored
    
    return conf

def run_spts_auto():
    args = get_args()
    log = log_book.read_log_book(args.log_file)
    files_to_do = log_book.get_cxi2do(args, log)
    conf = spts.config.read_configfile(args.config_file)

    #run_process(args, files_to_do[0], conf, log)

    start_time = time.time()
    iter_time = 0

    print(files_to_do)

    #iterate through files_to_do and process them
    with concurrent.futures.ProcessPoolExecutor() as executor:
        futures = [executor.submit(run_process, args, file, conf, log, silent=True) for file in files_to_do]
        for i, future in enumerate(concurrent.futures.as_completed(futures)):
            try:
                future.result()
                elapsed_time = time.time() - start_time
                iter_time = elapsed_time / (i + 1)
                remaining_files = len(files_to_do) - (i + 1)
                eta = iter_time * remaining_files
                print(f"Processed {i + 1}/{len(files_to_do)} files. ETA: {eta:.2f} seconds")
            except Exception as exc:
                print(f'Generated an exception: {exc}')
            

                

if __name__ == "__main__":
    run_spts_auto()



    
