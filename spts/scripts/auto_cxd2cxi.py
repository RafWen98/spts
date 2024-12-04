#!/usr/bin/env python
import argparse
import os
import numpy
import sys
import time
import h5py
import h5writer
import logging
import logging.handlers
import pandas as pd
import cxd_to_h5 as cxd
import concurrent.futures
import multiprocessing
from io import StringIO
from contextlib import redirect_stdout, redirect_stderr
import traceback

# Add the parent directory to the sys.path
parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(parent_dir)

import utils.log_book as log_book

# Configure logging
logging.basicConfig(level=logging.INFO)  # Set initial logging level


def tong_code(args):

    bg, bg_std, good_pixels = cxd.estimate_background(
        args.background_filename, args.bg_frames_max, args.filename)
    ff, ff_std = cxd.estimate_flatfield(
        args.flatfield_filepath, args.ff_frames_max, bg, good_pixels)
    roi = cxd.guess_ROI(ff, args.flatfield_filepath,
                    args.roi_low_limit, args.roi_fraction)

    if(args.filename is None):
        sys.exit(0)

    if not args.filename.endswith(".cxd"):
        print("ERROR: Given filename %s does not end with \".cxd\". Wrong format!" %
              args.filename)
        sys.exit(-1)

    if args.out_filename:
        f_out = args.out_filename
    else:
        f_out = args.filename[:-4] + ".cxi"

    # Initialise output CXI file
    print("Writing to %s" % f_out)
    W = h5writer.H5Writer(f_out)

    cxd.cxd_to_h5(args.filename, bg, ff, roi, good_pixels, W, args.percentile_filter, args.percentile_number,
              args.percentile_frames, args.crop_raw, args.min_x, args.max_x, args.min_y, args.max_y, args.skip_raw)

    # Write out information on the command used
    out = {"entry_1": {"process_1": {}}}
    out["entry_1"]["process_1"] = {"command": str(sys.argv)}
    out["entry_1"]["process_1"] = {"cwd": str(os.getcwd())}
    W.write_solo(out)
    # Close CXI file
    W.close()
    if args.skip_raw:
        h5py.File(f_out,'r+')['entry_1']['data_1']['data'] = h5py.SoftLink('/entry_1/image_1/data')

def get_args():
    parser = argparse.ArgumentParser(
        description='Conversion of CXD (Hamamatsu file format) to HDF5')
    parser.add_argument('-data_path', type=str, nargs='?',
                        help='path of stored data.', default = None)
    parser.add_argument('-log_file', type=str, nargs='?',
                        help='.csv filename of the log book.', default = None)
    parser.add_argument('-bg_file', '--background_file', type=str,
                        help='path to single background file for batch processing', default = False)
    parser.add_argument('-sn', '--start_number', type=int,
                        help='number of the first file to be processed.', default = None)
    parser.add_argument('-en', '--end_number', type=int,
                        help='number of the last file to be processed.', default = None)
    parser.add_argument('-bn', '--bg-frames-max', type=int,
                        help='Maximum number of frames used for background calculation.', default=100)

    parser.add_argument('-f', '--flatfield-filename', type=str,
                        help='CXD filename with flat field correction (laser on paper) data.', default=None)
    parser.add_argument('-fn', '--ff-frames-max', type=int,
                        help='Maximum number of frames used for flatfield calculation.', default=100)

    parser.add_argument('-rl', '--roi-low-limit', type=int,
                        help='Miminum intensity threshold for ROI calculations from flatfield.', default=10)
    parser.add_argument('-rf', '--roi-fraction', type=int,
                        help='Fraction of intensity above threshold to include in ROI.', default=0.999)

    parser.add_argument('-m', '--percentile-filter', action='store_true',
                        help='Apply a percentile filter to output images.')
    parser.add_argument('-p', '--percentile-number', type=int,
                        help='Percentile value for percentile filter.', default=50)
    parser.add_argument('-pf', '--percentile-frames', type=int,
                        help='Number of frames in kernel for percentile filter.', default=4)

    parser.add_argument('-crop', '--crop-raw', action='store_true',
                        help='Enable manual cropping of output images. Disables auto cropping')
    parser.add_argument('-minx', '--min-x', type=int,
                        help='Minimum x-coordinate of cropped raw data.', default=0)
    parser.add_argument('-maxx', '--max-x', type=int,
                        help='Maximum x-coordinate of cropped raw data.', default=2048)
    parser.add_argument('-miny', '--min-y', type=int,
                        help='Minimum y-coordinate of cropped raw data.', default=0)
    parser.add_argument('-maxy', '--max-y', type=int,
                        help='Maximum y-coordinate of cropped raw data.', default=2048)
    parser.add_argument('-o', '--out-filename', type=str,
                        help='destination file')
    parser.add_argument('-overwr', '--overwrite', type=bool,
                        help='overwrite already cxi files in folder', default=False)
    parser.add_argument('-sk', '--skip-raw', action='store_true',
                        help='Skip saving the raw data, instead linking to processed data')
    parser.add_argument('-q', '--quiet', action='store_true',
                        help="Don't show plots interactively")

    args = parser.parse_args()

    if args.log_file is None:
        print("ERROR: No log file given.")
        sys.exit(-1)

    if args.data_path is None:
        print("ERROR: No data path given.")
        sys.exit(-1)

    #check if data_path exists
    if not os.path.exists(args.data_path):
        print(f"ERROR: Data path does not exist. {args.data_path}")
        sys.exit(-1)
    else:
        print(f"Data path found. path {args.data_path}")

    #get log file and creat a panda dataset
    try:
        log = pd.read_csv(args.log_file)
        print(f"Logfile found and loaded. Path: {args.log_file}" )
    except Exception as e:
        print(f"ERROR: Log file not found or not in CSV format at path {args.log_file}")
        print(e)
        sys.exit(-1)

    if args.flatfield_filename is None:     #will try to find a flatfield file
        try:
            args.flatfield_filename = "data01624.cxd"
            args.flatfield_filepath = "data/consts/" + args.flatfield_filename
            flatfield_file_found = True
            print(f"Flatfield file found. Path: {args.flatfield_filepath}")
        except:
            try:
                args.flatfield_filename = "_flatfield01624.cxd"
                args.flatfield_filepath = os.path.join(args.data_path, args.flatfield_filename)
                flatfield_file_found = True
                print("_flatfield Flatfield file found.")
            except:
                print("Const '_flatfield.cxd' file not found, looking for data01624.cxd")
                try:
                    args.flatfield_filename = "data01624.cxd"
                    args.flatfield_filepath = os.path.join(args.data_path, args.flatfield_filename)
                    flatfield_file_found = True
                    print("data01624.cxd Flatfield file found.")
                except:
                    print("ERROR: No flatfield file found.")
                    sys.exit(-1)
    else:
        #check if flatfield file contains "/"
        if "/" in args.flatfield_filename:
            args.flatfield_filepath = args.flatfield_filename
        else:
            args.flatfield_filepath = os.path.join(args.data_path, args.flatfield_filename)
        #check if flatfield file exists
        if not os.path.exists(args.flatfield_filepath):
            print(f"ERROR: Flatfield file not found in folder. {args.flatfield_filepath}")
            sys.exit(-1)
        else:
            print(f"Flatfield file found. Path: {args.flatfield_filepath}")

    return args

def process_file(args, file, files_to_do, log):

    print("Processing file: ", file)
    print("File ", files_to_do.index(file) + 1, " of ", len(files_to_do))

    row = log.loc[log['File'] == file]

    if args.background_file:
        bg_file = args.background_file
        if args.bg_frames_max is None:
            bg_n_max = 100
            print("No background frames given. Using default value of 100.")
    else:
        bg_file = row['Dark Correction '].values[0]

        #chec if bg_file is a float
        if isinstance(bg_file, float):
            #convert to str
            bg_file = str(int(bg_file))

        #if bg_fie doesnt end with .cxd, but is a number like '2330', then it is a file number
        if not bg_file.endswith('.cxd'):
            #check if the number is a number
            if not bg_file.isdigit():
                print(f"ERROR: Background file found for {file} in log file is not a number or a .cxd file.")
                sys.exit(-1)
            #also check that the number has 5 values since the log file has 5 digit file numbers
            if not len(bg_file) == 5:
                #add zeros in front of the number until it has 5 digits
                bg_file = bg_file.zfill(5)
            bg_file = f'data{bg_file}.cxd'

        bg_row = log.loc[log['File'] == bg_file]
        bg_n_max = int(bg_row['frames'].values[0])

    args.filename = os.path.join(args.data_path, file)
    args.background_filename = os.path.join(args.data_path, bg_file)
    args.bg_frames_max = bg_n_max

    #check if background file exists
    if not os.path.exists(args.background_filename):
        print(f"ERROR: Background file not found in folder. {args.background_filename}")
        sys.exit(-1)

    original_level = logging.getLogger().level
    logging.getLogger().setLevel(logging.CRITICAL)
    tong_code(args)
    logging.info("This will not appear in the console.")
    logging.getLogger().setLevel(original_level)
        
def main():
    args = get_args()
    log = log_book.read_log_book(args.log_file)
    files_to_do = log_book.get_cxd2do(args, log)

    start_time = time.time()
    iter_time = 0

    
    
    #iterate through files_to_do and process them
    with concurrent.futures.ProcessPoolExecutor() as executor:
            futures = [executor.submit(process_file, args, file, files_to_do, log) for file in files_to_do]
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
                    traceback.print_exc()

if __name__ == "__main__":
    main()
    #main()