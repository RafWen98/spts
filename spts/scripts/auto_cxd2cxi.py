import argparse
import os
import numpy
import sys
import time
import h5py
import h5writer
import pandas as pd
import cxd_to_h5 as cxd


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
        


if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description='Conversion of CXD (Hamamatsu file format) to HDF5')
    parser.add_argument('log_file', type=str, nargs='?',
                        help='.csv filename of the log book.', default = None)
    parser.add_argument('data_path', type=str, nargs='?',
                        help='path of stored data.', default = None)
    parser.add_argument('-auto', '--automatic-mode', type=bool,
                        help='iterates through folder and creates non existing cxi files.', default = False)
    parser.add_argument('-s', '--start-filenumber', type=int,
                        help='number of the first file to be processed.', default = None)
    parser.add_argument('-e', '--end-filenumber', type=int,
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

    if args.automatic_mode:
        #get log file and creat a panda dataset
        try:
            log = pd.read_csv(args.log_file)
            print("Logfile found and loaded.")
        except:
            print("ERROR: Log file not found or not in CSV format.")
            sys.exit(-1)
        
        #check if data_path exists
        if not os.path.exists(args.data_path):
            print("ERROR: Data path does not exist.")
            sys.exit(-1)
        else:
            print("Data path found.")

        flatfield_file_found = False
        if args.flatfield_filename is None:
            try:
                args.flatfield_filename = "_flatfield01624.cxd"
                args.flatfield_filepath = os.path.join(args.data_path, args.flatfield_filename)
                flatfield_file_found = True
                print("_flatfield Flatfield file found.")
            except:
                print("Const _flatfield01624.cxd file not found, looking for data01624.cxd")
                try:
                    args.flatfield_filename = "data01624.cxd"
                    args.flatfield_filepath = os.path.join(args.data_path, args.flatfield_filename)
                    flatfield_file_found = True
                    print("data01624.cxd Flatfield file found.")
                except:
                    print("ERROR: No flatfield file found.")
                    sys.exit(-1)

        #check data_path and create a list of all files ending with .cxd
        files = [f for f in os.listdir(args.data_path) if f.endswith('.cxd')]
        #check data_path and create a list of all files ending with .cxi
        files_done = [f for f in os.listdir(args.data_path) if f.endswith('.cxi')]
        #create a list of files that still need to be processed
        files_to_do = list(set(files) - set(files_done))
        
        #order the files according to the file number
        files_to_do.sort()
        print("found these files to process:")
        print(files_to_do)
        if flatfield_file_found:
            print("Flatfield file found: ", args.flatfield_filename)
            files_to_do.remove(args.flatfield_filename)

        files_to_do = list(set(files_to_do) - set(["_flatfield01624.cxd", "data01624.cxd"]))
        print(files_to_do)
        

        print(files_to_do[0])
        print(args.flatfield_filename)
        iter_time = 0
        #iterate through files_to_do and process them
        for file in files_to_do:
            print("Processing file: ", file)
            #track the time of the last few iterations and give an estimate for the remaining time
            start_time = time.time()
            print("File ", files_to_do.index(file)+1, " of ", len(files_to_do))
            print("Estimated time remaining: ", iter_time*(len(files_to_do) - files_to_do.index(file)))


            #some of the files have the incorrect ending in the logfile
            row = log.loc[log['File'] == file[:-4] + ".cxd"]    #find row
            bg_file = row['Dark Correction '].values[0]         #extract background file name
            bg_n_max = row['frames'].values[0]                  #extract number of frames of background file

            #extract flatfield file name from the row 
            #(flatfield not implemented so standard flatfield file)
            #ff_file = row['Flatfield file name'].values[0]

            args.filename = os.path.join(args.data_path, file)
            args.background_filename = os.path.join(args.data_path, bg_file)
            args.bg_frames_max = bg_n_max
            tong_code(args)

            end_time = time.time()
            iter_time = end_time - start_time
            print("Time taken: ", iter_time)
        
    






