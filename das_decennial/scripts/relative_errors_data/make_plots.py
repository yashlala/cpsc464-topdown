import matplotlib as mpl
mpl.use('agg')
import matplotlib.pyplot as plt

import numpy as np
import os, sys
import glob, json, pickle

# Specific to mode=heatMap. Also super hacky.
magnitude_bin_edges_dict = json.load(open("PL94/heatMaps/low_cap_RE_adaptive_truth/true_magnitude_bins_PLB_0.125.json", 'r'))
REL_ERR_BIN_EDGES = [0.0] + [2.**(i-3) for i in range(13)] + [np.inf]
###############

def histogramToObservations(hist2d, xbins, ybins):
    xs, ys = [], []
    print("Unraveling hist2d into individual obs:")
    for (i, j), count in np.ndenumerate(hist2d):
        xval = (xbins[i+1] + xbins[i])/2.
        yval = (ybins[j+1] + ybins[j])/2.
        if count > 0 and xval < 1000.:
            print(f"({i}, {j}) ;; ({xval}, {yval}) ;; {count}")
            xs += [xval]*int(count)
            ys += [yval]*int(count)
    return xs, ys

def genHeatMap(npyPath=None, threshold=None):
    assert threshold == 0, "Heat map option 'threshold>0' not yet implemented."

    relative_error_histogram = np.load(npyPath)
    baseName = os.path.splitext(npyPath)[0]
    print(f"npy base name: {baseName}")
    geolevel = baseName.split("_PLB_")[0].split("relative_error_")[1]
    magnitude_bin_edges = magnitude_bin_edges_dict[geolevel]
    print(f"Magnitude bin edges: {magnitude_bin_edges}")
    rel_err_bin_edges = REL_ERR_BIN_EDGES[:-1] +[2.**14.]
    print(f"Rel err bin edges: {rel_err_bin_edges}")

    print(f"For {baseName}, magnitude-error 2d histogram:")
    print(relative_error_histogram)

    print(f"Total # of estimates for {baseName}: {relative_error_histogram.sum()}")
    #fig, axes = plt.subplots(nrows=1, ncols=1)
    print("Getting xs, ys...")
    xs, ys = histogramToObservations(relative_error_histogram, magnitude_bin_edges, rel_err_bin_edges)
    print("Got xs:")
    print(xs)
    print("Got ys:")
    print(ys)
    print("Generating axes.hist2d...")
    plt.hist2d(xs, ys, bins=30, cmap="Greys", normed=True)
    plt.colorbar()

    #print(dir(axes))
    plt.title("2DHist: Rel Err vs True Magnitude")
    plt.ylabel("Relative Error")
    plt.xlabel("True Magnitude")
    #axes.set_yticklabels([str(label) for label in bin_edges])
    #axes.set_xticklabels([str(label) for label in bin_edges])

    figName = f"{baseName}.png"
    print(f"Saving fig {figName}...")
    plt.savefig(figName)
    print(f"Done saving fig {figName}")
    plt.close()

def genScatterplot(inputFilename=None, threshold=None):
    print("Loading scatterplot input data...")
    with open(inputFilename, 'rb') as fp:
        scatterpoints = pickle.load(fp)
    xs, ys = scatterpoints[0], scatterpoints[1]
    print("Smallest x, smallest y: ", min(xs), " :: ", min(ys))
    print("Largest x, largest y: ", max(xs), " :: ", max(ys))

    (lb, ub) = threshold
    if threshold != (0.0, 0.0):
        xys = list(zip(xs, ys))
        new_xys = []
        if lb >= 1.0 and ub >= 1.0:
            for xy in xys:
                if xy[0] >= lb and xy[0] <= ub:
                    #print("Adding xy to new xys: ", xy)
                    new_xys.append(xy)
        #elif threshold == -1:
        elif lb >= 0.0 and ub >= 0.0:
            floatLB = lb * float(max(xs))
            floatUB = ub * float(max(xs))
            for xy in xys:
                if xy[0] >= floatLB and xy[0] <= floatUB:
                    #print("Adding xy to new xys: ", xy)
                    new_xys.append(xy)
        else:
            adaptiveLB = 5. * float(max(xs))/100.
            adaptiveUB = 10. * adaptiveLB
            for xy in xys:
                if xy[0] >= adaptiveLB and xy[0] <= adaptiveUB:
                    new_xys.append(xy)
        #else:
        #    raise ValueError("Urgh.")
        xs, ys = list(zip(*new_xys))

    #print("xs, ys look like:")
    #print(xs[:100])
    #print(ys[:100])

    inputDir = os.path.dirname(inputFilename)
    baseName = os.path.splitext(inputFilename)[0]
    print(f"From {inputDir}, loaded input data from {baseName}.pkl. Creating scatterplot...")

    print("Creating scatterplot...")
    plt.scatter(xs, ys)
    plt.title("Scatter: Rel Err vs True Magnitude")
    plt.ylabel("Relative Error")
    plt.xlabel("True Magnitude")
    print("Created scatterplot")

    strThreshold = str(threshold).replace('(','').replace(')','').replace(' ','')
    figName = f"{baseName}_threshold{strThreshold}.png"
    print(f"Saving fig {figName}...")
    plt.savefig(figName)
    print(f"Done saving fig {figName}")
    plt.close()

def genPlot(mode=None, inputFilename=None, threshold=None):
    genPlotter = {"heatMap":genHeatMap, "scatterplot":genScatterplot}
    genPlotter[mode](inputFilename=inputFilename, threshold=threshold)

def getCWDFileNames(mode=None, targetPath=None):
    inputFileSuffix = {"heatMap":".npy", "scatterplot":".pkl"}
    return glob.glob(targetPath+"*"+inputFileSuffix[mode])

def main(mode=None, targetPath=None, threshold=None):
    filenames = getCWDFileNames(mode=mode, targetPath=targetPath)
    for filename in filenames:
        #if "Block" not in filename:
        #if "Block_Group" in filename or "Block" not in filename:
        #if "Nation" in filename and "20" in filename:
        #if "County" in filename:
        if True:
            print(f"Processing filename {filename}")
            genPlot(mode=mode, inputFilename=filename, threshold=threshold)

if __name__ == "__main__":
    assert len(sys.argv) == 5, "Missing argument: python make_plots.py pathToTargetFolder mode lb ub"
    scriptName = sys.argv[0]
    targetFolderPath = sys.argv[1]
    mode = sys.argv[2]
    lb = float(sys.argv[3])
    ub = float(sys.argv[4])
    #zeros = True if sys.argv[3]=="True" else False
    main(mode=mode, targetPath=targetFolderPath, threshold=(lb,ub))
