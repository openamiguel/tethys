# tethys
A parser for the Tethys database by the Pacific Northwest National Laboratory. 

Check out [this website](https://tethys.pnnl.gov/) for the source data.

Sample scripts to run the code:

`python main.py -folderpath /Users/openamiguel/Desktop/tethys -logpath /Users/openamiguel/Desktop`
This script carries out the full extract-transform-load pipeline on Tethys. 

`python main.py -folderpath /Users/openamiguel/Desktop/tethys -logpath /Users/openamiguel/Desktop -suppressDownload`
This script does not re-download the data. It simply deduplicates the existing data and writes it into one file.
