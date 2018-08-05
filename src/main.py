#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Aug 4, 15:24:00, 2018

Parses the online Tethys database for paper titles.

In my code, a tag is defined as a broad research category (e.g. stressors), and
a subtag is defined a more specific research category (e.g. chemicals as 
stressors).

The page number in Tethys' online information follows an interesting format. 
Page 1 has no suffix, page 2 has the suffix "?page=1", and so on, with page N 
having suffix "?page={}".format(N - 1). 

@author: openamiguel
"""

from bs4 import BeautifulSoup
import logging
import os
import pandas as pd
import re
import sys
import time
import urllib.error
import urllib.request

# Up-to-date as of August 4, 2018
DELIM = "\t"
TABLE_COLUMNS = ['title', 'authors', 'date', 'content_type', 'technology_type', 'stressor', 'receptor']
TETHYS_URL = "https://tethys.pnnl.gov{}{}{}"
TETHYS_TAG_SUBTAG = {
			'stressor': ['chemicals', 'dynamic-device', 'emf', 'energy-removal', 
					'lighting', 'noise', 'static-device'], 
			'receptor': ['bats', 'benthic-invertebrates', 'birds', 'ground-nesting-birds', 
					'passerines', 'raptors', 'seabirds', 'shorebirds', 'waterfowl', 
					'ecosystem', 'fish', 'marine-mammals', 'sea-turtles', 
					'terrestrial-mammals', 'farfield-environment', 'nearfield-habitat', 
					'socio-economics', 'aesthetics', 'climate-change', 'fishing', 
					'legal-and-policy', 'navigation', 'recreation', 'stakeholder-engagement'], 
			'technology-type': ['marine-energy-general', 'riverine', 'ocean-current', 
					'otec', 'salinity-gradient', 'tidal', 'wave', 'wind-energy-general', 
					'land-based-wind', 'offshore-wind'], 
			'interactions': ['attraction', 'avoidance', 'changes-sediment-transport', 
					'changes-water-quality', 'collisionevasion', 'entrapment']
			}

def scrape_all_papers(logger, fpath):
	""" Iterates through all tags and subtags to get the papers stored on Tethys.
		Inputs: folderpath to write files to
		Outputs: none (all outputs written to files)
	"""
	# Iterates through all tags and subtags
	for tag in TETHYS_TAG_SUBTAG:
		logger.info("Currently processing TAG: %s", tag)
		tag_df = pd.DataFrame(columns=TABLE_COLUMNS)
		for subtag in TETHYS_TAG_SUBTAG[tag]:
			logger.info("Currently processing SUBTAG: %s", subtag)
			subtag_df = pd.DataFrame(columns=TABLE_COLUMNS)
			# Gets data from all pages
			pagenum = 0
			while True:
				logger.info("Currently processing PAGE NUMBER: %2d", pagenum)
				page_df = scrape_page(logger, tag, subtag, pagenum=pagenum)
				if page_df is None:
					logger.info("Page number %2d not found, continuing...", pagenum)
					break
				# Adds the dataframe from this page to the subtag dataframe
				logger.debug("Merging PAGE NUMBER %2d data with SUBTAG %s data", pagenum, subtag)
				subtag_df = pd.concat([subtag_df, page_df], sort=False)
				logger.info("Finished processing PAGE NUMBER: %2d", pagenum)
				pagenum += 1
			# Writes the subtag dataframe to a file
			logger.debug("Writing SUBTAG %s data to file...", subtag)
			subtag_df.to_csv("{}{}-{}.csv".format(fpath, tag, subtag), sep=DELIM, index=False)
			# Adds the subtag dataframe to the tag dataframe
			logger.debug("Merging SUBTAG %s data with TAG %s data", subtag, tag)
			tag_df = pd.concat([tag_df, subtag_df], sort=False)
			logger.info("Finished processing SUBTAG: %s", subtag)
		# Writes the subtag dataframe to a file
		logger.debug("Writing TAG %s data to file...", tag)
		tag_df.to_csv("{}{}.csv".format(fpath, tag), sep=DELIM, index=False)
		logger.info("Finished processing TAG: %s", tag)
	return True

def scrape_page(logger, tag, subtag, pagenum=0):
	""" Gets the table data from a given tag, subtag and page number
		Reads a Tethys table in the same order as the other code, ensuring that
			the output is consistent. 
		Inputs: tag, subtag, and page number
		Outputs: dataframe of paper data from given URL
	"""
	# Gets the Tethys URL based on tag and subtag
	pagenum_suffix = "?page={}".format(pagenum) if pagenum > 0 else ""
	tag_subtag_url = TETHYS_URL.format("/" + tag, "/" + subtag, pagenum_suffix)
	# Tries to read a table from the website
	# Handles several kinds of errors
	data = None
	try: 
		data = pd.read_html(tag_subtag_url, match="Title")
	except urllib.error.HTTPError as e:
		logger.error(e)
		logger.error("This error found at URL {}".format(tag_subtag_url))
		return None
	except ValueError as e:
		logger.error(e)
		if "No tables found" in str(e):
			# A more meaningful error message than one from ValueError alone
			logger.error("URL {} does not have a table of papers".format(tag_subtag_url))
		return None
	# Warns the user that multiple tables were matched
	if len(data) > 1:
		logger.warning("WARNING: multiple tables at {} that might contain paper data".format(tag_subtag_url))
	# Assumes that the first/only match is the one with the paper data
	page_df = data[0]
	# Implicit check to see whether this is the right table
	# The wrong table may have a different number of columns
	try:
		page_df.columns = TABLE_COLUMNS
	except ValueError as e:
		logger.error(e)
		# A more meaningful error message than one from ValueError alone
		if "Length mismatch" in str(e):
			logger.error("First match of table at URL {} does not contain paper data".format(tag_subtag_url))
		return None
	# Scrapes for the external links
	logger.debug("Retrieving URLs for TAG %s SUBTAG %s PAGENUM %2d...", tag, subtag, pagenum)
	urls = scrape_page_urls(logger, tag, subtag, pagenum=pagenum)
	logger.debug("Merging URLs with the page data...")
	page_df = pd.concat([page_df, urls], axis=1, sort=False)
	# Returns the dataframe of page data
	return page_df

def scrape_page_urls(logger, tag, subtag, pagenum=0):
	""" Gets a list of URLs from a given tag, subtag and page number
		Reads a Tethys table in the same order as the other code, ensuring that
			the output is consistent. 
		Inputs: tag, subtag, and page number
		Outputs: list of all paper/report URLs to get from Tethys website
	"""
	# Builds an empty list to store the outputs
	pub_link_list = []
	# Formats the given link around the standard Tethys format
	pagenum_suffix = "?page={}".format(pagenum) if pagenum > 0 else ""
	page_url = TETHYS_URL.format("/" + tag, "/" + subtag, pagenum_suffix)
	html = None
	# Tries to look for the given link and handles errors accordingly
	try:
		html = urllib.request.urlopen(page_url).read()
	# Marked as logger.critical because the absence of URLs will break the code while running
	except urllib.error.HTTPError as e:
		logger.critical(e)
		logger.critical("CRITICAL!!! A NoneType object was returned by scrape_page_urls, " + 
				"which will break the scrape_page code at Line 131")
		return None
	soup = BeautifulSoup(html, "lxml")
	# Iterates through all <a href> tags that point to Tethys publications
	# These links are in-house locations for Tethys papers
	for link in soup.findAll('a', attrs={'href': re.compile("/publications/")}):
		# Tries to read the publication link from Tethys
		link_end = link.get('href')
		tethys_pub_link = TETHYS_URL.format(link_end, "", "")
		logger.debug("Looking for external source link at Tethys link %s", tethys_pub_link)
		tethys_pub_html = urllib.request.urlopen(tethys_pub_link).read()
		tethys_pub_soup = BeautifulSoup(tethys_pub_html, "lxml")
		# Tries to get the external publication link (ex. Wiley)
		# Default value is the original Tethys link
		pub_link = tethys_pub_link
		try:
			pub_link = tethys_pub_soup.find('a', href=True, text='External Link')['href']
			logger.debug("Found external source link %s at Tethys link %s", pub_link, tethys_pub_link)
		except TypeError:
			try:
				pub_link = tethys_pub_soup.find('a', href=True, text='Access File')['href']
				logger.debug("Found internal PDF source link %s at Tethys link %s", pub_link, tethys_pub_link)
			except TypeError:
				logger.warning("Did not find any source link at Tethys link %s", tethys_pub_link)
				logger.warning("In lieu of source link, the Tethys description link was provided.")
		# Adds the link to the list of links
		pub_link_list.append(pub_link)
	# Convert the list to a Series and return
	pub_link_series = pd.Series(pub_link_list, name="paper_url")
	return pub_link_series

def main():
	""" Run the code and time the whole process """
	# Get the command argument prompts
	prompts = sys.argv
	logdir = prompts[prompts.index('-logpath')+1]
	if logdir[-1] == '/': logdir = logdir[:-1]
	# Initialize logger
	logger = logging.getLogger(__name__)
	logger.setLevel(logging.DEBUG)
	# Set file path for logger
	handler = logging.FileHandler('{}/tethys.log'.format(logdir))
	handler.setLevel(logging.DEBUG)
	# Format the logger
	formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
	handler.setFormatter(formatter)
	# Add the new format
	logger.addHandler(handler)
	# Format the console logger
	consoleHandler = logging.StreamHandler()
	consoleHandler.setLevel(logging.INFO)
	consoleHandler.setFormatter(formatter)
	# Add the new format to the logger file
	logger.addHandler(consoleHandler)
	# Indicate which file is running
	logger.info("----------INITIALIZING NEW RUN OF %s----------", os.path.basename(__file__))
	# Save the folder path
	fpath = prompts[prompts.index('-folderpath')+1]
	if fpath[-1] != '/': fpath = fpath + "/"
	start_time = time.time()
	fpath = "/Users/openamiguel/Desktop/tethys/"
	scrape_all_papers(logger, fpath)
	end_time = time.time()
	logger.info("Full time elapsed: {0:.6f}".format(end_time - start_time))

if __name__ == "__main__":
	main()
