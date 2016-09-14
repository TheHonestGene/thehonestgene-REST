"""
Common utlity functions
"""
from os import environ
from configparser import ConfigParser
from configparser import RawConfigParser
import csv
import numpy as np


STORAGE_PATH = environ.get('STORAGE_PATH', '.')
DATA_PATH = environ.get('DATA_PATH', '%s/DATA' % STORAGE_PATH)
ORIGINAL_GENOTYPE_PATH = environ.get('ORIGINAL_GENOTYPE_PATH', '%s/GENOTYPES/ORIGINAL' % STORAGE_PATH)
CONVERTED_GENOTYPE_PATH = environ.get('CONVERTED_GENOTYPE_PATH', '%s/GENOTYPES/CONVERTED' % STORAGE_PATH)
IMPUTED_GENOTYPE_PATH = environ.get('IMPUTED_GENOTYPE_PATH', '%s/GENOTYPES/IMPUTED' % STORAGE_PATH)
OAUTH_CONFIG = environ.get('OAUTH_CONFIG', './oauth.ini')
TRAIT_FILE = environ.get('TRAIT_FILE', './traits.ini')


def _can_do_oauth(opts):
    return opts['client_secret'] != '' and opts['client_id'] and opts['redirect_url'] != '' and opts['scope'] != '' and opts['oauth_url'] != ''

def _get_trait_histogram(trait_name):
    histogram = [['Risk']]
    with open('%s/%s.csv' % (DATA_PATH, trait_name)) as fhandle:
        for row in csv.reader(fhandle):
            histogram.append([float(row[0])])
    return histogram

PARSER = ConfigParser()
PARSER.read(OAUTH_CONFIG)
TRAIT_PARSER = RawConfigParser()
TRAIT_PARSER.read(TRAIT_FILE)


GENOTYPE_PROVIDERS = {}
for provider in PARSER.sections():
    provider_dict = {}
    for option in PARSER.options(provider):
        provider_dict[option] = PARSER.get(provider, option)
    provider_dict['has_oauth'] = _can_do_oauth(provider_dict)
    GENOTYPE_PROVIDERS[provider] = provider_dict



TRAITS = []
for trait in TRAIT_PARSER.sections():
    trait_dict = {'name':trait}
    for option in TRAIT_PARSER.options(trait):
        if option == 'quantitive':
            trait_dict[option] = TRAIT_PARSER.getboolean(trait, option)
        else:
            trait_dict[option] = TRAIT_PARSER.get(trait, option)
    trait_dict['histogram'] = _get_trait_histogram(trait)
    trait_dict['meanRisk'] = float(np.mean(np.asarray(trait_dict['histogram'][1:])))
    TRAITS.append(trait_dict)
