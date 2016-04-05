from os import environ
from configparser import ConfigParser 
from configparser import RawConfigParser

STORAGE_PATH = environ.get('STORAGE_PATH','.')
DATA_PATH = environ.get('DATA_PATH','%s/DATA' % STORAGE_PATH)
ORIGINAL_GENOTYPE_PATH = environ.get('ORIGINAL_GENOTYPE_PATH','%s/GENOTYPES/ORIGINAL' % STORAGE_PATH)
CONVERTED_GENOTYPE_PATH = environ.get('CONVERTED_GENOTYPE_PATH','%s/GENOTYPES/CONVERTED' % STORAGE_PATH)
IMPUTED_GENOTYPE_PATH = environ.get('IMPUTED_GENOTYPE_PATH','%s/GENOTYPES/IMPUTED' % STORAGE_PATH)
OAUTH_CONFIG = environ.get('OAUTH_CONFIG','./oauth.ini')
TRAIT_FILE = environ.get('TRAIT_FILE','./traits.ini')


def _can_do_oauth(opts):
    return opts['client_secret'] != '' and opts['client_id'] and opts['redirect_url']!='' and opts['scope'] != '' and opts['oauth_url'] != ''

parser = ConfigParser()
parser.read(OAUTH_CONFIG)
trait_parser = RawConfigParser()
trait_parser.read(TRAIT_FILE)


GENOTYPE_PROVIDERS = {}
providers = parser.sections()
for provider in providers:
    provider_dict = {}
    for option in parser.options(provider):
        provider_dict[option] = parser.get(provider,option)
    provider_dict['has_oauth'] =  _can_do_oauth(provider_dict)
    GENOTYPE_PROVIDERS[provider] = provider_dict



TRAITS = []
traits = trait_parser.sections()
for trait in traits:
    trait_dict = {'name':trait}
    for option in trait_parser.options(trait):
        trait_dict[option] = trait_parser.get(trait,option)
    TRAITS.append(trait_dict)
   
 
