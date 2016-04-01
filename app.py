import hug
import genotype
import os
import settings

from thehonestgenepipeline import imputation as imp
from thehonestgenepipeline import ancestry as anc
from thehonestgenepipeline import riskprediction as risk
from ancestor.core import ancestry
from celery.result import AsyncResult
import cloud
import falcon 
import json
import os
from configparser import ConfigParser

parser = ConfigParser()
parser.read(settings.OAUTH_CONFIG)


WEIGHTS = 'european_weights.hdf5'
PCS_FILE='hapmap_european_pcs.hdf5' 
import cgi

@hug.directive()
def getfile(name='file', request = None,**kwargs):
    '''Returns passed in parameter multiplied by itself'''
    # TODO: Either validate that content type is multipart/form-data
    # here, or in another hook before allowing execution to proceed.

    # This must be done to avoid a bug in cgi.FieldStorage
    env = request.env
    env.setdefault('QUERY_STRING', '')

    # TODO: Add error handling, when the request is not formatted
    # correctly or does not contain the desired field...

    # TODO: Consider overriding make_file, so that you can
    # stream directly to the destination rather than
    # buffering using TemporaryFile (see http://goo.gl/Yo8h3P)
    form = cgi.FieldStorage(fp=request.stream, environ=env)

    file_item = form[name]
    if file_item.file:
        body = file_item.file.read()
        # It's an uploaded file
        return body
    else:
        # TODO: Raise an error
        pass

# TODO extract celery state endpoint to directive or output formatter

def _can_do_oauth(opts):
    print(opts)
    return opts['client_secret'] != '' and opts['client_id'] and opts['redirect_url']!='' and opts['scope'] != '' and opts['oauth_url'] != ''

def _retrieve_gentoype_providers(parser):
    providers = parser.sections()
    data = {}
    for provider in providers:
        provider_dict = {}
        for option in parser.options(provider):
            provider_dict[option] = parser.get(provider,option)
        data[provider] = provider_dict
    return data
    

GENOTYPE_PROVIDERS = _retrieve_gentoype_providers(parser)
OAUTH_PROVIDERS = {}  
for provider,opts in GENOTYPE_PROVIDERS.items():
     if _can_do_oauth(opts):
        OAUTH_PROVIDERS[provider] = cloud.CloudResource(opts['client_secret'],opts['client_id'],opts['redirect_url'],opts['scope'],opts['oauth_url'])

def check_cloud_provider(provider):
    '''checks if the provider is available'''
    if provider not in OAUTH_PROVIDERS:
        raise Exception('Cloud provider %s not found' % provider)
    return OAUTH_PROVIDERS[provider]    

@hug.post('/id')
def generate_id():
    '''Generate a unique id for identifiying an analysis run'''
    return {'id':genotype.generate_id()}

@hug.post('/genotype/{id}')
def upload_genotype(body:getfile,id):
    '''Upload a genotype'''
    return genotype.get_genotype_infos(genotype.upload_genotype(body.decode("utf-8"),id))


@hug.get('/genotype/{id}')
def get_genotype_infos(id):
    '''Retrieve information about a specific genotype'''
    return genotype.get_genotype_infos(id)
    

@hug.post('/imputation/cancel/{task_id}')
def cancel_imputation(task_id):
    '''Cancel a running process'''
    res = imp.imputation.AsyncResult(task_id)
    res.revoke(terminate=True)
    return {}
    
@hug.post('/imputation')
def run_imputation(id):
    '''Start the imputation process'''
    res = imp.imputation.delay(id)
    return {'id':res.id,'state':res.state}

@hug.get('/imputation')
def get_imputation_state(task_id,wait=False):
    '''Get the current state of the imputation'''
    res = imp.imputation.AsyncResult(task_id)
    state = _retrieveTaskState(res,wait)
    if state['state'] == 'SUCCESS' and 'data' in state:
        state['data']['imputation']['num_imputed_snps'] = sum(state['data']['imputation']['chr_stats'].values())

    return state

@hug.post('/ancestry')
def run_ancestry(id):
    '''Start the ancestry'''
    res = anc.analysis.delay(id,WEIGHTS,PCS_FILE)
    return {'id':res.id,'state':res.state}

@hug.get('/ancestry')
def get_ancestry_state(task_id,wait=False):
    '''Returns the current state of the ancestry analysis'''
    res = anc.analysis.AsyncResult(task_id)
    state = _retrieveTaskState(res,wait)    
    return state
    
@hug.post('/ancestry/cancel/{task_id}')
def cancel_ancestry(task_id):
    '''Cancel a running ancestry'''
    res = risk.run.AsyncResult(task_id)
    res.revoke(terminate=True)
    return {}


@hug.get('/traits')
def get_available_traits():
    '''Returns the available traits for risk prediction'''
    return [{'name':'height','title':'Height','description':'When populations share genetic background and environmental factors, average height is frequently characteristic within the group. Exceptional height variation (around 20% deviation from average) within such a population is sometimes due to gigantism or dwarfism, which are medical conditions caused by specific genes or endocrine abnormalities.'}, 
    {'name':'diabetes','title':'Diabetes','description':'Diabetes mellitus (DM), commonly referred to as diabetes, is a group of metabolic diseases in which there are high blood sugar levels over a prolonged period.[2] Symptoms of high blood sugar include frequent urination, increased thirst, and increased hunger. If left untreated, diabetes can cause many complications.[3] Acute complications include diabetic ketoacidosis and nonketotic hyperosmolar coma.[4] Serious long-term complications include cardiovascular disease, stroke, chronic kidney failure, foot ulcers, and damage to the eyes.'}, 
    {'name':'schizophrenia','title':'Schizophrenia','description':'Schizophrenia is a mental disorder characterized by abnormal social behavior and failure to understand reality.[2] Common symptoms include false beliefs, unclear or confused thinking, hearing voices, reduced social engagement and emotional expression, and a lack of motivation.[2][3] People often have additional mental health problems such as anxiety disorders, major depressive illness or substance use disorder.[4] Symptoms typically come on gradually, begin in young adulthood, and last a long time.['}]



@hug.post('/riskprediction/cancel/{task_id}')
def cancel_prediction(task_id):
    '''Cancel a running prediction'''
    res = risk.run.AsyncResult(task_id)
    res.revoke(terminate=True)
    return {}

@hug.post('/riskprediction/{id}/{trait}')
def run_prediction(id,trait):
    '''Returns the available traits for risk prediction'''
    # TODO check if trait is available
    res = risk.run.delay(id,trait)
    return {'id':res.id,'state':res.state}

@hug.get('/riskprediction')
def get_prediction_state(task_id,wait=False):
    '''Returns the current state of the risk prediction analysis'''
    res = risk.run.AsyncResult(task_id)
    state = _retrieveTaskState(res,wait)    
    return state

@hug.get('/pcs')
def get_pcs_for_population(population=None):
    '''Returns the PCS for a given population'''
    pcs = _transform_pcs(ancestry.load_pcs_from_file('%s/%s' % (settings.DATA_PATH,PCS_FILE)))
    if population is not None:
        return pcs[population] 
    return pcs
    
@hug.get('/plotpcs')
def get_pcs_forplotting(pc1,pc2):
   pcs = ancestry.load_pcs_from_file('%s/%s' % (settings.DATA_PATH,PCS_FILE))
   num_pop = len(pcs['populations'].keys())
   header = ['PC1']
   data =  []
   data.append(header)
   for population, mask in pcs['populations'].items():
       header.append(population)
       pc2_ix = len(header)-1
       pop_pcs = pcs['pcs'][mask]
       for pc in pop_pcs:
           value = [pc[0]]
           value.extend([None]*(pc2_ix-1))
           value.append(pc[1])
           value.extend([None]*(num_pop - pc2_ix +1))
           data.append(value)
   header.append('That\'s you')
   value = [None]*(num_pop+2)
   value[0] = float(pc1)
   value[-1] = float(pc2)
   data.append(value)
   return data     
       
       

def _transform_pcs(pcs):
    transformed_pcs =  {}
    for population, mask in pcs['populations'].items():
        transformed_pcs[population] = pcs['pcs'][mask].tolist()
    return transformed_pcs


@hug.get('/cloud')
def get_available_cloud_providers():
    '''Returns available cloud providers'''
    return [{'name':provider,'logoUrl':opts.get('logo_url',''),'description':opts.get('description',''),'webpage':opts.get('webpage',''),'clientId':opts.get('client_id'),'redirectUrl':opts.get('redirect_url'),'tokenurl':opts.get('token_url',''),'scope':opts.get('scope',''),'oauthSupported':provider in OAUTH_PROVIDERS} for (provider,opts) in GENOTYPE_PROVIDERS.items()]
    


@hug.post('/cloud/{provider}/token')
def get_token_for_provider(provider,request):
    '''Returns access token for specified provider'''
    provider = check_cloud_provider(provider)
    token_result = provider.get_token(request.headers['CODE'])
    token_result['userInfo'] =  provider.get_genotypes(token_result['access_token'])
    return token_result     
    
@hug.get('/cloud/{provider}/genotypes')
def get_genotypes_for_provider(provider,request):
    '''Retrieves genotypes for specified provider'''
    provider = check_cloud_provider(provider)
    return provider.get_genotypes(request.headers['ACCESS-TOKEN'])

@hug.post('/cloud/{provider}/genome/{genotypeid}/{id}')
def transfer_genome(provider,genotypeid,id,request):
    '''Uploads genotype data for specified provider'''
    provider = check_cloud_provider(provider)
    data = provider.get_genotype_data(request.headers['ACCESS-TOKEN'],genotypeid)
    columns = data.columns.values.tolist() 
    data.sort([columns[1],columns[2]],inplace=True)
    return genotype.get_genotype_infos(genotype.upload_genotype(data.to_csv(sep='\t',index=False,header=False),id))


def _retrieveTaskState(res,wait=False):
    state = {'id':res.id,'state':res.state}
    if wait:
        state['data'] = res.get(timeout=60)
        state['state'] = res.state
    else:    
        if state['state'] == 'SUCCESS':
            state['data'] =  res.get(no_ack=False)
    return state
    