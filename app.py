import hug
import genotype
import os
from thehonestgenepipeline import imputation as imp
from thehonestgenepipeline import ancestry as anc
from celery.result import AsyncResult
import cloud
import falcon 

STORAGE_PATH = os.environ['STORAGE_PATH'] 
genotype.STORAGE_PATH = '%s/GENOTYPES/ORIGINAL' % STORAGE_PATH

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


GENOTYPE_PROVIDERS = {'23andme':cloud.CloudResource('d892eef346313a58fae9340140955cf0','ca3fe941649f96541961f797e5fb0beb','http://localhost:5000/receive_code/','genomes basic names','https://api.23andme.com/1/demo/')}

@hug.directive()
def cloud_provider(provider='', **kwargs):
    '''checks if the provider is available'''
    if provider not in GENOTYPE_PROVIDERS:
        raise Exception('Cloud provider %s not found' % provider)
    return GENOTYPE_PROVIDERS[provider]    

@hug.post('/genotype')
def upload_genotype(body:getfile):
    '''Upload a genotype'''
    return genotype.get_genotype_infos(genotype.upload_genotype(body.decode("utf-8")))


@hug.get('/genotype')
def get_genotype_infos(id):
    print('get genotype')
    '''Retrieve information about a specific genotype'''
    return genotype.get_genotype_infos(id)
    
    
@hug.post('/imputation')
def run_imputation(id):
    '''Start the imputation process'''
    res = imp.imputation.delay(id)
    return {'id':res.id,'state':res.state}

@hug.get('/imputation')
def get_imputation_state(task_id):
    '''Get the current state of the imputation'''
    res = imp.imputation.AsyncResult(task_id)
    state = {'id':res.id,'state':res.state}
    if state['state'] == 'SUCCESS':
        state['data'] =  res.get()
    return state

@hug.post('/ancestry')
def run_ancestry(id):
    '''Start the ancestry'''
    res = anc.analysis.delay(id,WEIGHTS,PCS_FILE)
    return {'id':res.id,'state':res.state}

@hug.get('/ancestry')
def get_ancestry_state(task_id):
    res = anc.analysis.AsyncResult(task_id)
    state = {'id':res.id,'state':res.state}
    if state['state'] == 'SUCCESS':
        state['data'] = res.get()    
    return state


@hug.post('/cloud/{provider}/token')
def get_token_for_provider(provider:cloud_provider ):
    '''Returns access token for specified provider'''
    return provider.get_token()
    
@hug.get('/cloud/{provider}/genotypes')
def get_genotypes_for_provider(provider:cloud_provider,request):
    '''Retrieves genotypes for specified provider'''
    return provider.get_genotypes(request['ACCESS_TOKEN'])

@hug.get('/cloud/{provider}/genome/{id}')
def transfer_genome(provider:cloud_provider,request):
    '''Uploads genotype data for specified provider'''
    data = provider.get_genotype_data(request['ACCESS_TOKEN'],id)
    return genotype.get_genotype_infos(data)


