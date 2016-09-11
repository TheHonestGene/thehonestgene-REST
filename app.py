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


OAUTH_PROVIDERS ={}
for provider,opts in settings.GENOTYPE_PROVIDERS.items():
     if opts['has_oauth']:
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


@hug.post('/imputation/{id}/cancel/{task_id}')
def cancel_imputation(id,task_id):
    '''Cancel a running process'''
    res = imp.imputation.AsyncResult(task_id)
    res.revoke(terminate=True)
    return {}

@hug.post('/imputation/{id}')
def run_imputation(id):
    '''Start the imputation process'''
    res = imp.imputation.delay(id)
    return {'id':res.id,'state':res.state}

@hug.get('/imputation/{id}/state/{task_id}')
def get_imputation_state(id,task_id,wait=False):
    '''Get the current state of the imputation'''
    res = imp.imputation.AsyncResult(task_id)
    state = _retrieveTaskState(res,wait)
    if state['state'] == 'SUCCESS' and 'data' in state:
        state['data']['imputation']['num_imputed_snps'] = sum(state['data']['imputation']['chr_stats'].values())

    return state

@hug.post('/ancestry/{id}')
def run_ancestry(id):
    '''Start the ancestry'''
    res = anc.analysis.delay(id,'world',check_population='EUR')
    return {'id':res.id,'state':res.state}

@hug.get('/ancestry/{id}/state/{task_id}')
def get_ancestry_state(id,task_id,wait=False):
    '''Returns the current state of the ancestry analysis'''
    res = anc.analysis.AsyncResult(task_id)
    state = _retrieveTaskState(res,wait)
    return state

@hug.post('/ancestry/{id}/cancel/{task_id}')
def cancel_ancestry(id,task_id):
    '''Cancel a running ancestry'''
    res = risk.run.AsyncResult(task_id)
    res.revoke(terminate=True)
    return {}


@hug.get('/traits')
def get_available_traits():
    '''Returns the available traits for risk prediction'''
    return settings.TRAITS



@hug.post('/riskprediction/{id}/cancel/{task_id}')
def cancel_prediction(id,task_id):
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

@hug.get('/riskprediction/{id}/state/{task_id}')
def get_prediction_state(id,task_id,wait=False):
    '''Returns the current state of the risk prediction analysis'''
    res = risk.run.AsyncResult(task_id)
    state = _retrieveTaskState(res,wait)
    return state

@hug.get('/pcs')
def get_pcs_for_population(platform,region='world',population=None):
    '''Returns the PCS for a given population'''
    pcs = _transform_pcs(ancestry.load_pcs_from_file('%s/AN_DATA/hapmap_%s_%s_pcs.hdf5' % (settings.DATA_PATH,platform,region)))
    if population is not None:
        return pcs[population]
    return pcs

@hug.get('/plotpcs')
def get_pcs_forplotting(platform,pc1,pc2,region='world'):
   pcs = ancestry.load_pcs_from_file('%s/AN_DATA/hapmap_%s_%s_pcs.hdf5' % (settings.DATA_PATH,platform,region))
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
   header.append('YOU')
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
    return [{'name':provider,'title':opts.get('title',provider),'logoUrl':opts.get('logo_url',''),'description':opts.get('description',''),'webpage':opts.get('webpage',''),'clientId':opts.get('client_id'),'redirectUrl':opts.get('redirect_url'),'tokenurl':opts.get('token_url',''),'scope':opts.get('scope',''),'oauthSupported':opts.get('has_oauth',False)} for (provider,opts) in settings.GENOTYPE_PROVIDERS.items()]



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
    source = provider
    provider = check_cloud_provider(provider)
    data = provider.get_genotype_data(request.headers['ACCESS-TOKEN'],genotypeid)
    columns = data.columns.values.tolist()
    data.sort([columns[1],columns[2]],inplace=True)
    return genotype.get_genotype_infos(genotype.upload_genotype(data.to_csv(sep='\t',index=False,header=False),id,source))


def _retrieveTaskState(res,wait=False):
    state = {'id':res.id,'state':res.state}
    if wait:
        state['data'] = res.get(timeout=60)
        state['state'] = res.state
    else:
        if state['state'] == 'SUCCESS':
            state['data'] =  res.get(no_ack=False)
    return state
