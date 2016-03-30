import hug
import genotype
import os
STORAGE_PATH = os.environ['STORAGE_PATH'] 
from thehonestgenepipeline import imputation as imp
from thehonestgenepipeline import ancestry as anc
from thehonestgenepipeline import riskprediction as risk
from ancestor.core import ancestry
from celery.result import AsyncResult
import cloud
import falcon 
import json
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

# TODO extract celery state endpoint to directive or output formatter 

GENOTYPE_PROVIDERS = {'23andMe':cloud.CloudResource('d892eef346313a58fae9340140955cf0','ca3fe941649f96541961f797e5fb0beb','http://localhost:5000/receive_code/','genomes basic names','https://api.23andme.com/1/demo/')}




def check_cloud_provider(provider):
    '''checks if the provider is available'''
    if provider not in GENOTYPE_PROVIDERS:
        raise Exception('Cloud provider %s not found' % provider)
    return GENOTYPE_PROVIDERS[provider]    

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


@hug.get('/traits')
def get_available_traits():
    '''Returns the available traits for risk prediction'''
    return [{'name':'height','title':'Height','description':'When populations share genetic background and environmental factors, average height is frequently characteristic within the group. Exceptional height variation (around 20% deviation from average) within such a population is sometimes due to gigantism or dwarfism, which are medical conditions caused by specific genes or endocrine abnormalities.'}, 
    {'name':'diabetes','title':'Diabetes','description':'Diabetes mellitus (DM), commonly referred to as diabetes, is a group of metabolic diseases in which there are high blood sugar levels over a prolonged period.[2] Symptoms of high blood sugar include frequent urination, increased thirst, and increased hunger. If left untreated, diabetes can cause many complications.[3] Acute complications include diabetic ketoacidosis and nonketotic hyperosmolar coma.[4] Serious long-term complications include cardiovascular disease, stroke, chronic kidney failure, foot ulcers, and damage to the eyes.'}, 
    {'name':'schizophrenia','title':'Schizophrenia','description':'Schizophrenia is a mental disorder characterized by abnormal social behavior and failure to understand reality.[2] Common symptoms include false beliefs, unclear or confused thinking, hearing voices, reduced social engagement and emotional expression, and a lack of motivation.[2][3] People often have additional mental health problems such as anxiety disorders, major depressive illness or substance use disorder.[4] Symptoms typically come on gradually, begin in young adulthood, and last a long time.['}]

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
    pcs = _transform_pcs(ancestry.load_pcs_from_file('%s/DATA/%s' % (STORAGE_PATH,PCS_FILE)))
    if population is not None:
        return pcs[population] 
    return pcs
    
@hug.get('/plotpcs')
def get_pcs_forplotting(pc1,pc2):
   pcs = ancestry.load_pcs_from_file('%s/DATA/%s' % (STORAGE_PATH,PCS_FILE))
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
    return [
        {'name':'23andMe','logoUrl':'https://api.23andme.com/res/img/logos/icn_logo.b46b629c0c0a.png','description':'23andMe is a privately held personal genomics and biotechnology company based in Mountain View, California. The company is named for the 23 pairs of chromosomes in a normal human cell.','webpage':'https://www.23andme.com','oauthurl':'https://api.23andme.com/authorize/?redirect_uri=http://localhost:5000/receive_code/&response_type=code&client_id=ca3fe941649f96541961f797e5fb0beb&scope=genomes basic names'},
        {'name':'Opensnps','logoUrl':'https://pbs.twimg.com/profile_images/1561077286/twitter.png','webpage':'https://opensnp.org/','description':'openSNP allows customers of direct-to-customer genetic tests to publish their test results, find others with similar genetic variations, learn more about their results by getting the latest primary literature on their variations, and help scientists find new associations.','oauthurl':''},
        {'name':'Family Tree DNA','logoUrl':'https://dnaexplained.files.wordpress.com/2013/06/family-tree-dna-logo.jpg?w=584','webpage':'','description':'Family Tree DNA is a division of Gene by Gene, a commercial genetic testing company based in Houston, Texas. Family Tree DNA offers analysis of autosomal DNA, Y-DNA, and mitochondrial DNA to individuals for genealogical purposes.','oauthurl':''},
        {'name':'Ancestry','logoUrl':'http://c.mfcreative.com/mars/landing/dna/homepage/ancestrydna-logo.png','webpage':'http://dna.ancestry.com/','description':'','oauthurl':''}]
    


@hug.post('/cloud/{provider}/token')
def get_token_for_provider(provider,request):
    '''Returns access token for specified provider'''
    provider = check_cloud_provider(provider)
    token_result = provider.get_token(request.headers['CODE'])
    token_result['genotypes'] =  provider.get_genotypes(token_result['access_token'])
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
        state['data'] = res.get()
        state['state'] = res.state
    else:    
        if state['state'] == 'SUCCESS':
            state['data'] =  res.get()
    return state
    