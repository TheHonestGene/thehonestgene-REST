import hug
import genotype
import os
from thehonestgenepipeline import imputation as imp
from thehonestgenepipeline import ancestry as anc
from celery.result import AsyncResult

STORAGE_PATH = os.environ['STORAGE_PATH'] 
genotype.STORAGE_PATH = '%s/GENOTYPES/ORIGINAL' % STORAGE_PATH

WEIGHTS = 'weights.hdf5'
PCS_FILE='1k_genomes_pcs.hdf5' 

@hug.post('/genotype')
def upload_genotype(body):
    '''Upload a genotype'''
    return genotype.get_genotype_infos(genotype.upload_genotype(body))


@hug.get('/genotype')
def get_genotype_infos(id):
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
