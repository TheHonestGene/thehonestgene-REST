import os
import h5py
import requests
import json
import uuid
import pandas as pd
from cryptography.fernet import Fernet

STORAGE_PATH = '%s/DATA' % os.environ['STORAGE_PATH'] 

SNP_INDEX = pd.read_csv('%s/snps_index.data' % STORAGE_PATH,skiprows=3,delimiter='\t',index_col=0,encoding='utf-8')

class CloudResource(object):

    def __init__(self,client_secret,client_id,redirect_uri,scope,url):
        self.url = url
        self.client_secret = client_secret
        self.client_id = client_id
        self.redirect_uri = redirect_uri
        self.scope = scope

    def get_token(self,code):
        payload = {'code':code,'grant_type':'authorization_code','client_id': self.client_id, 'client_secret': self.client_secret, 'redirect_uri':self.redirect_uri,'scope':self.scope}
        res = requests.post('https://api.23andme.com/token/',data=payload)
        if res.ok:
            token_dict = res.json()
            return token_dict
        else:
            raise Exception('Error retrieving token: %s' % res.text)
            
    def get_genotypes(self,access_token):
         url = '%s/user/' % self.url
         headers = {'Authorization':'Bearer %s' % access_token}
         # filter for genotyped = true and retrieve more infos
         res = requests.get(url,headers=headers)
         if res.ok:
             return res.json()['profiles']
         else:
             raise Exception('Error retrieving profiles: %s' % res.text)
    
    def get_genotype_data(self,access_token,id):
        url = '%s/genomes/%s?unfiltered=true' % (self.url,id)
        headers = {'Authorization':'Bearer %s' % access_token}
        res = requests.get(url,headers=headers)
        if res.ok:
            data = self._coordinate_with_index(res.json()['genome'])
            return data
        else:
            raise Exception('Error retrieving genomes: %s ' % res.text)
                
    def _coordinate_with_index(self,genome):
        alleles = [genome[ix:ix+2] for ix in range(0,len(genome),2)]
        p = pd.Series(alleles,name='allele')
        data = SNP_INDEX.join(p,how='inner')
        fd = data.dropna()
        fd = fd[(fd['chromosome'] != '0') & (fd['chromosome_position'] != 0)]
        return fd


        
     

