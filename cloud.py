import os
import h5py
import requests
import json
import uuid
import pandas as pd
from cryptography.fernet import Fernet
from settings import DATA_PATH

SNP_INDEX = pd.read_csv('%s/snps_index.data' % DATA_PATH,skiprows=3,delimiter='\t',index_col=0,encoding='utf-8')

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
        # retrieve account
        account =self._get_account(access_token)
        account_map = {profile['id']:ix for ix,profile in enumerate(account['profiles'])}
        # retrieve profile infos
        profile_infos = self._get_account_info(access_token)
        if profile_infos is not None:
            account['firstname'] = profile_infos['first_name']
            account['lastname'] = profile_infos['last_name']
            for geno_prof in profile_infos['profiles']:
                account['profiles'][account_map[geno_prof['id']]]['firstname'] = geno_prof['first_name'] 
                account['profiles'][account_map[geno_prof['id']]]['lastname'] = geno_prof['last_name']
        for profile in account['profiles']:
            id = profile['id']
            account['profiles'][account_map[id]]['pics'] = self._get_profile_pic(access_token,id)   
        return account
        
        
         
    
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
        
        
    def _get_account_info(self,access_token):
        url = '%s/names/' % self.url
        headers = {'Authorization':'Bearer %s' % access_token}
        # filter for genotyped = true and retrieve more infos
        res = requests.get(url,headers=headers)
        if res.ok:
            return res.json()
        return None
        
        
    def _get_account(self,access_token):
        url = '%s/user/' % self.url
        headers = {'Authorization':'Bearer %s' % access_token}
        # filter for genotyped = true and retrieve more infos
        res = requests.get(url,headers=headers)
        if res.ok:
            return res.json()
        else:
            raise Exception('Error retrieving profiles: %s' % res.text)
    
    
    def _get_profile_pic(self,access_token,profile_id):
        url = '%s/profile_picture/%s' % (self.url, profile_id) 
        headers = {'Authorization':'Bearer %s' % access_token}
        res = requests.get(url,headers=headers)
        if res.ok:
            data = res.json()
            return res.json()
        return []

        
     

