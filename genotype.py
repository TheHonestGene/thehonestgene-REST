import uuid
import os
from imputor.core import impute
from imputor.core import genotype_parser
from cryptography.fernet import Fernet
import h5py
import pandas as pd
from settings import ORIGINAL_GENOTYPE_PATH


def generate_id():
    while True:
        uid = str(uuid.uuid4())
        if os.path.exists(os.path.join(ORIGINAL_GENOTYPE_PATH, '%s.hdf5' % uid)) == False:
            return uid

def upload_genotype(data,id,source=None):
    #id = generate_id()
    filename = id
    genotype_path = os.path.join(ORIGINAL_GENOTYPE_PATH, '%s.hdf5' % filename)
    #data = req.stream.read()
    genotype_parser.convert_genotype_to_hdf5(data,genotype_path,source)
    #token = self.fernat.encrypt(data)
        #with open(genotype_path, 'wb') as genotype_file:
    #   genotype_file.write(token)
    return id
    
def get_genotype_infos(id):
    f = None
    data = {}
    try:
        f = h5py.File('%s/%s.hdf5' % (ORIGINAL_GENOTYPE_PATH,id),'r')
        source = f.attrs.get('source',None)
        version = f.attrs.get('version',None)
        if 'gender' in f.attrs:
            gender = f.attrs['gender']
        else:
            gender = 'm' if 'ChrY' in f.keys()  else 'f'
        num_of_snps = 0
        chr_stats = {}
        for i in range(1,23):
            chr = f['Chr%s' % i]
            chr_data = {'num_of_snps':len(chr['positions']),'annotations':None}
            num_of_snps+= chr_data['num_of_snps']
            s = pd.Series(chr['snps'])
            annotations = s.groupby(s.values).count().to_dict()
            annotations = {key.decode():int(val) for key,val in annotations.items() }
            chr_data['annotations'] = annotations
            chr_stats['Chr%s'%i] = chr_data
        data['chr_stats'] = chr_stats
        data['num_of_snps'] = num_of_snps    
        data['source'] = source
        data['version'] = version
        data['gender'] = gender    
    finally:
        if f:
            f.close()
    return {'genotype':id,'data':data}



