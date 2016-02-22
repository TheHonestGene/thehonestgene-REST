import uuid
import os
from imputor.core import impute
from cryptography.fernet import Fernet

STORAGE_PATH='/DATA'

def _generate_id():
    return str(uuid.uuid4())

def upload_genotype(data):
    genotype_id = _generate_id()
    filename = genotype_id

    genotype_path = os.path.join(STORAGE_PATH, '%s.hdf5' % filename)
    #data = req.stream.read()
    impute.convert_genotype_to_hdf5(data,genotype_path)
    #token = self.fernat.encrypt(data)
        #with open(genotype_path, 'wb') as genotype_file:
    #   genotype_file.write(token)
    return genotype_id
    
def get_genotype_infos(id):
    return {'genotype':id}



