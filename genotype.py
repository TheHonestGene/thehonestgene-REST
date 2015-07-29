import falcon
import uuid
import os
import h5py
from cryptography.fernet import Fernet

def _generate_id():
    return str(uuid.uuid4())



class GenotypeResource(object):

    def __init__(self,storage_path,key):
        self.storage_path = storage_path
        self.fernat = Fernet(key)

    def on_post(self,req,resp):
        genotype_id = _generate_id()
        filename = genotype_id
     
        genotype_path = os.path.join(self.storage_path, filename)
        data = req.stream.read()
        self.convert_to_hdf5(data,genotype_path)
        #token = self.fernat.encrypt(data)
        #with open(genotype_path, 'wb') as genotype_file:
         #   genotype_file.write(token)

        resp.status = falcon.HTTP_201
        resp.body = genotype_id

    def on_get(self,req,resp):
        resp.body = '{"message": "Hello world!"}'
        resp.status = falcon.HTTP_200
        
    
    def convert_to_hdf5(self,data,genotype_id):
        csv_content = data.decode("utf-8")
        start_chr = None
        f = h5py.File('%s.hdf5' % genotype_id)
        pos_snps =[]
        
        for row in csv_content.splitlines():
            if len(row) == 0 or row[0] == '#':
                continue
            cols = row.split("\t")
            if len(cols) != 4:
                continue        
            if cols[1] != start_chr:
                if start_chr is not None:
                    sorted(pos_snps, key=lambda x: x[0])
                    positions,snps = zip(*pos_snps)
                    pos_dset = group.create_dataset('positions',(len(positions),),chunks=True,compression='lzf',dtype='i8',data=positions)
                    snp_dset = group.create_dataset('snps',(len(positions),),chunks=True,compression='lzf',dtype='S2',data=snps)
                    pos_snps =[]   
                start_chr = cols[1]
                group = f.create_group("Chr%s" % start_chr)
            pos_snps.append((int(cols[2]),cols[3].encode('utf-8')))
        sorted(pos_snps, key=lambda x: x[0])
        positions,snps = zip(*pos_snps)   
        pos_dset = group.create_dataset('positions',(len(positions),),chunks=True,compression='lzf',dtype='i8',data=positions)
        snp_dset = group.create_dataset('snps',(len(positions),),chunks=True,compression='lzf',dtype='S2',data=snps)
                
            
            
        