import falcon
import genotype

api = application = falcon.API()

api.add_route('/genotype',genotype.GenotypeResource('data','EKegFrJF7wsjBL8Z9HX0JiGlLEuwkNTsV5D5qjhy5Pk='))
