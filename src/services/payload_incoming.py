from src.models import REQ_ONE

class IncomingService:
    def incoming_payload_extractor(self, REQ_ONE: REQ_ONE , ministryId ):
        year = REQ_ONE.year
        year = REQ_ONE.year
        govId = REQ_ONE.govId
        presidentId = REQ_ONE.presidentId
        dataSet = REQ_ONE.dataSet 
            
        return {
            "year" : year,
            "govId" : govId,
            "presidentId" : presidentId,
            "dataSet" : dataSet,
            "ministryId" : ministryId
        }
    
    def query_aggregator(self, extracted_data):
        print(extracted_data)
        
    
        return {
            "extracted_data" : extracted_data
        }
    
    
    
    
    
        