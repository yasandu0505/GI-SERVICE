from src.models import REQ_ONE

class IncomingService:
    def incoming_payload_extractor(self, REQ_ONE: REQ_ONE , ministryId ):
        return {"dataReq": REQ_ONE , "ministryId" : ministryId}
        