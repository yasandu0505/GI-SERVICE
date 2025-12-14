import asyncio
import time 
from src.utils.util_functions import decode_protobuf_attribute_name
from src.services.core_opengin_service import OpenGINService
from aiohttp import ClientSession
from src.utils.http_client import http_client

class OrganisationService:
    def __init__(self, config: dict):
        self.config = config

    @property
    def session(self) -> ClientSession:
        """Access the global session"""
        return http_client.session

    async def activePortfolioList(self, presidentId, selectedDate):
       
        # eg item -> single portfolio object with id, appointedMinisters -> list of people for portfolio with ids
        async def enrich_portfolio_item(portfolio, appointedMinisters):
            portfolio_task = OpenGINService.get_node_data_by_id(
                self,
                entityId=portfolio.get('relatedEntityId'),
            )
            
            minister_tasks = [
                OpenGINService.get_node_data_by_id(
                    self,
                    entityId=m.get("relatedEntityId"),
                )
                for  m in appointedMinisters
            ]
            
            results = await asyncio.gather(portfolio_task, *minister_tasks, return_exceptions=True)
            
            portfolio_data = results[0]
            minister_data_list = results[1:]
            
            if isinstance(portfolio_data, dict) and "error" not in portfolio_data:
                portfolio["decodedName"] = decode_protobuf_attribute_name(
                    portfolio_data.get("name", "")
                )
            else:
                 print(f"Error fetching portfolio data: {portfolio_data}")
                 portfolio["decodedName"] = "Unknown"
            
            portfolio["ministers"] = []
            for minister_data, minister in zip(minister_data_list, appointedMinisters):
                if isinstance(minister_data, dict) and "error" not in minister_data:
                    minister_name = decode_protobuf_attribute_name(
                        minister_data.get("name", "")
                    )
                    minister_id = minister_data.get("id", "")
                else:
                    print(f"Error fetching minister data: {minister_data}")
                    minister_name = "Unknown"
                    minister_id = minister.get("relatedEntityId", "") # Fallback to relation ID

                portfolio["ministers"].append({
                    "ministerId": minister_id,
                    "ministerName": minister_name
                })

        url = f"{self.config['BASE_URL_QUERY']}/v1/entities/{presidentId}/relations"
        headers = {"Content-Type": "application/json"}
        payload = {
            "name": "AS_MINISTER",
            "activeAt": f"{selectedDate}T00:00:00Z"
        }
        
        activePortfolioList = [] # portfolio ids
        
        async with self.session.post(url, headers=headers, json=payload) as response:
            response.raise_for_status()
            activePortfolioList = await response.json()
        
        # get ids of people for each minister (in parallel)
        tasksforMinistersAppointed = [OpenGINService.fetch_relation(self,id=portfolio.get('relatedEntityId'), relationName="AS_APPOINTED", activeAt=f"{selectedDate}T00:00:00Z") for portfolio in activePortfolioList]                  
        appointedList = await asyncio.gather(*tasksforMinistersAppointed, return_exceptions=True)

        await asyncio.gather(*[
            enrich_portfolio_item(activePortfolioList[i], appointedList[i])
            for i in range(len(activePortfolioList))
        ])

        return activePortfolioList