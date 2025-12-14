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
    
    # eg item -> single portfolio object with id, appointedMinisters -> list of people for portfolio with ids
    async def enrich_portfolio_item(self,portfolio, appointedMinisters,selectedDate):
        portfolio_task = OpenGINService.get_node_data_by_id(
            self,
            entityId=portfolio.get('relatedEntityId'),
        )
        
        minister_tasks_node_details = ([
            OpenGINService.get_node_data_by_id(
                self,
                entityId=minister.get("relatedEntityId"),
            ) for  minister in appointedMinisters
        ])

        minister_tasks_is_president = ([
            OpenGINService.fetch_relation(
                self,
                entityId=minister.get('relatedEntityId'),
                relationName="AS_PRESIDENT",
                activeAt=f"{selectedDate}T00:00:00Z",
                direction="INCOMING"

                ) for  minister in appointedMinisters
        ])

        results = await asyncio.gather(portfolio_task, *minister_tasks_node_details, *minister_tasks_is_president, return_exceptions=True)

        num_ministers = len(appointedMinisters)

        portfolio_data = results[0]
        minister_data_list = results[1:1+num_ministers]
        minister_is_president = results[1+num_ministers:]
            
        if isinstance(portfolio_data, dict) and "error" not in portfolio_data:
            # retrieve the decoded portfolio name
            portfolio["decodedName"] = decode_protobuf_attribute_name(
                portfolio_data.get("name", "")
            )
            # check if the portfolio is newly created or not
            created_date = portfolio_data.get("created","")
            portfolio["isNew"] = created_date == f"{selectedDate}T00:00:00Z"
        else:
            print(f"Error fetching portfolio data: {portfolio_data}")
            portfolio["decodedName"] = "Unknown"
            portfolio["isNew"] = False
            
        portfolio["ministers"] = []
        for minister_data, minister_relation, minister in zip(minister_data_list, minister_is_president, appointedMinisters):
            if isinstance(minister_data, dict) and "error" not in minister_data:
                minister_name = decode_protobuf_attribute_name(
                    minister_data.get("name", "")
                )
                minister_id = minister_data.get("id", "")
                created_date_minister = minister_data.get("created","")
                is_new = created_date_minister == f"{selectedDate}T00:00:00Z"
                if minister_relation and len(minister_relation) > 0:
                    is_president = len(minister_relation[0]) > 0
                else:
                    is_president = False

            else:
                print(f"Error fetching minister data: {minister_data}")
                minister_name = "Unknown"
                minister_id = minister.get("relatedEntityId", "")
                is_new = False
                is_president = False

            portfolio["ministers"].append({
                "ministerId": minister_id,
                "ministerName": minister_name,
                "isNew": is_new,
                "isPresident": is_president
            })   

    async def activePortfolioList(self, presidentId, selectedDate):
       
        url = f"{self.config['BASE_URL_QUERY']}/v1/entities/{presidentId}/relations"
        headers = {"Content-Type": "application/json"}
        payload = {
            "name": "AS_MINISTER",
            "activeAt": f"{selectedDate}T00:00:00Z"
        }
        
        activePortfolioList = [] # portfolio ids
        
        async with self.session.post(url,  headers=headers, json=payload) as response:
            response.raise_for_status()
            activePortfolioList = await response.json()
        
        # get ids of people for each minister (in parallel)
        tasksforMinistersAppointed = [OpenGINService.fetch_relation(self,entityId=portfolio.get('relatedEntityId'), relationName="AS_APPOINTED", activeAt=f"{selectedDate}T00:00:00Z") for portfolio in activePortfolioList]                  
        appointedList = await asyncio.gather(*tasksforMinistersAppointed, return_exceptions=True)

        await asyncio.gather(*[
            self.enrich_portfolio_item(activePortfolioList[i], appointedList[i],selectedDate)
            for i in range(len(activePortfolioList))
        ])

        finalResult = {
            "activeMinistries": len(activePortfolioList),
            "newMinistries": 0,
            "newMinisters": 0,
            "ministriesUnderPresident": 0,
            "portfolioList" : activePortfolioList,
        }

        return finalResult