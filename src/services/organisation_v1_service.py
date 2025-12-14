import asyncio
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
    
    async def enrich_person_data(self, personId, selectedDate):
        """ Enrich person data when the personId and selectedData given
            - isNew attribute explains if the person is new person or not
            - isPresident attribute explains if the person is/was a president on the given selected date
        """

        person_node_data = OpenGINService.get_node_data_by_id(
            self,
            entityId=personId
        )

        person_is_president_relation = OpenGINService.fetch_relation(
                self,
                entityId=personId,
                relationName="AS_PRESIDENT",
                activeAt=f"{selectedDate}T00:00:00Z",
                direction="INCOMING"
        )

        result = await asyncio.gather(person_node_data, person_is_president_relation, return_exceptions=True)

        person_data = result[0]
        person_is_president = result[1]

        created_date_minister = person_data.get("created","")
        is_new = created_date_minister == f"{selectedDate}T00:00:00Z"
        if person_is_president and len(person_is_president) > 0:
            is_president = len(person_is_president[0]) > 0
        else:
            is_president = False

        return {
            "personId": personId,
            "personName": decode_protobuf_attribute_name(
                person_data.get("name", "Unknown")
            ),
            "isNew": is_new,
            "isPresident": is_president
        }

    # eg item -> single portfolio object with id, appointedMinisters -> list of people for portfolio with ids
    async def enrich_portfolio_item(self,portfolio, appointedMinisters,selectedDate):
        """This function takes one portolio, appointed minister list and a selected date
            - Output the portfolio by adding the ministers list with details
        """

        # task for get node details
        portfolio_task = OpenGINService.get_node_data_by_id(
            self,
            entityId=portfolio.get('relatedEntityId'),
        )

        minister_data = []
        # if the appointedMinister list is not empty (because for if there is no any minister appointed, the president for that date should be assigned)
        if(len(appointedMinisters) > 0):
            minister_data = [
                self.enrich_person_data(
                    personId=minister.get("relatedEntityId"),
                    selectedDate=selectedDate
                    ) for  minister in appointedMinisters
            ]
            # result contains portfolio_task result and minister_data results respectively
            results = await asyncio.gather(portfolio_task, *minister_data, return_exceptions=True)
            
            portfolio_data = results[0]
            minister_data_list = results[1:]
        else:
            # if the appointed minister list is empty, assign the president for that selected date
            minister_data = OpenGINService.fetch_relation(
                self,
                entityId="gov_01",
                relationName="AS_PRESIDENT",
                activeAt=f"{selectedDate}T00:00:00Z",
                direction="OUTGOING"
            )
            results = await asyncio.gather(portfolio_task, minister_data, return_exceptions=True)
            portfolio_data = results[0]
            president_relation = results[1:]

            # enrich person data
            president_enrich_task = self.enrich_person_data(
                personId=president_relation[0][0].get("relatedEntityId"),
                selectedDate=selectedDate
            )
            results_president_enrich = await asyncio.gather(president_enrich_task, return_exceptions=True)
            minister_data_list = results_president_enrich


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
        # extend the minister list with enriched person data
        portfolio["ministers"].extend(minister_data_list)

    async def activePortfolioList(self, presidentId, selectedDate):
       
        # First retrieve the relation list of the active portfolios under given president and given date    
        url = f"{self.config['BASE_URL_QUERY']}/v1/entities/{presidentId}/relations"
        headers = {"Content-Type": "application/json"}
        payload = {
            "name": "AS_MINISTER",
            "activeAt": f"{selectedDate}T00:00:00Z"
        }
        
        #portfolio relations
        activePortfolioList = []
        
        async with self.session.post(url,  headers=headers, json=payload) as response:
            response.raise_for_status()
            activePortfolioList = await response.json()
        
        # get relations of people for each portfolio (in parallel)
        tasksforMinistersAppointed = [OpenGINService.fetch_relation(self,entityId=portfolio.get('relatedEntityId'), relationName="AS_APPOINTED", activeAt=f"{selectedDate}T00:00:00Z") for portfolio in activePortfolioList]                  
        appointedList = await asyncio.gather(*tasksforMinistersAppointed, return_exceptions=True)

        # enrich all portfolios (in parallel)
        await asyncio.gather(*[
            self.enrich_portfolio_item(activePortfolioList[i], appointedList[i], selectedDate)
            for i in range(len(activePortfolioList))
        ])

        # Calculate final counts
        newMinistries = newMinisters = ministriesUnderPresident = 0

        for portfolio in activePortfolioList:
            newMinistries += portfolio.get("isNew", False)
            ministers = portfolio.get("ministers",[])
            for minister in ministers:
                newMinisters += minister.get("isNew", False)
                ministriesUnderPresident += minister.get("isPresident",False)

        # final result to return
        finalResult = {
            "activeMinistries": len(activePortfolioList),
            "newMinistries": newMinistries,
            "newMinisters": newMinisters,
            "ministriesUnderPresident": ministriesUnderPresident,
            "portfolioList" : activePortfolioList,
        }

        return finalResult