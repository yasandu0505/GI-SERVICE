import asyncio
from src.utils.util_functions import decode_protobuf_attribute_name
from aiohttp import ClientSession
from src.utils.http_client import http_client

class OrganisationService:
    """
    This service is responsible for executing aggregate functions by calling the OpenGINService and processing the returned data.
    """
    def __init__(self, config: dict, opengin_service):
        self.config = config
        self.opengin_service = opengin_service

    @property
    def session(self) -> ClientSession:
        """Access the global session"""
        return http_client.session
    
    # enrich person data
    async def enrich_person_data(self, person, selected_date, president_id=None):
        """ Enrich person data when the personId and selected_data given
            - isNew attribute explains if the person is new person or not
            - isPresident attribute explains if the person is/was a president on the given selected date
        """

        person_node_data = self.opengin_service.get_entity_by_id(
            entityId=person.get("relatedEntityId")
        )

        result = await asyncio.gather(person_node_data, return_exceptions=True)

        person_data = result[0]

        # check if the person is president or not
        is_president = False
        if person_data.get("id","") == president_id:
            is_president = True

        # check if the person is newly appointed or not
        minister_start_date = person.get("startTime","")
        is_new = minister_start_date == f"{selected_date}T00:00:00Z"

        return {
            "id": person.get("relatedEntityId"),
            "name": decode_protobuf_attribute_name(
                person_data.get("name", "Unknown")
            ),
            "isNew": is_new,
            "isPresident": is_president
        }

    # eg item -> single portfolio object with id, appointedMinisters -> list of people for portfolio with ids
    async def enrich_portfolio_item(self,portfolio, appointedMinisters, president_id, selected_date):
        """This function takes one portolio, appointed minister list and a selected date
            - Output the portfolio by adding the ministers list with other details
        """

        # task for get node details
        portfolio_task = self.opengin_service.get_entity_by_id(
            entityId=portfolio.get('relatedEntityId'),
        )

        minister_data = []
        # if the appointedMinister list is not empty (because for if there is no any minister appointed, the president for that date should be assigned)
        if(len(appointedMinisters) > 0):
            minister_data = [
                self.enrich_person_data(
                    person=minister,
                    president_id=president_id,
                    selected_date=selected_date
                    ) for  minister in appointedMinisters
            ]
            # result contains portfolio_task result and minister_data results respectively
            results = await asyncio.gather(portfolio_task, *minister_data, return_exceptions=True)
            
            portfolio_data = results[0]
            minister_data_list = results[1:]
        else:
            # if the appointed minister list is empty, assign the president for that selected date
            minister_data = self.opengin_service.fetch_relation(
                entityId="gov_01",
                relationName="AS_PRESIDENT",
                activeAt=f"{selected_date}T00:00:00Z",
                direction="OUTGOING"
            )
            results = await asyncio.gather(portfolio_task, minister_data, return_exceptions=True)
            portfolio_data = results[0]
            president_relation = results[1:]

            # enrich person data
            president_enrich_task = self.enrich_person_data(
                president_id=president_id,
                person=president_relation[0][0],
                selected_date=selected_date
            )
            results_president_enrich = await asyncio.gather(president_enrich_task, return_exceptions=True)
            minister_data_list = results_president_enrich

        if isinstance(portfolio_data, dict) and "error" not in portfolio_data:
            # retrieve the decoded portfolio name
            portfolio["id"] = portfolio_data.get("id","")
            portfolio["name"] = decode_protobuf_attribute_name(
                portfolio_data.get("name", "")
            )
            # check if the portfolio is newly created or not
            start_time = portfolio.get("startTime","")
            portfolio["isNew"] = start_time == f"{selected_date}T00:00:00Z"
        else:
            print(f"Error fetching portfolio data: {portfolio_data}")
            portfolio["name"] = "Unknown"
            portfolio["isNew"] = False
        
        # arrange the final portfolio details by removing unnecessary keys in the json block
        for k in ("relatedEntityId", "startTime", "endTime", "direction"):
            portfolio.pop(k, None)
            
        portfolio["ministers"] = []
        # extend the minister list with enriched person data
        portfolio["ministers"].extend(minister_data_list)

    # active portfolio list
    async def active_portfolio_list(self, president_id, selected_date):
        """
        Docstring for activePortfolioList
        
        :param president_id: President Id
        :param selected_date: Selected Date

        output type: 
        {
            "activeMinistries": 0,
            "newMinistries": 0,
            "newMinisters": 0,
            "ministriesUnderPresident": 0,
            "portfolioList": [
                {
                "id": "",
                "name": "",
                "isNew": false,
                "ministers": [
                    {
                    "id": "",
                    "name": "",
                    "isNew": false,
                    "isPresident": false
                    }
                ]
                },
            ]
        }
        """
        # First retrieve the relation list of the active portfolios under given president and given date    
        url = f"{self.config['BASE_URL_QUERY']}/v1/entities/{president_id}/relations"
        headers = {"Content-Type": "application/json"}
        payload = {
            "name": "AS_MINISTER",
            "activeAt": f"{selected_date}T00:00:00Z"
        }
        
        #portfolio relations
        activePortfolioList = []
        
        async with self.session.post(url,  headers=headers, json=payload) as response:
            response.raise_for_status()
            activePortfolioList = await response.json()
        
        # get relations of people for each portfolio (in parallel)
        tasksforMinistersAppointed = [self.opengin_service.fetch_relation(entityId=portfolio.get('relatedEntityId'), relationName="AS_APPOINTED", activeAt=f"{selected_date}T00:00:00Z") for portfolio in activePortfolioList]                  
        appointedList = await asyncio.gather(*tasksforMinistersAppointed, return_exceptions=True)

        # enrich all portfolios (in parallel)
        await asyncio.gather(*[
            self.enrich_portfolio_item(activePortfolioList[i], appointedList[i], president_id, selected_date)
            for i in range(len(activePortfolioList))
        ])

        # Calculate final counts
        newMinistries = newMinisters = ministriesUnderPresident = 0

        for portfolio in activePortfolioList:
            newMinistries += portfolio.get("isNew", False)
            ministers = portfolio.get("ministers",[])
            for minister in ministers:
                if minister:
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