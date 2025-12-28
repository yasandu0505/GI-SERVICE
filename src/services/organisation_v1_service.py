from src.exception.exceptions import GatewayTimeoutError
from src.exception.exceptions import ServiceUnavailableError
from src.exception.exceptions import BadRequestError
from src.exception.exceptions import NotFoundError
from src.exception.exceptions import InternalServerError
import asyncio
from src.utils.util_functions import decode_protobuf_attribute_name,normalize_timestamp
from aiohttp import ClientSession
from src.utils import http_client

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
    async def enrich_person_data(self, selected_date, person_relation=None, president_id=None, is_president=False):
        """ Enrich person data when the person_relation and selected_date given
            - isNew attribute explains if the person is new person or not
            - isPresident attribute explains if the person is/was a president on the given selected date
            - If want to return the president as the default minister, no need a person_relation, just pass president_id and is_president
        """
        try:
            # handle cases where president is assigned as default
            if is_president and person_relation == None:
                entity = {"id": president_id}
                person_node_data = await self.opengin_service.get_entity_by_id(
                    entity=entity
                )

                id = president_id
                is_new = False
            else:
                entity = {"id": person_relation.get("relatedEntityId")}
                person_node_data = await self.opengin_service.get_entity_by_id(
                    entity=entity
                )
                id = person_relation.get("relatedEntityId")
                person_start_date = person_relation.get("startTime","")
                is_new = person_start_date == f"{selected_date}T00:00:00Z"

            # check if the person is president or not
            if person_node_data.get("id","") == president_id:
                is_president = True

            # decode name from protobuf
            name = decode_protobuf_attribute_name(person_node_data.get("name", "Unknown"))

            return {
                "id": id,
                "name": name,
                "isNew": is_new,
                "isPresident": is_president
            }
        except (BadRequestError, NotFoundError, ServiceUnavailableError, InternalServerError, GatewayTimeoutError):
            raise
        except Exception as e:
            print(f'Error fetching person data: {e}')
            raise InternalServerError(str(e))

    # eg: portfolio_relation -> single portfolio relation object with id, appointed_ministers_list -> list of people for portfolio with ids, president_id -> Id of the president
    async def enrich_portfolio_item(self,portfolio_relation, appointed_ministers_list, president_id, selected_date):
        """This function takes one portolio relation, appointed minister list and a selected date
            - Output the portfolio by adding the ministers list with other details
        """
        try:
            # task for get node details
            entity = {"id": portfolio_relation.get('relatedEntityId')}
            portfolio_task = self.opengin_service.get_entity_by_id(
                entity=entity,
            )

            # if the appointedMinister list is not empty (because for if there is no any minister appointed, the president for that date should be assigned)
            if(len(appointed_ministers_list) > 0):
                person_data = [
                    self.enrich_person_data(
                        person_relation=person,
                        president_id=president_id,
                        selected_date=selected_date
                        ) for  person in appointed_ministers_list
                ]
                # result contains portfolio_task result and person_data results respectively
                results = await asyncio.gather(portfolio_task, *person_data, return_exceptions=True)
                
                portfolio_data = results[0]
                person_data_list = results[1:]
            else:
                # if the appointed minister list is empty, assign the president(for that date) for that selected date
                president_enrich_task = self.enrich_person_data(
                    president_id=president_id,
                    is_president=True,
                    selected_date=selected_date
                )
                results_president_enrich = await asyncio.gather(portfolio_task, president_enrich_task, return_exceptions=True)

                portfolio_data = results_president_enrich[0]
                person_data_list = [results_president_enrich[1]]

            if isinstance(portfolio_data, dict) and "error" not in portfolio_data:
                # retrieve the decoded portfolio name
                portfolio_relation["id"] = portfolio_data.get("id","")
                portfolio_relation["name"] = decode_protobuf_attribute_name(
                    portfolio_data.get("name", "")
                )
                # check if the portfolio is newly created or not
                start_time = portfolio_relation.get("startTime","")
                portfolio_relation["isNew"] = start_time == f"{selected_date}T00:00:00Z"
            else:
                print(f"Error fetching portfolio data: {portfolio_data}")
                portfolio_relation["name"] = "Unknown"
                portfolio_relation["isNew"] = False
            
            # arrange the final portfolio details by removing unnecessary keys in the json block
            for k in ("relatedEntityId", "startTime", "endTime", "direction"):
                portfolio_relation.pop(k, None)
                
            portfolio_relation["ministers"] = []
            # extend the minister list with enriched person data
            portfolio_relation["ministers"].extend(person_data_list)

        except (BadRequestError, NotFoundError, ServiceUnavailableError, InternalServerError, GatewayTimeoutError):
            raise  
        except Exception as e:
            print(f"Error enriching portfolio item: {e}")
            raise InternalServerError(str(e))

    # this function takes the portfolio relation and get the active minister lists. then arrange the response
    async def process_portfolio_item(self, portfolio_relation, president_id, selected_date):
        try:
            appointed_ministers = await self.opengin_service.fetch_relation(
                entityId=portfolio_relation.get('relatedEntityId'),
                relationName="AS_APPOINTED",
                activeAt=f"{selected_date}T00:00:00Z"
            )
            
            await self.enrich_portfolio_item(portfolio_relation, appointed_ministers, president_id, selected_date)

        except (BadRequestError, NotFoundError, ServiceUnavailableError, InternalServerError, GatewayTimeoutError):
            raise
        except Exception as e:
            print(f"Error fetching portfolio item: {e}")
            raise InternalServerError(str(e))

    # active portfolio list
    async def active_portfolio_list(self, president_id, selected_date: str):
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
        if president_id is None or president_id == "":
            raise BadRequestError("President ID is required")

        if selected_date is None or selected_date == "":
            raise BadRequestError("Selected date is required")

        try:
            # First retrieve the relation list of the active portfolios under given president and given date     
            activePortfolioList = await self.opengin_service.fetch_relation(
                entityId=president_id,
                relationName="AS_MINISTER",
                activeAt=f"{selected_date}T00:00:00Z"
            )
            
            # Process each portfolio item in parallel
            results =await asyncio.gather(*[
                self.process_portfolio_item(portfolio, president_id, selected_date)
                for portfolio in activePortfolioList
            ], return_exceptions=True)

            # Track successes and failures
            exceptions = []
            successful_portfolios = []

            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    exceptions.append({
                        "portfolioId": activePortfolioList[i].get('id', 'unknown'),
                        "error": str(result)
                    })
                    print(f"Error processing portfolio {activePortfolioList[i].get('id')}: {result}")
                else:
                    successful_portfolios.append(activePortfolioList[i])
            
            if len(exceptions) == len(results):
                raise InternalServerError("Failed to process all portfolios")

            # Calculate final counts
            newMinistries = newMinisters = ministriesUnderPresident = 0

            for portfolio in successful_portfolios:
                newMinistries += portfolio.get("isNew", False)
                ministers = portfolio.get("ministers",[])
                for minister in ministers:
                    if isinstance(minister, dict):
                        newMinisters += minister.get("isNew", False)
                        ministriesUnderPresident += minister.get("isPresident",False)

            # final result to return
            finalResult = {
                "activeMinistries": len(successful_portfolios),
                "newMinistries": newMinistries,
                "newMinisters": newMinisters,
                "ministriesUnderPresident": ministriesUnderPresident,
                "portfolioList" : successful_portfolios,
            }

            return finalResult

        except (BadRequestError, NotFoundError, ServiceUnavailableError, InternalServerError, GatewayTimeoutError):
            raise
        except Exception as e:
            raise InternalServerError(str(e))
    
    # helper: enrich department
    async def enrich_department_item(self, department_relation, selected_date):

        department_id = department_relation.get("relatedEntityId")

        entity = {"id": department_id}
        department_data_task = self.opengin_service.get_entity_by_id(entity=entity)
        dataset_task = self.opengin_service.fetch_relation(entityId=department_id, relationName="AS_CATEGORY")

        # run parallel calls to get department data and parent category relations to ensure the department has data
        department_data, dataset_relations = await asyncio.gather(department_data_task, dataset_task, return_exceptions=True)

        # decode name
        name = decode_protobuf_attribute_name(department_data.get("name", "Unknown"))
            
        # check the department is new or not
        department_start_date = department_relation.get("startTime","")
        is_new = department_start_date == f"{selected_date}T00:00:00Z"

        # check the department has data or not
        has_data = bool(dataset_relations)
            
        final_result = {
            "id": department_id,
            "name": name,
            "isNew": is_new,
            "hasData": has_data
        }

        return final_result

    # API: departments by portfolio
    async def departments_by_portfolio(self, portfolio_id, selected_date):
        """
        Docstring for department_by_portfolio
        
        :param portfolio_id: Portfolio Id
        :param selected_date: Selected Date

        output type: 
        {
            "totalDepartments": 0,
            "newDepartments": 0,
            "departmentList": [
                {
                "id": "",
                "name": "",
                "isNew": false,
                "hasData": false
                },
            ]
        }
        """

        if portfolio_id is None or portfolio_id == "":
            raise BadRequestError("Portfolio ID is required")

        if selected_date is None or selected_date == "":
            raise BadRequestError("Selected date is required")

        try:
            department_relation_list = await self.opengin_service.fetch_relation(
                entityId=portfolio_id,
                relationName="AS_DEPARTMENT",
                activeAt=f"{selected_date}T00:00:00Z"
            )
            
            # tasks to run in parallel
            enrich_department_tasks = [
                self.enrich_department_item(department_relation=department_relation, selected_date=selected_date)
                for department_relation in department_relation_list
            ]

            results = await asyncio.gather(*enrich_department_tasks, return_exceptions=True)

            departments = [
                r for r in results if not isinstance(r, Exception)
            ]

            # Calculate final counts
            new_departments = sum(1 for d in departments if d.get("isNew"))

            # final departments to return
            finalResult = {
                "totalDepartments": len(departments),
                "newDepartments": new_departments,
                "departmentList" : departments,
            }

            return finalResult

        except (BadRequestError, NotFoundError, ServiceUnavailableError, InternalServerError, GatewayTimeoutError):
            raise
        except Exception as e:
            raise InternalServerError(str(e))