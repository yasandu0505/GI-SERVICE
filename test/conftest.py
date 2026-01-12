
from src.services.data_service import DataService
import pytest
from aiohttp import ClientError
from unittest.mock import patch, PropertyMock, MagicMock
from services.opengin_service import OpenGINService
from src.utils.http_client import HTTPClient
from services.organisation_service import OrganisationService
from unittest.mock import AsyncMock
from src.utils.util_functions import Util

# MockResponse class to simulate aiohttp responses
class MockResponse:
    def __init__(self, json_data, status=200):
        self._json_data = json_data
        self.status = status

    async def json(self):
        return self._json_data

    def raise_for_status(self):
        if self.status >= 400:
            raise ClientError("HTTP error")

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        pass

# Fixture for OpenGINService tests
@pytest.fixture
def mock_session():
    """Fixture that provides a mocked session and patches HTTPClient"""
    session = MagicMock() 
    with patch.object(HTTPClient, 'session', new_callable=PropertyMock) as mock_prop:
        mock_prop.return_value = session
        yield session

@pytest.fixture
def mock_service():
    """Service fixture with mocked session"""
    return OpenGINService(config={})


# Fixtueres for OrganisationService tests
@pytest.fixture
def mock_opengin_service():
    return AsyncMock(spec=OpenGINService)

@pytest.fixture
def organisation_service(mock_opengin_service):
    """OrganisationService fixture with mocked OpenGINService"""
    config = {}
    return OrganisationService(config, mock_opengin_service)

# Fixtures for Util functions tests
@pytest.fixture
def util():
    return Util()

# Fixtures for data service tests
@pytest.fixture
def data_service(mock_opengin_service):
    """DataService fixture with mocked OpenGINService"""
    config = {}
    return DataService(config, mock_opengin_service)

