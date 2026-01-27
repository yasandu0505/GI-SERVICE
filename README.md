# GI-SERVICE

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

**General Information Service** is a FastAPI-based backend service that acts as a middle-layer API adapter between **OpenGIN (Open General Information Network)** and the **OpenGINXplore** frontend application.

The service is responsible for communicating with OpenGIN APIs, processing and aggregating the retrieved government information, and exposing frontend-friendly endpoints tailored to OpenGINXplore’s data needs. It abstracts the complexity of OpenGIN’s data structures and delivers well-structured, optimized responses for visualization and exploration.

## Features

| Feature | Description |
|--------|-------------|
| Active Ministries by Date | Provides an API to retrieve the list of ministries that were active on a given date, enabling time-aware views of government structures. |
| Active Departments by Ministry | Exposes endpoints to fetch departments active under a specific ministry for a given date, ensuring historically accurate organizational data. |
| Latest Dataset Access | Supplies various types of datasets (tabular) for the most recent available years, optimized for frontend consumption. |
| Prime Minister & Minister Details | Retrieves active Prime Minister details along with assigned ministers for a specified date, including portfolio associations. |
| Backend for Frontend (BFF) APIs | Acts as a dedicated BFF layer for the frontend, orchestrating parallel API calls to upstream services and returning frontend-ready responses. |


## Getting Started

## API Documentation

Once the server is running, you can access:

- **Interactive API Docs**: `http://localhost:8000/docs` (Swagger UI)
- **Alternative Docs**: `http://localhost:8000/redoc` (ReDoc)

## API Endpoints

Organization Contract: [See Contract](gi_service/contract/rest/data_api_contract.yaml)

Data Contract: [See Contract](gi_service/contract/rest/organisation_api_contract.yaml)

## Contributing

Please see our [Contributing Guidelines](CONTRIBUTING.md).

## Code of Conduct

Please see our [Code of Conduct](CODE_OF_CONDUCT.md).

## Security

Please see our [Security Policy](SECURITY.md).

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.