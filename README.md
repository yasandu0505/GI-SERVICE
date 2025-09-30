# GI-SERVICE

**General Information Service** - API Adapter for OpenGIN (Open General Information Network)

A FastAPI-based service that provides data processing and API endpoints for government information management, including entity attributes, organizational charts, and data visualization capabilities.

## 🚀 Features

- **Entity Management**: Create and manage government entities (departments, ministries, etc.)
- **Attribute Processing**: Process and store entity attributes with metadata
- **Organizational Charts**: Generate timeline-based organizational structures
- **Data Visualization**: Transform data for chart generation
- **Category Management**: Hierarchical category creation and relationships
- **RESTful APIs**: Clean, documented API endpoints

## 📋 Prerequisites

- Python 3.8 or higher
- pip (Python package installer)
- Git

## 🛠️ Installation & Setup

### 1. Clone the Repository

```bash
git clone <repository-url>
cd GI-SERVICE
```

### 2. Create Virtual Environment

```bash
# Create virtual environment
python -m venv venv

# Activate virtual environment
# On Windows:
venv\Scripts\activate
# On macOS/Linux:
source venv/bin/activate
```

### 3. Install Dependencies

```bash
pip install -r requirements.txt
```

### 4. Environment Configuration

Create a `.env` file in the root directory:

```env
# Base URLs for CRUD and Query services
BASE_URL_CRUD=http://0.0.0.0:8080
BASE_URL_QUERY=http://0.0.0.0:8081

# Optional: Add other environment variables as needed
```

### 5. Run the Application

```bash
# Development server
uvicorn main:app --reload --host 0.0.0.0 --port 8000

# Or using the Procfile (for production)
uvicorn main:app --host 0.0.0.0 --port $PORT
```

The API will be available at: `http://localhost:8000`

## 📚 API Documentation

Once the server is running, you can access:

- **Interactive API Docs**: `http://localhost:8000/docs` (Swagger UI)
- **Alternative Docs**: `http://localhost:8000/redoc` (ReDoc)

## 🔌 API Endpoints

### Entity Attributes

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/allAttributes` | Get all available attributes |
| `POST` | `/data/entity/{entityId}` | Get attributes for a specific entity |
| `POST` | `/data/attribute/{entityId}` | Get data for a specific attribute |

### Data Writing

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/data/writeAttributes` | Process and write attributes to entities |

### Organizational Charts

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/data/orgchart/timeline` | Get timeline for organizational chart |
| `POST` | `/data/orgchart/ministries` | Get ministries for selected date |
| `POST` | `/data/orgchart/departments` | Get departments for selected ministry |

## 🧪 Testing the APIs

### 1. Test Basic Connectivity

```bash
curl http://localhost:8000/docs
```

### 2. Test All Attributes Endpoint

```bash
curl -X GET "http://localhost:8000/allAttributes" \
     -H "accept: application/json"
```

### 3. Test Entity Attributes

```bash
curl -X POST "http://localhost:8000/data/entity/your-entity-id" \
     -H "Content-Type: application/json" \
     -d '{
       "entityId": "your-entity-id",
       "filters": {}
     }'
```

### 4. Test Data Writing

```bash
curl -X POST "http://localhost:8000/data/writeAttributes" \
     -H "Content-Type: application/json" \
     -d '{
       "base_url": "/path/to/your/data"
     }'
```

## 📁 Project Structure

```
GI-SERVICE/
├── src/
│   ├── dependencies/          # Dependency injection
│   ├── models/               # Pydantic models
│   ├── routers/              # API route handlers
│   └── services/             # Business logic
├── chartFactory/             # Chart generation utilities
├── test/                     # Test data and scripts
├── main.py                   # FastAPI application entry point
├── requirements.txt          # Python dependencies
├── Procfile                  # Deployment configuration
└── README.md                # This file
```

## 🔧 Configuration

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `BASE_URL_CRUD` | CRUD service base URL | `http://0.0.0.0:8080` |
| `BASE_URL_QUERY` | Query service base URL | `http://0.0.0.0:8081` |

### Data Processing

The service processes data in the following format:

```json
{
  "attributeName": "example_attribute",
  "relatedEntityName": "Department Name",
  "relation": "2022 - Government - Minister - Department",
  "attributeData": {
    "columns": ["col1", "col2"],
    "rows": [["val1", "val2"]]
  },
  "attributeMetadata": {
    "storage_type": "tabular",
    "dataset_name": "Example Dataset"
  }
}
```

## 🚀 Deployment

### Using Heroku

1. Install Heroku CLI
2. Login to Heroku: `heroku login`
3. Create app: `heroku create your-app-name`
4. Deploy: `git push heroku main`

### Using Docker

```dockerfile
FROM python:3.9-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .
EXPOSE 8000

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
```

## 🐛 Troubleshooting

### Common Issues

1. **Port already in use**: Change the port in the uvicorn command
2. **Environment variables not loaded**: Ensure `.env` file is in the root directory
3. **Import errors**: Make sure virtual environment is activated
4. **API not responding**: Check if the CRUD and Query services are running

### Debug Mode

```bash
# Run with debug logging
uvicorn main:app --reload --log-level debug
```

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch: `git checkout -b feature-name`
3. Commit changes: `git commit -am 'Add feature'`
4. Push to branch: `git push origin feature-name`
5. Submit a Pull Request

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 📞 Support

For support and questions:
- Create an issue in the repository
- Contact the development team

---

**Happy Coding! 🎉**
