# Contributing Guidelines

Thank you for your interest in contributing to this project! We welcome contributions from everyone. This document provides guidelines and best practices for contributing.

## Code of Conduct
Please read our [Code of Conduct](CODE_OF_CONDUCT.md) before contributing.

By participating in this project, you agree to maintain a respectful and inclusive environment for everyone.

## How to Contribute

There are many ways to contribute to this project:

- **Report Bugs**: Submit bug reports with detailed information
- **Suggest Features**: Propose new features or improvements
- **Improve Documentation**: Fix typos, clarify explanations, add examples
- **Submit Code**: Fix bugs or implement new features
- **Review Pull Requests**: Help review and test contributions from others

## Getting Started

### Prerequisites

### Prerequisites

- Python 3.8 to 3.13
- pip (Python package installer)
- Git
- Docker (Optional)

### Development Setup

### Installation & Setup

**Clone the Repository**

   ```bash
   git clone https://github.com/LDFLK/GI-SERVICE.git
   cd GI-SERVICE
   ```

### Method 1 (Manual)
1. **Create Virtual Environment**

   ```bash
   # Create virtual environment
   python -m venv .venv

   # Activate virtual environment
   # On Windows:
   .venv\Scripts\activate

   # On macOS/Linux:
   source .venv/bin/activate
   ```

2. **Install Dependencies**

   ```bash
   pip install -r requirements.txt
   ```

3. **Environment Configuration**

   Create a `.env` file in the root directory:

   ```env
   # Base URLs for Read(Query) services in OpenGIN
   BASE_URL_QUERY=http://0.0.0.0:8081
   ```

4. **Run the Application**

   ```bash
   # Development server
   uvicorn main:app --reload --host 0.0.0.0 --port 8000

   # Or using the Procfile (for production)
   uvicorn main:app --host 0.0.0.0 --port $PORT
   ```

   The API will be available at: `http://localhost:8000`

### Method 2 (Docker)

   ```bash
   # Make sure docker deamon running
   
   # Up containers with existing image
   docker compose up 

   # Up container & build image
   docker compose up --build
   ```

## Configuration

#### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `BASE_URL_QUERY` | Query(Read) OpenGIN service URL | `http://0.0.0.0:8081` |

## Making Changes

### Branching Strategy

Create a topic branch from the main branch for your changes:

```bash
git checkout -b feature/your-feature-name
```

### Commit Messages

Write clear and meaningful commit messages. We recommend following this format:

```
[TYPE] Short description (max 50 chars)

Longer description if needed. Explain the "why" behind the change,
not just the "what". Reference any related issues.

Fixes #123
```

**Types**: `feat`, `fix`, `docs`, `style`, `refactor`, `test`, `chore`

### Coding Standards

<!-- Describe coding style, linting, formatting requirements -->
<!-- Example:
- Follow PEP 8 for Python code
- Run `black` for formatting
- Run `flake8` for linting
-->

### Testing

<!-- Describe testing requirements -->
<!-- Example:
- Add unit tests for new functionality
- Ensure all tests pass: `pytest`
- Maintain or improve code coverage
-->

All changes should include appropriate tests. Run the test suite before submitting:

```bash
# Add your test command here
```

## Submitting Changes

### Pull Request Process

1. Ensure your code follows the project's coding standards
2. Update documentation if needed
3. Add or update tests as appropriate
4. Run the full test suite and ensure it passes
5. Push your branch and create a Pull Request

### Pull Request Guidelines

- Provide a clear title and description
- Reference any related issues (e.g., "Fixes #123")
- Keep changes focused and atomic
- Be responsive to feedback and review comments

<!-- For projects requiring CLA -->
<!-- ### Contributor License Agreement (CLA)
For significant contributions, you may need to sign a Contributor License Agreement.
-->

### Review Process

<!-- Describe how PRs are reviewed and merged -->
<!-- Example:
- PRs require at least one approval from a maintainer
- CI checks must pass
- Changes may be requested before merging
-->

## Communication

<!-- List communication channels -->
<!-- Example:
- GitHub Issues: For bug reports and feature requests
- GitHub Discussions: For questions and general discussion
- Mailing List: dev@project.apache.org
- Slack/Discord: [Link to channel]
-->

## Recognition

<!-- Describe how contributors are recognized -->
<!-- Example:
Contributors are recognized in our CONTRIBUTORS.md file and release notes.
-->

We value all contributions and appreciate your effort to improve this project!

## Additional Resources

<!-- Add links to helpful resources -->
<!-- Example:
- [Project Documentation](link)
- [Issue Tracker](link)
- [Development Roadmap](link)
-->

---

*These guidelines are inspired by the [Apache Way](https://www.apache.org/theapacheway/) and [Open Source Guides](https://opensource.guide/).*
