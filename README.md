# Project to analyze portfolio with technical analysis and asset development over time

## Coverage badge
<!-- README.md -->
[![Go Coverage](https://github.com/jussivirtanen1/AssetManagement/wiki/coverage.svg)](https://raw.githack.com/wiki/jussivirtanen1/AssetManagement/coverage.html)

## Analysis
- Files contain ready asset development over time and technical analysis of moving averages. In addition volatilities and beta can be computed.

## Database connections and configuration
- Password for database fetched based on username running the code.
- User should have a local file containing necessary configurations and NOT containing passwords or secrets. This file should NOT be uploaded in remote repo, remember to include it in .gitignore.

## Tools:
### In use
- Github Actions
- Pytest and coverage
- Python and PostgreSQL
### Consideration
- Poetry
- Docker