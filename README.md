# Project to analyze portfolio with technical analysis and asset development over time

## Coverage badge
<!-- README.md -->
[![codecov on main](https://codecov.io/gh/jussivirtanen1/AssetManagement/graph/badge.svg?branch=main&token=WDE38GTPO2)](https://codecov.io/gh/jussivirtanen1/AssetManagement)
[![codecov on dev](https://codecov.io/gh/jussivirtanen1/AssetManagement/graph/badge.svg?branch=dev&token=WDE38GTPO2)](https://codecov.io/gh/jussivirtanen1/AssetManagement)


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
- Poetry
### Consideration
- Docker