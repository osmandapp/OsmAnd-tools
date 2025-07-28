# 📄 Product Requirements Document (PRD)

## 🟢 Business Context

- Environment:
    * Development platform is Java 17+ on Linux or Windows 10.
    * SQLite database.
    * Web server based on Spring and HTML/CSS with TypeScript.

- Problems:
    * The main issue is that users input address's query in non-standard ways: like missing countries or states, putting
  building numbers in wrong place, using different abbreviations, etc. There are big number of address variations.
    * Testing team spend much time to test Search addresses capabilities of free-form user query in @SearchUICore class.
    * Lead of testing team wants to see all problems with Search addresses in unified report after each testing.

- Primary KPI:
    * Reduce time to test different scenarios of free-form user query.
    * Improve quality of Search addresses service by finding data areas with not found addresses, or incorrect logic 
      in service which does not allow to find addresses.

## 🎯 Goals

The product must enable:

1. Flexible ingestion of heterogeneous address datasets (external REST, internal CSV, etc.).
2. Storage of these datasets to support repeatable, large-scale quality tests.
3. Automated evaluation of free-form address queries against the existing search service with metrics collection.
4. An intuitive web experience for dataset lifecycle management, test execution, and results exploration.
5. A secure, documented REST API that powers the UI and permits future integrations.

Implementation specifics, detailed API definitions, performance targets, and UI interaction flows are maintained in 
the dedicated Functional Requirements document (`fr.md`).
