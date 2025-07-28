# 📡 REST API Features – Implementation Checklist

This document tracks all REST endpoints defined in `fr.md` along with their current implementation status.

*Legend*: `[ ]` – *to do*, `[~]` – *in progress*, `[x]` – *done*

## General

- [ ] **OpenAPI/Swagger documentation** is generated for the entire `/admin/test/**` namespace (#FR-General).
- [ ] **Global URI prefix** `/admin/test/` is applied to every controller class.
- [ ] **Problem Details (RFC 7807)** error responses are produced consistently (#FR-24).

## 1. Data Ingestion

- [x] **POST `/admin/test/csv/count`** – reads a local CSV file (path provided in request) and returns total row 
  count `N` (#FR-2-1).
- [ ] **POST `/admin/test/refreshDataset`** – reads a local CSV file (path provided in request) and persists a random sample (Reservoir Sampling) limited by `sizeLimit` (#FR-2-2).

## 2. Test Execution

- [ ] **POST `/admin/test/eval/{datasetId}`** – starts an asynchronous address search evaluation job; body contains 
  `addressExpression` (+ optional locale & other params) (#FR-7, FR-8-1).
- [ ] **GET `/admin/test/eval/{jobId}`** – polls job progress or final status (#FR-10).
- [ ] **POST `/admin/test/eval/cancel/{jobId}`** – requests cancellation of a running job (#FR-11).
- [ ] **WebSocket `/admin/test/eval/ws/{jobId}`** – real-time job progress push channel (#FR-17).

## 3. Reporting

- [ ] **GET `/admin/test/reports/{datasetId}`** – returns aggregated metrics for latest or specified run (#FR-13).
- [ ] **GET `/admin/test/reports/{datasetId}/download`** – downloads raw result set in `csv` or `json` format (#FR-14).

## 4. Dataset Management & Querying

- [ ] **GET `/admin/test/datasets`** – paginated & filterable list of datasets including latest job status (#FR-18).  
  Parameters: `page`, `size`, `search`, `status`, `sort`.
- [ ] **GET `/admin/test/datasets/{datasetId}/jobs`** – paginated list of all jobs for a dataset (#FR-19).  
  Parameters: `page`, `size`.
- [ ] **POST `/admin/test/datasets/{datasetId}/delete`** – deletes dataset metadata, SQLite table and related
  results rows (referenced in FR-16).

## 5. Security

- [ ] All endpoints above require **JWT authentication**; test/job operations limited to role `ADMIN` (#FR-20-21).

## Test Search API

### Ingest data from Overpass API

- **URL:** `/admin/test/overpass`
- **Method:** `POST`
- **Body:** `OverpassTestRequest`

```json
{
  "datasetName": "my_test_dataset",
  "query": "node[amenity=restaurant](around:1000, 50.45, 30.52);out;"
}
```