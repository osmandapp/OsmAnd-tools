# UI Features To-Do List

Below is the backlog of user-interface-specific requirements extracted from `fr.md`.  Each item is expressed as an
unchecked task so that it can be ticked off upon completion.

## Datasets Page

- [ ] Render a list of datasets showing **Name**, **Source**, **Created**, **Updated**, and **Actions** columns.
- [ ] Display a **status icon** for the most recent test job of each dataset.
- [ ] Show a tooltip with the backend error message when the status icon is **FAILED** when user hovering over it.
- [ ] Provide expand/collapse control to reveal the list of **historical test runs** for a dataset.
- [ ] Lazy-load historical runs on demand via `GET /admin/test/datasets/{datasetId}/jobs`.
- [ ] Within each historical run row expose a **Report** button that can either display the aggregated report (FR-13)
  or download raw results (FR-14).
- [ ] Add a **search & filter toolbar** that debounces input ≤300 ms and hits `GET /admin/test/datasets` with
  `search` and `status` parameters.
- [ ] Implement a tree-view presentation where the dataset row is a parent node and job rows are child nodes.

## Dataset Management

- [ ] Implement **Create**, **Delete**, and **Refresh** actions via modal dialogs bound to ingestion endpoints (FR-1 and FR-2).
- [ ] Support browsing of CSV files from server.
- [ ] Support **drag-and-drop CSV upload** to server in addition to the native file picker.
- [ ] Show a confirmation dialog (“Are you sure?”) before deleting a dataset.
- [ ] Display any ingestion or deletion errors in a modal dialog.
- [ ] When a dataset is deleted, visually update the list once the backend has confirmed cascade deletion of related data.

## Test Execution

- [ ] Allow the user to select a dataset and start a **test run**.
- [ ] Open a WebSocket connection to `admin/test/eval/ws/{jobId}` and stream real-time progress updates.
- [ ] Update the progress UI live as `processed`, `total`, and `error` fields change.
- [ ] When a job finishes with **FAILED**, pop a modal dialog showing the error message.
- [ ] When a job finishes with **CANCELLED**, silently update the status icon and job list.

## Reporting

- [ ] Provide a view for the **aggregated metrics report** (Total, Found, In1km, AvgDistance, AvgPlace).
- [ ] Enable download of **raw results** in CSV or JSON format.

## Pagination & Sorting

- [ ] Paginate the dataset list (`page`, `size`) and honour server-side sorting (`sort`).
- [ ] Paginate the job list per dataset.

## Security & Permissions

- [ ] Ensure UI actions that delete datasets or start tests are visible only to users with role **ADMIN**.

## User Experience & Design

- [ ] Make the SPA responsive across modern browsers (Chrome, Firefox, Safari, Edge).
- [ ] Provide clear visual feedback (hover, active, loading indicators) for all interactive elements.
- [ ] Apply OsmAnd branding (colour palette, typography, component styling) consistently.
