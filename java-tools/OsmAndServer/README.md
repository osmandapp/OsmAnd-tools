
Used in combination with configuration project
- https://github.com/osmandapp/web-server-config (Provides content for Folder website/ on server)

Frontend is provided by
- https://github.com/osmandapp/web (maps/ and web/ built and copied to website/)

## Crash reports

`POST /api/crash-report?platform=android&version=5.4.5&osversion=14` with the report as the body (zip of tombstones and exception.log as `application/octet-stream` or multipart part `file`, max 50 MB).
`CrashReportController` stores one file per report in `CRASH_REPORTS_LOCATION` (the endpoint is disabled if it is not set), with no database and no personal data.
Copying to data.osmand.net, cleanup schedules (cron.d) and server setup: `servers/crash-reports/README.md` in web-server-config.
