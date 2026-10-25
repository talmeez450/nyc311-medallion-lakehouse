# Power BI build guide (NYC 311 Gold layer)

Power BI Desktop runs on Windows. Install it from the Microsoft Store ("Power BI Desktop") if you do not have it.

## Part A. Get the connection details from Databricks

1. In Databricks click **SQL Warehouses** (left sidebar, under SQL). Open **Serverless Starter Warehouse**.
2. Open the **Connection details** tab. Copy **Server hostname** and **HTTP path**. Keep them in a Notepad window.
3. Create a personal access token: click your profile icon (top right), **Settings**, **Developer**, **Access tokens**,
   **Manage**, **Generate new token**. Name it `powerbi`, set a lifetime of 30 days, and click **Generate**. Copy the token
   now (it is shown once). **Never put the token in GitHub, the report or a screenshot.**
4. The warehouse starts on demand. The first connection can take about a minute. If Power BI times out, wait and retry.

## Part B. Connect and load the data

1. Open Power BI Desktop, **Home**, **Get data**, **More...**, search **Databricks**, choose **Databricks**, **Connect**.
2. Paste **Server Hostname** and **HTTP Path**. Choose **Import** as the data connectivity mode (the .pbix file then opens
   on any computer and still refreshes from Databricks). Click **OK**.
3. Authentication: choose **Personal Access Token**, paste the token, **Connect**.
4. In the Navigator open **workspace**, then **nyc311**. Tick these ten items: `dim_time`, `dim_location`, `dim_agency`,
   `dim_complaint_type`, `fact_311_requests`, `agg_daily_requests`, `vw_pipeline_runs`, `vw_quarantine_summary`,
   `vw_layer_row_counts`, `vw_batch_reconciliation`.
5. Click **Transform Data** (not Load). In `fact_311_requests` select the columns `source_row_hash`,
   `source_extract_timestamp` and `silver_load_timestamp`, right-click, **Remove columns** (they are only used by the pipeline
   and make the file larger). Click **Close and Apply**. Loading about 2.7 million fact rows takes a few minutes.

## Part C. Build the semantic model (Model view, the diagram icon on the left)

1. Create these relationships by dragging the column from the many side to the one side (or **Manage relationships**, **New**).
   Every one must be **Many to one (*:1)**, single direction:

| From (many side) | To (one side) | State |
|---|---|---|
| fact_311_requests[created_date_key] | dim_time[date_key] | Active |
| fact_311_requests[closed_date_key] | dim_time[date_key] | **Inactive** (untick Make this relationship active) |
| fact_311_requests[location_key] | dim_location[location_key] | Active |
| fact_311_requests[agency_key] | dim_agency[agency_key] | Active |
| fact_311_requests[complaint_type_key] | dim_complaint_type[complaint_type_key] | Active |
| agg_daily_requests[created_date_key] | dim_time[date_key] | Active |
| agg_daily_requests[agency_key] | dim_agency[agency_key] | Active |
| agg_daily_requests[complaint_type_key] | dim_complaint_type[complaint_type_key] | Active |

2. Arrange the tables as a star: the fact table in the middle, the four dimensions around it. Take a screenshot of the Model
   view for the report. The audit views stay unconnected (they describe the pipeline, not tickets).
3. Mark the date table: click `dim_time`, **Table tools**, **Mark as date table**, date column `full_date`.
4. Sorting: select `dim_time[month_name]`, **Column tools**, **Sort by column**, `month`. Do the same for `day_name` sorted by
   `day_of_week`.
5. Hide the key columns from the report view (right-click, **Hide in report view**): all `*_key` columns, `date_key`.
6. Add the measures: open `bi/dax_measures.dax`. For each measure click **Home**, **New measure**, paste it, press Enter.
   Create the measures of the first block while `fact_311_requests` is selected, and the audit block while `vw_pipeline_runs`
   (or `vw_quarantine_summary` for the last one) is selected.
7. Apply the theme: **View**, **Themes**, **Browse for themes**, select `bi/nyc311_theme.json`.
8. Format: select `Resolution Rate %` and set the format to Percentage; `Avg Resolution Days` to Decimal, 1 place.

## Part D. Page 1: "311 Operations" (answers the Phase 1 business questions)

Rename the page. Build the visuals by clicking an empty area, choosing the visual in the Visualizations pane and dragging
fields into the wells.

| # | Visual | Fields | Notes |
|---|---|---|---|
| 1 | Slicer | dim_time[year_month] | Style: dropdown |
| 2 | Slicer | dim_location[borough] | Style: dropdown |
| 3 | Card x4 | Total Requests, Resolution Rate %, Avg Resolution Days, Open Requests | KPIs |
| 4 | Clustered bar chart | Y-axis: dim_agency[agency_code]. X-axis: Avg Resolution Hours | Filters pane, Top N: top 10 by Total Requests. Sort by X-axis descending. **Q1: average resolution time by agency** |
| 5 | Bubble map | Latitude: dim_location[centroid_latitude] (Average). Longitude: dim_location[centroid_longitude] (Average). Bubble size: Rodent Requests. Tooltips: dim_location[zip_code] | Filters pane: Rodent Requests is greater than 0. **Q2: rodent complaints by zip code**. Use a filled/heat style if you prefer |
| 6 | Bar chart | Y-axis: dim_complaint_type[complaint_type]. X-axis: Open Requests | Filters pane, Top N: top 5 by Open Requests. **Q3: top 5 unresolved complaint types** |
| 7 | Line chart | X-axis: dim_time[full_date]. Y-axis: Total Requests | Time-series trend |
| 8 | Column chart | X-axis: dim_location[borough]. Y-axis: Total Requests | Categorical breakdown |
| 9 | Matrix | Rows: dim_time[day_name]. Columns: fact_311_requests[created_hour]. Values: Total Requests | Format, Cell elements, Background color: gradient (a heat map of when people call) |

If the rodent map is empty, run notebook 07 section 2 in Databricks: it lists complaint types and the group each got. Edit
`COMPLAINT_GROUP_RULES` in `_common` if Rodent complaints are named differently, then rebuild Gold with `gold_full_refresh=true`.

## Part E. Page 2: "Pipeline audit and data quality" (required: visuals from audit and error logging tables)

| # | Visual | Fields | Notes |
|---|---|---|---|
| 1 | Card x4 | Pipeline Success Rate %, Failed Runs, Quarantined Rows, Last Pipeline Run | |
| 2 | Clustered column chart | X-axis: vw_pipeline_runs[layer]. Y-axis: Avg Run Seconds | Sort X-axis by vw_pipeline_runs[layer_order] (Column tools, Sort by column) |
| 3 | Table | vw_pipeline_runs: start_time, layer, batch_id, status, rows_read, rows_inserted, rows_updated, rows_quarantined, duration_seconds, error_message | Format, Cell elements, Font color by rules on `status`: Failure red, Success green |
| 4 | Bar chart | Y-axis: vw_quarantine_summary[reason]. X-axis: Quarantined Rows | |
| 5 | Column chart | X-axis: vw_layer_row_counts[layer]. Y-axis: vw_layer_row_counts[row_count] | Shows Bronze, Silver, Gold and Quarantine row counts |
| 6 | Table | vw_batch_reconciliation: batch_id, rows_landed, rows_in_bronze, silver_inserted, silver_updated, silver_quarantined, failures | Proves nothing is lost between layers |

If `vw_quarantine_summary` is empty, that is a good result (nothing was rejected). Say so in a text box on the page.

## Part F. Save and check

1. **File**, **Save as**, `NYC311_Dashboard.pbix`.
2. Click **Refresh** on the Home tab to confirm it reloads from Databricks.
3. Test the interactivity: click a borough in a chart and check that the other visuals filter.
4. Take screenshots: Model view, page 1, page 2.
5. Check the file size. If it is over about 200 MB, remove more unused columns in Transform Data.
6. Submit the .pbix file. (Publishing to the Power BI service needs a work or school account and is optional.)

## If Power BI cannot connect

* Make sure the warehouse is running (SQL Warehouses page, start it) and the token has not expired.
* Fall back to Tableau Desktop (Connect, **Databricks** connector, same hostname, HTTP path and token).
