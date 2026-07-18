# Power BI — PipelineX Dashboard

The `.pbix` file is built on top of the four Synapse views:

- `vw_monthly_revenue_by_category`
- `vw_customer_ltv`
- `vw_seller_performance`
- `vw_weekly_cohort`

## Connect to Synapse Serverless

1. **Get Data → Azure → Azure Synapse Analytics SQL**
2. Server: `<workspace>-ondemand.sql.azuresynapse.net`
3. Database: `pipelinex_gold`
4. Data Connectivity mode: **DirectQuery** (keeps refreshes free — nothing is imported)
5. Authenticate with **Microsoft account** (your Azure student account)

## Report pages

| Page | Visuals |
| --- | --- |
| **Executive Overview** | KPI cards (revenue, orders, avg review), monthly revenue line, top-10 categories bar |
| **Customer** | LTV distribution, state choropleth, cohort matrix (new vs returning) |
| **Operations** | Delivery-days histogram, seller scorecard, review-score heatmap by state |
| **Product** | Category treemap, tag word cloud (MongoDB enrichment), margin waterfall |

## Publishing

Free Power BI Desktop → export as `.pbix` and commit here. A Power BI Pro
license is only needed to publish to the online Service; the local `.pbix`
runs against Synapse with no license.
