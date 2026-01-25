## Cortex narrative

### What it does
- The Map View includes a Cortex assistant panel that answers natural language questions about the map data.
- The assistant translates a user question into SQL using a Snowflake Cortex Analyst semantic view and returns a preview of the results.
- This keeps the interaction grounded in the same curated metrics that drive the choropleth map.

### How it works
- The app sends the user’s prompt to the Cortex Analyst REST endpoint.
- The request references the semantic view `ANALYTICS.SILVER.DISASTER_EXPLORER`, which defines the tables, dimensions, and time dimensions available to the assistant.
- Cortex Analyst responds with an interpretation and the generated SQL.
- The app executes that SQL in Snowflake and returns a preview table to the user.

### REST API flow
1. Build the request body with the user prompt and the semantic view name.
2. POST to `/api/v2/cortex/analyst/message`.
3. Parse the response content blocks:
   - `text` for the explanation
   - `sql` for the generated SQL statement
4. Run the SQL and render the first rows in a table.

### Error handling and fallbacks
- If the REST call fails, the assistant returns the error message so the user has immediate feedback.
- If the SQL cannot be executed, the UI displays the SQL error instead of a blank response.
- If the response has no SQL, the assistant returns the interpretation text only.

### Development notes
- The assistant uses the active Snowflake connection and its REST client, so no separate API token handling is required.
- The semantic view controls available fields and guards against unintended table access.
- Result previews are intentionally small to keep the UI responsive and avoid large data transfers.
