# Personal Workspace Assistant (ADK + MCP)

A lightweight multi-tool AI assistant that helps manage tasks/notes and perform dataset analytics via BigQuery.  
Deployed on Google Cloud Run using Google Agent Development Kit (ADK) with the ADK Dev UI.

## Features
- Natural language conversation
- Task management (create / list / complete) with persistent storage (Datastore / Firestore in Datastore mode)
- Notes management (save / list) with persistent storage
- Wikipedia lookup + short summaries
- BigQuery analytics via MCP (read-only SQL for dataset Q&A)
- Modular tool architecture (easy to extend)

## Tech Stack
- Python
- Google Agent Development Kit (ADK)
- MCP tooling
- FastAPI + Uvicorn
- Google Cloud Run
- Google Cloud Datastore / Firestore (Datastore mode)
- BigQuery
- LangChain (Wikipedia tool)

## Project Structure (example)
```text
.
├── agent.py
├── tools.py                 # optional helper tools (if used)
├── requirements.txt
├── .env
└── data/                    # optional local CSVs (if used for loading)
```
## Prerequisites
* A Google Cloud project with billing enabled
* Enabled APIs (as needed):
* Vertex AI (Gemini via Vertex)
* Cloud Run
* Datastore / Firestore (Datastore mode)
* BigQuery + BigQuery Storage API
* A Cloud Run service account with required permissions

## Environment Variables

Create a .env file in the project root:
```bash
PROJECT_ID=
SERVICE_ACCOUNT=
MODEL=gemini-2.5-flash-lite
DB_ID=
BQ_DATASET=
GOOGLE_GENAI_USE_VERTEXAI=True
GOOGLE_API_KEY=""
```

Note: When using Vertex AI on Cloud Run, the runtime typically uses the Cloud Run service account credentials (no local key file required).

## IAM Permissions (recommended)

Assign these roles to your Cloud Run service account:
* roles/aiplatform.user
* roles/datastore.user
* roles/bigquery.jobUser
* roles/bigquery.dataViewer (or dataset-level permissions)

## Run Locally
Install dependencies:
```bash
pip install -r requirements.txt
```
Run:
```bash
uvicorn agent:app --reload
```
(If your entrypoint differs, adjust accordingly.)

## Deploy to Cloud Run (ADK)
```bash
uvx --from google-adk==1.14.0 \
adk deploy cloud_run \
  --project=$PROJECT_ID \
  --region=europe-west1 \
  --service_name=asgen-guide \
  --with_ui \
  . \
  -- \
  --service-account=$SERVICE_ACCOUNT
```
## Usage Examples
### Tasks / Notes
* Add a task: prepare the demo
* List my tasks
* Mark task 123 as completed
* Save a note titled "Interview prep" with this content: STAR stories and project highlights
* List my notes
### Wikipedia
* Look up Docker on Wikipedia and summarize in 5 bullet points
* Explain Kubernetes in a short summary
### BigQuery (read-only analytics)
* Using BigQuery, show the latest available date in marketdata.gold_silver_raw.
* Using BigQuery, list the top 10 cryptocurrencies by market cap in the dataset.
* Using BigQuery, compare gold vs silver monthly returns in the last 12 months.
### Extending the Assistant
* Add new MCP tools (e.g., Calendar, Drive, etc.)
* Add sub-agents (specialists) coordinated by a primary agent
* Add new datasets/tables and analysis prompts

## Demo
[▶ Watch Demo on YouTube](https://youtu.be/H7aYrHqxsvw)

[![Demo Video](https://img.youtube.com/vi/H7aYrHqxsvw/0.jpg)](https://youtu.be/H7aYrHqxsvw)

## Contributing
Pull requests are welcome. For major changes, open an issue first.

## License
MIT