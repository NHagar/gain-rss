# GAIN RSS Feed Updater

This project automatically updates an RSS feed by monitoring the [Generative AI in the Newsroom](https://generative-ai-newsroom.com/) Medium publication and maintaining a local database and XML feed.

## How it Works

1. **Daily Updates**: The GitHub Action runs daily at 8 AM UTC to check for new articles
2. **Duplicate Detection**: Compares URLs (stripped of parameters) against the local database
3. **RSS Feed Generation**: Adds new items to the top of the local RSS feed in `data/gain_feed.xml`
4. **Database Storage**: Stores article metadata in a DuckDB database for efficient querying

## Files

- `update_feed.py`: Main script that handles the daily updates
- `data/gain_feed.xml`: The generated RSS feed file
- `data/gain_feed.duckdb`: DuckDB database storing article metadata
- `.github/workflows/update-feed.yml`: GitHub Action for automated daily updates

## Manual Execution

To run the update script manually:

```bash
python update_feed.py
```

## Dependencies

- Python 3.12+
- duckdb
- requests
- pandas
- pydantic

Install dependencies with:
```bash
uv sync
```

## GitHub Action

The automated workflow:
- Runs daily at 8 AM UTC
- Can be triggered manually via workflow_dispatch
- Commits and pushes any changes to the feed and database